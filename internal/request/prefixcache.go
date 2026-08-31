package request

import (
	"bytes"
	"encoding/json"
	"io"
	"mime"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

// prefixCachePolicy is a request plugin that normalizes /v1/chat/completions
// request bodies before proxying so that semantically identical requests produce
// identical bytes, maximizing the backend prompt/prefix cache hit rate.
//
// Normalization targets the top-level "tools" list (gjson locates it, sjson splices
// the result back in): every element is decoded and re-encoded in canonical form —
// object keys sorted at all levels, numbers in shortest form (1.0 -> 1, 1e3 -> 1000),
// no HTML escaping (<, >, & and non-ASCII stay raw) — and the list is sorted by tool
// name (function tools: function.name, custom tools: custom.name, per the OpenAI
// spec; ties fall back to the canonical bytes). Semantically equal tools lists in
// any key order / number literal / whitespace therefore yield one canonical array.
// Every byte outside the tools array is passed through untouched.
//
// A tools array containing invalid UTF-8 is left unchanged: re-encoding would replace
// the invalid bytes with U+FFFD, corrupting tool definitions that are part of the prompt.
type prefixCachePolicy struct{}

func newPrefixCachePolicy() *prefixCachePolicy { return &prefixCachePolicy{} }

// compile-time proof that prefixCachePolicy is a request plugin
var _ plugin = (*prefixCachePolicy)(nil)

const completionsPath = "/v1/chat/completions"

// ModifyRequest only normalizes POST /v1/chat/completions with a JSON body;
// everything else passes through unchanged
func (c *prefixCachePolicy) ModifyRequest(r *http.Request) {
	if r.Method != http.MethodPost || r.URL.Path != completionsPath {
		return
	}
	mediaType, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil || mediaType != "application/json" {
		return
	}
	body, err := io.ReadAll(r.Body)
	if err != nil {
		return
	}
	r.Body.Close()
	if len(bytes.TrimSpace(body)) == 0 {
		return
	}
	out := normalizeChatCompletions(body)
	// always (re)install the body: r.Body was drained and closed above, so even the
	// unchanged case must be restored or the proxy would forward an empty body
	r.Body = io.NopCloser(bytes.NewReader(out))
	r.ContentLength = int64(len(out))
	r.Header.Set("Content-Length", strconv.Itoa(len(out)))
}

// normalizeChatCompletions canonicalizes the top-level "tools" array (sorted by
// tool name, each element re-encoded canonically); it returns the input unchanged
// when the body has no tools array, the array is malformed, or its bytes are not
// valid UTF-8
func normalizeChatCompletions(body []byte) []byte {
	tools := gjson.GetBytes(body, "tools")
	if !tools.IsArray() || isEmptyArray(tools.Raw) { // missing or empty array: nothing to do
		return body
	}
	raw := []byte(tools.Raw)
	if !utf8.Valid(raw) {
		return body // re-encoding would turn invalid bytes into U+FFFD
	}
	var items []any
	if err := json.Unmarshal(raw, &items); err != nil {
		return body
	}
	canonical := make([][]byte, len(items))
	names := make([]string, len(items))
	for i, it := range items {
		b, err := canonicalJSON(it)
		if err != nil {
			return body
		}
		canonical[i] = b
		names[i] = toolName(it)
	}
	order := make([]int, len(items))
	for i := range order {
		order[i] = i
	}
	sort.SliceStable(order, func(a, b int) bool {
		if names[a] != names[b] {
			return names[a] < names[b]
		}
		return bytes.Compare(canonical[a], canonical[b]) < 0
	})
	var sb strings.Builder
	sb.WriteByte('[')
	for n, i := range order {
		if n > 0 {
			sb.WriteByte(',')
		}
		sb.Write(canonical[i])
	}
	sb.WriteByte(']')
	if sb.String() == tools.Raw { // already canonical: keep the original bytes
		return body
	}
	out, err := sjson.SetRawBytes(body, "tools", []byte(sb.String()))
	if err != nil { // malformed body: pass through unchanged
		return body
	}
	return out
}

// isEmptyArray reports whether a raw JSON array spans only its brackets
// (whitespace in between, e.g. "[]" or "[ ]")
func isEmptyArray(raw string) bool {
	rest := strings.Trim(raw, " \t\n\r")
	if len(rest) < 2 || rest[0] != '[' || rest[len(rest)-1] != ']' {
		return false
	}
	return strings.TrimSpace(rest[1:len(rest)-1]) == ""
}

// canonicalJSON encodes a decoded JSON value in canonical form: object keys sorted
// at every level (Go marshals maps that way), numbers in their shortest float64
// form, and no HTML escaping, so semantically equal values always yield identical bytes
func canonicalJSON(v any) ([]byte, error) {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(v); err != nil {
		return nil, err
	}
	// Encoder appends a trailing newline that does not belong inside the array
	return bytes.TrimRight(buf.Bytes(), "\n"), nil
}

// toolName returns the name used to sort a tool entry. Per the OpenAI spec the
// tools list items are a union of two shapes: function tools carry the name under
// function.name (required by the spec), custom tools under custom.name; unknown
// shapes yield "" so the canonical-byte tiebreak keeps the sort deterministic
func toolName(v any) string {
	m, ok := v.(map[string]any)
	if !ok {
		return ""
	}
	for _, key := range []string{"function", "custom"} {
		obj, ok := m[key].(map[string]any)
		if !ok {
			continue
		}
		if name, _ := obj["name"].(string); name != "" {
			return name
		}
	}
	return ""
}
