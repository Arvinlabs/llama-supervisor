package main

import (
	"bytes"
	"encoding/json"
	"io"
	"mime"
	"net/http"
	"sort"
	"strconv"
)

// prefixCachePolicy is a request policy modifier that normalizes /v1/chat/completions
// request bodies before proxying so that semantically identical requests produce
// identical bytes, maximizing the backend prompt/prefix cache hit rate.
//
// Normalization (JSON object key order is semantically insignificant, so the request
// meaning does not change):
//   - the tools list is sorted by tool name (function tools: function.name, custom
//     tools: custom.name, per the OpenAI spec)
//   - all JSON object keys are re-marshaled in sorted order recursively (Go marshals
//     maps with sorted keys), including tool parameter schemas
type prefixCachePolicy struct{}

func newPrefixCachePolicy() *prefixCachePolicy { return &prefixCachePolicy{} }

const completionsPath = "/v1/chat/completions"

// modifyRequest only normalizes POST /v1/chat/completions with a JSON body;
// everything else (and bodies that are not valid JSON) passes through unchanged
func (c *prefixCachePolicy) modifyRequest(r *http.Request) {
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
	out, err := normalizeChatCompletions(body)
	if err != nil {
		// not valid JSON: restore the original body
		r.Body = io.NopCloser(bytes.NewReader(body))
		r.ContentLength = int64(len(body))
		return
	}
	r.Body = io.NopCloser(bytes.NewReader(out))
	r.ContentLength = int64(len(out))
	r.Header.Set("Content-Length", strconv.Itoa(len(out)))
}

// normalizeChatCompletions canonicalizes a chat-completions request body:
// sort the tools list by function name, then re-marshal with every object's keys sorted
func normalizeChatCompletions(body []byte) ([]byte, error) {
	var req map[string]any
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, err
	}
	sortTools(req["tools"])
	return json.Marshal(req)
}

// sortTools sorts the tools list by function name; when names tie, fall back to the
// canonical JSON bytes so the order stays deterministic
func sortTools(v any) {
	list, ok := v.([]any)
	if !ok || len(list) < 2 {
		return
	}
	names := make([]string, len(list))
	for i, item := range list {
		names[i] = toolName(item)
	}
	sort.SliceStable(list, func(i, j int) bool {
		if names[i] != names[j] {
			return names[i] < names[j]
		}
		bi, _ := json.Marshal(list[i])
		bj, _ := json.Marshal(list[j])
		return string(bi) < string(bj)
	})
}

// toolName returns the name used to sort a tool entry. Per the OpenAI spec the
// tools list items are a union of two shapes: function tools carry the name under
// function.name (required by the spec), custom tools under custom.name; unknown
// shapes yield "" so the byte-order tiebreak keeps the sort deterministic
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
