package request

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
)

// without a tools array the body must pass through byte-identical: key order,
// number literal forms and whitespace are never touched
func TestNormalizeChatCompletionsNoToolsUnchanged(t *testing.T) {
	in := []byte(`{"stream":true, "model":"m","temperature":1.0,"top_p":0.900,"max_tokens":1e3,
"messages":[{"role":"user","content":"<script> a & b </script> 你好, 世界"}]}`)
	out := normalizeChatCompletions(in)
	if !bytes.Equal(out, in) {
		t.Fatalf("body without tools must be byte-identical:\n%s\n%s", in, out)
	}
}

// the tools array is fully canonicalized: elements sorted by function name, keys
// sorted inside elements, numbers in shortest form, no HTML escaping; bytes outside
// the tools array are untouched
func TestNormalizeChatCompletionsOnlyToolsTouched(t *testing.T) {
	in := []byte(`{"model":"m","temperature":1.0,"tools":[` +
		`{"type":"function","function":{"name":"zebra","description":"z <q> & r","parameters":{"type":"object","properties":{"b":1.0,"a":2}}}},` +
		`{"type":"function","function":{"name":"alpha","description":"a","parameters":{"type":"object","properties":{"y":{},"x":{}}}}}` +
		`],"messages":[{"role":"user","content":"hi"}]}`)
	out := normalizeChatCompletions(in)
	// bytes outside the tools array are untouched
	if !strings.Contains(string(out), `"temperature":1.0`) {
		t.Fatalf("bytes outside tools must be preserved: %s", out)
	}
	// elements re-encoded canonically: all keys sorted, 1.0 -> 1, < and & NOT escaped
	if !strings.Contains(string(out), `{"function":{"description":"a","name":"alpha","parameters":{"properties":{"x":{},"y":{}},"type":"object"}},"type":"function"}`) {
		t.Fatalf("alpha element not canonical: %s", out)
	}
	if !strings.Contains(string(out), `{"function":{"description":"z <q> & r","name":"zebra","parameters":{"properties":{"a":2,"b":1},"type":"object"}},"type":"function"}`) {
		t.Fatalf("zebra element not canonical: %s", out)
	}
	// order: alpha before zebra
	var req struct {
		Tools []struct {
			Function struct {
				Name string `json:"name"`
			} `json:"function"`
		} `json:"tools"`
	}
	if err := json.Unmarshal(out, &req); err != nil {
		t.Fatal(err)
	}
	if len(req.Tools) != 2 || req.Tools[0].Function.Name != "alpha" || req.Tools[1].Function.Name != "zebra" {
		t.Fatalf("tools not sorted by function name: %s", out)
	}
	if !bytes.HasSuffix(out, []byte(`],"messages":[{"role":"user","content":"hi"}]}`)) {
		t.Fatalf("bytes after the tools array were altered: %s", out)
	}
}

// custom tools (type "custom") carry the name under custom.name and sort with function tools
func TestNormalizeChatCompletionsSortsCustomTools(t *testing.T) {
	in := []byte(`{"model":"m","tools":[` +
		`{"type":"custom","custom":{"name":"zeta","format":{"type":"text"}}},` +
		`{"type":"function","function":{"name":"mid","parameters":{"type":"object"}}},` +
		`{"type":"custom","custom":{"name":"alpha","format":{"type":"text"}}}` +
		`],"messages":[]}`)
	out := normalizeChatCompletions(in)
	var req struct {
		Tools []struct {
			Type   string `json:"type"`
			Custom struct {
				Name string `json:"name"`
			} `json:"custom"`
			Function struct {
				Name string `json:"name"`
			} `json:"function"`
		} `json:"tools"`
	}
	if err := json.Unmarshal(out, &req); err != nil {
		t.Fatal(err)
	}
	if len(req.Tools) != 3 {
		t.Fatalf("expected 3 tools, got %d: %s", len(req.Tools), out)
	}
	want := map[string]string{"alpha": "custom", "mid": "function", "zeta": "custom"}
	for i, tl := range req.Tools {
		name := tl.Custom.Name
		if tl.Type == "function" {
			name = tl.Function.Name
		}
		if typ, ok := want[name]; !ok || tl.Type != typ {
			t.Fatalf("tools[%d] = %s/%s, want %s: %s", i, tl.Type, name, typ, out)
		}
	}
}

// semantically identical tools lists — different element order, key order inside
// elements, number literal forms and whitespace — must all normalize to identical
// bytes; message order and content are always preserved
func TestNormalizeChatCompletionsCanonicalForm(t *testing.T) {
	m := `],"messages":[{"role":"user","content":"second"},{"role":"system","content":"first"}]}`
	vars := `{"type":"object","properties":{"scale":1.0}}`
	varsAlt := `{"type":"object","properties":{"scale":1}}`
	bodies := []string{
		`{"model":"m","tools":[` +
			`{"type":"function","function":{"name":"b","parameters":` + vars + `}},` +
			`{"type":"function","function":{"name":"a","parameters":` + vars + `}}` + m,
		`{"model":"m","tools":[
			{"type":"function","function":{"parameters":` + varsAlt + `,"name":"a"}},
			{"function":{"name":"b","parameters":` + vars + `},"type":"function"}
		` + m,
	}
	first := normalizeChatCompletions([]byte(bodies[0]))
	for i, b := range bodies[1:] {
		got := normalizeChatCompletions([]byte(b))
		if !bytes.Equal(first, got) {
			t.Fatalf("bodies %d and %d must normalize identically:\n%s\n%s", 0, i+1, first, got)
		}
	}
	var req struct {
		Messages []struct {
			Role    string `json:"role"`
			Content string `json:"content"`
		} `json:"messages"`
	}
	if err := json.Unmarshal(first, &req); err != nil {
		t.Fatal(err)
	}
	if len(req.Messages) != 2 || req.Messages[0].Content != "second" || req.Messages[1].Content != "first" {
		t.Fatalf("message order changed: %+v", req.Messages)
	}
}

// an already-canonical tools array (sorted names, sorted keys, bare "," separators)
// leaves the body byte-identical
func TestNormalizeChatCompletionsAlreadySortedUnchanged(t *testing.T) {
	in := []byte(`{"model":"m","tools":[{"function":{"name":"a"},"type":"function"},{"function":{"name":"b"},"type":"function"}],"messages":[]}`)
	out := normalizeChatCompletions(in)
	if !bytes.Equal(out, in) {
		t.Fatalf("already-canonical body must be byte-identical:\n%s\n%s", in, out)
	}
}

// a single already-canonical element and an empty tools array are left unchanged
func TestNormalizeChatCompletionsShortToolsUnchanged(t *testing.T) {
	for _, in := range [][]byte{
		[]byte(`{"model":"m","tools":[{"function":{"name":"a"},"type":"function"}],"messages":[]}`),
		[]byte(`{"model":"m","tools":[],"messages":[]}`),
	} {
		if out := normalizeChatCompletions(in); !bytes.Equal(out, in) {
			t.Fatalf("body must be unchanged:\n%s\n%s", in, out)
		}
	}
}

// a "tools" key nested deeper (or inside a string) must not match: only the
// top-level tools array is sortable
func TestNormalizeChatCompletionsNestedToolsIgnored(t *testing.T) {
	for _, in := range [][]byte{
		[]byte(`{"model":"m","tools":[{"function":{"name":"b"},"type":"function"}],"messages":[{"role":"user","content":"tools":[{"name":"x"}]}]}`),
		[]byte(`{"model":"m","metadata":{"tools":[1,2]},"messages":[]}`),
		[]byte(`{"model":"m","other":{"tools":[{"name":"x"},{"name":"y"}]},"messages":[]}`),
	} {
		if out := normalizeChatCompletions(in); !bytes.Equal(out, in) {
			t.Fatalf("nested/string tools must not be touched:\n%s\n%s", in, out)
		}
	}
}

// malformed bodies (invalid JSON, trailing data, unterminated tools array) pass
// through unchanged
func TestNormalizeChatCompletionsMalformedUnchanged(t *testing.T) {
	for _, in := range [][]byte{
		[]byte(`{"model":`),
		[]byte(`{"model":"m"} junk`),
		[]byte(`{"model":"m","tools":[{"type":"function"`),
		[]byte(`no json at all`),
	} {
		if out := normalizeChatCompletions(in); !bytes.Equal(out, in) {
			t.Fatalf("malformed body must be unchanged:\n%s\n%s", in, out)
		}
	}
}

// a tools array with invalid UTF-8 is left unchanged (re-encoding would replace the
// bytes with U+FFFD and corrupt the prompt); invalid UTF-8 OUTSIDE the tools array
// does not prevent the tools from being normalized
func TestNormalizeChatCompletionsInvalidUTF8(t *testing.T) {
	toolsInvalid := []byte(`{"tools":[{"type":"function","function":{"name":"b","description":"caf\xE9"}},{"type":"function","function":{"name":"a"}}],"messages":[]}`)
	if out := normalizeChatCompletions(toolsInvalid); !bytes.Equal(out, toolsInvalid) {
		t.Fatalf("tools with invalid UTF-8 must be unchanged: %s", out)
	}
	toolsValid := []byte(`{"tools":[{"type":"function","function":{"name":"b"}},{"type":"function","function":{"name":"a"}}],"messages":[{"content":"caf\xE9"}]}`)
	out := normalizeChatCompletions(toolsValid)
	// invalid bytes outside the tools array are preserved verbatim
	if !bytes.Contains(out, []byte(`"content":"caf\xE9"`)) {
		t.Fatalf("non-UTF-8 bytes outside tools must be preserved: %s", out)
	}
	// and the tools array is still normalized
	if !strings.Contains(string(out), `{"function":{"name":"a"},"type":"function"},{"function":{"name":"b"`) {
		t.Fatalf("tools not sorted: %s", out)
	}
}

// unicode-escaped key spelling (\u0074ools) is recognized as the top-level tools key
func TestNormalizeChatCompletionsEscapedKey(t *testing.T) {
	in := []byte(`{"\u0074ools":[{"type":"function","function":{"name":"b"}},{"type":"function","function":{"name":"a"}}],"model":"m"}`)
	out := normalizeChatCompletions(in)
	// the original \u0074ools key spelling must be preserved and the array sorted
	if !strings.HasPrefix(string(out), `{"\u0074ools":[{"function":{"name":"a"},"type":"function"},`) {
		t.Fatalf("escaped tools key not sorted / key spelling changed: %s", out)
	}
}

// ModifyRequest only normalizes POST /v1/chat/completions with a JSON body
func TestPrefixCacheModifyRequest(t *testing.T) {
	c := newPrefixCachePolicy()

	t.Run("normalizes chat completions", func(t *testing.T) {
		body := `{"model":"m","tools":[{"type":"function","function":{"name":"b","parameters":{"type":"object","properties":{"q":{}}}}},{"type":"function","function":{"name":"a","parameters":{"type":"object","properties":{"q":{}}}}}], "temperature": 1.0, "messages":[{"role":"user","content":"hi"}]}`
		r := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(body))
		r.Header.Set("Content-Type", "application/json")
		c.ModifyRequest(r)
		got, err := io.ReadAll(r.Body)
		if err != nil {
			t.Fatal(err)
		}
		if r.ContentLength != int64(len(got)) {
			t.Fatalf("ContentLength = %d, body len = %d", r.ContentLength, len(got))
		}
		if cl := r.Header.Get("Content-Length"); cl != strconv.Itoa(len(got)) {
			t.Fatalf("Content-Length header = %q, want %d", cl, len(got))
		}
		var req struct {
			Tools []struct {
				Function struct {
					Name string `json:"name"`
				} `json:"function"`
			} `json:"tools"`
			Temperature float64 `json:"temperature"`
		}
		if err := json.Unmarshal(got, &req); err != nil {
			t.Fatal(err)
		}
		if len(req.Tools) != 2 || req.Tools[0].Function.Name != "a" || req.Tools[1].Function.Name != "b" {
			t.Fatalf("tools not sorted: %s", got)
		}
		// bytes outside the tools array stay as the client sent them
		if !strings.Contains(string(got), `"temperature": 1.0, `) {
			t.Fatalf("whitespace/number form outside tools must be preserved: %s", got)
		}
	})

	t.Run("other path passes through", func(t *testing.T) {
		body := `{"model":"m"}`
		r := httptest.NewRequest(http.MethodPost, "/v1/completions", strings.NewReader(body))
		r.Header.Set("Content-Type", "application/json")
		c.ModifyRequest(r)
		got, _ := io.ReadAll(r.Body)
		if string(got) != body {
			t.Fatalf("body changed: %s", got)
		}
	})

	t.Run("non-JSON body passes through", func(t *testing.T) {
		body := `model=m`
		r := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(body))
		r.Header.Set("Content-Type", "text/plain")
		c.ModifyRequest(r)
		got, _ := io.ReadAll(r.Body)
		if string(got) != body {
			t.Fatalf("body changed: %s", got)
		}
	})

	t.Run("invalid JSON passes through unchanged", func(t *testing.T) {
		body := `{"model":`
		r := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(body))
		r.Header.Set("Content-Type", "application/json")
		c.ModifyRequest(r)
		got, _ := io.ReadAll(r.Body)
		if string(got) != body {
			t.Fatalf("body changed: %s", got)
		}
	})

	t.Run("GET passes through", func(t *testing.T) {
		r := httptest.NewRequest(http.MethodGet, "/v1/chat/completions", nil)
		before := r.Body
		c.ModifyRequest(r)
		if r.Body != before {
			t.Fatal("GET must not be touched")
		}
	})
}
