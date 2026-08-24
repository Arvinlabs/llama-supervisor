package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strconv"
	"strings"
	"testing"
)

// object keys in the normalized body must be sorted at every level (Go marshals maps with sorted keys)
func TestNormalizeChatCompletionsSortedKeys(t *testing.T) {
	in := []byte(`{"stream":true,"model":"m","max_tokens":32,"messages":[{"role":"user","content":"hi"}]}`)
	out, err := normalizeChatCompletions(in)
	if err != nil {
		t.Fatal(err)
	}
	got := keyOrder(t, out)
	want := []string{"max_tokens", "messages", "model", "stream"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("top-level key order = %v, want %v", got, want)
	}
}

// the tools list is sorted by function name and tool parameter schema keys are sorted
func TestNormalizeChatCompletionsSortsTools(t *testing.T) {
	in := []byte(`{"model":"m","tools":[` +
		`{"type":"function","function":{"name":"zebra","description":"z","parameters":{"type":"object","properties":{"b":{},"a":{}}}}},` +
		`{"type":"function","function":{"name":"alpha","description":"a","parameters":{"type":"object","properties":{"y":{},"x":{}}}}}` +
		`],"messages":[{"role":"user","content":"hi"}]}`)
	out, err := normalizeChatCompletions(in)
	if err != nil {
		t.Fatal(err)
	}
	var req struct {
		Tools []struct {
			Function struct {
				Name       string `json:"name"`
				Parameters struct {
					Properties map[string]any `json:"properties"`
				} `json:"parameters"`
			} `json:"function"`
		} `json:"tools"`
	}
	if err := json.Unmarshal(out, &req); err != nil {
		t.Fatal(err)
	}
	if len(req.Tools) != 2 || req.Tools[0].Function.Name != "alpha" || req.Tools[1].Function.Name != "zebra" {
		t.Fatalf("tools not sorted by function name: %+v", req.Tools)
	}
	// parameter keys sorted: in the raw bytes "x" must come before "y", "a" before "b"
	s := string(out)
	if !strings.Contains(s, `"x":{},"y":{}`) || !strings.Contains(s, `"a":{},"b":{}`) {
		t.Fatalf("tool parameter keys not sorted: %s", s)
	}
}

// custom tools (type "custom") carry the name under custom.name and must sort with function tools
func TestNormalizeChatCompletionsSortsCustomTools(t *testing.T) {
	in := []byte(`{"model":"m","tools":[` +
		`{"type":"custom","custom":{"name":"zeta","format":{"type":"text"}}},` +
		`{"type":"function","function":{"name":"mid","parameters":{"type":"object"}}},` +
		`{"type":"custom","custom":{"name":"alpha","format":{"type":"text"}}}` +
		`],"messages":[{"role":"user","content":"hi"}]}`)
	out, err := normalizeChatCompletions(in)
	if err != nil {
		t.Fatal(err)
	}
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
		typ, ok := want[name]
		if !ok || tl.Type != typ {
			t.Fatalf("tools[%d] = %s/%s, want %s: %s", i, tl.Type, name, typ, out)
		}
	}
}

// message order is semantically significant and must be preserved
func TestNormalizeChatCompletionsPreservesMessages(t *testing.T) {
	in := []byte(`{"model":"m","messages":[{"role":"user","content":"second"},{"role":"system","content":"first"}]}`)
	out, err := normalizeChatCompletions(in)
	if err != nil {
		t.Fatal(err)
	}
	var req struct {
		Messages []struct {
			Role    string `json:"role"`
			Content string `json:"content"`
		} `json:"messages"`
	}
	if err := json.Unmarshal(out, &req); err != nil {
		t.Fatal(err)
	}
	if len(req.Messages) != 2 || req.Messages[0].Content != "second" || req.Messages[1].Content != "first" {
		t.Fatalf("message order changed: %+v", req.Messages)
	}
}

// semantically identical requests with different tools/key order produce identical bytes
func TestNormalizeChatCompletionsCanonicalForm(t *testing.T) {
	a := []byte(`{"model":"m","tools":[{"type":"function","function":{"name":"a","parameters":{"type":"object","properties":{"p":{}}}}}],"messages":[{"role":"user","content":"hi"}]}`)
	b := []byte(`{"messages":[{"role":"user","content":"hi"}],"model":"m","tools":[{"type":"function","function":{"parameters":{"properties":{"p":{}},"type":"object"},"name":"a"}}]}`)
	oa, err := normalizeChatCompletions(a)
	if err != nil {
		t.Fatal(err)
	}
	ob, err := normalizeChatCompletions(b)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(oa, ob) {
		t.Fatalf("canonical forms differ:\n%s\n%s", oa, ob)
	}
}

// invalid JSON must fail (modifyRequest then passes the body through unchanged)
func TestNormalizeChatCompletionsInvalidJSON(t *testing.T) {
	if _, err := normalizeChatCompletions([]byte(`{"model":`)); err == nil {
		t.Fatal("expected error for invalid JSON")
	}
}

// modifyRequest only normalizes POST /v1/chat/completions with a JSON body
func TestPrefixCacheModifyRequest(t *testing.T) {
	c := newPrefixCachePolicy()

	t.Run("normalizes chat completions", func(t *testing.T) {
		body := `{"model":"m","tools":[{"type":"function","function":{"name":"b","parameters":{"type":"object","properties":{"q":{}}}}},{"type":"function","function":{"name":"a","parameters":{"type":"object","properties":{"q":{}}}}}],"messages":[{"role":"user","content":"hi"}]}`
		r := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(body))
		r.Header.Set("Content-Type", "application/json")
		c.modifyRequest(r)
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
		}
		if err := json.Unmarshal(got, &req); err != nil {
			t.Fatal(err)
		}
		if len(req.Tools) != 2 || req.Tools[0].Function.Name != "a" || req.Tools[1].Function.Name != "b" {
			t.Fatalf("tools not sorted: %+v", req.Tools)
		}
	})

	t.Run("other path passes through", func(t *testing.T) {
		body := `{"model":"m"}`
		r := httptest.NewRequest(http.MethodPost, "/v1/completions", strings.NewReader(body))
		r.Header.Set("Content-Type", "application/json")
		c.modifyRequest(r)
		got, _ := io.ReadAll(r.Body)
		if string(got) != body {
			t.Fatalf("body changed: %s", got)
		}
	})

	t.Run("non-JSON body passes through", func(t *testing.T) {
		body := `model=m`
		r := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(body))
		r.Header.Set("Content-Type", "text/plain")
		c.modifyRequest(r)
		got, _ := io.ReadAll(r.Body)
		if string(got) != body {
			t.Fatalf("body changed: %s", got)
		}
	})

	t.Run("invalid JSON passes through unchanged", func(t *testing.T) {
		body := `{"model":`
		r := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(body))
		r.Header.Set("Content-Type", "application/json")
		c.modifyRequest(r)
		got, _ := io.ReadAll(r.Body)
		if string(got) != body {
			t.Fatalf("body changed: %s", got)
		}
	})

	t.Run("GET passes through", func(t *testing.T) {
		r := httptest.NewRequest(http.MethodGet, "/v1/chat/completions", nil)
		before := r.Body
		c.modifyRequest(r)
		if r.Body != before {
			t.Fatal("GET must not be touched")
		}
	})
}

// end-to-end: with the prefix cache modifier enabled the proxy forwards the normalized body to the backend
func TestProxyForwardsNormalizedBody(t *testing.T) {
	var backendBody []byte
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		backendBody, _ = io.ReadAll(r.Body)
		w.Write([]byte(`{"ok":true}`))
	}))
	defer backend.Close()

	sup := newBackendProxy(Config{Backend: backend.URL, Request: &RequestGroup{PrefixCache: true}}, context.Background())
	body := `{"model":"m","tools":[{"type":"function","function":{"name":"z","parameters":{"type":"object","properties":{"b":{},"a":{}}}}},{"type":"function","function":{"name":"a","parameters":{"type":"object","properties":{"c":{}}}}}],"messages":[{"role":"user","content":"hi"}]}`
	r := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(body))
	r.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	sup.ServeHTTP(w, r)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d", w.Code)
	}
	var req struct {
		Tools []struct {
			Function struct {
				Name string `json:"name"`
			} `json:"function"`
		} `json:"tools"`
	}
	if err := json.Unmarshal(backendBody, &req); err != nil {
		t.Fatalf("backend body not JSON: %v: %s", err, backendBody)
	}
	if len(req.Tools) != 2 || req.Tools[0].Function.Name != "a" || req.Tools[1].Function.Name != "z" {
		t.Fatalf("backend received unsorted tools: %s", backendBody)
	}
	if s := string(backendBody); !strings.Contains(s, `"name":"z","parameters":{"properties":{"a":{},"b":{}},"type":"object"`) {
		t.Fatalf("backend received unsorted keys: %s", s)
	}
}

// keyOrder returns the object key order of a top-level JSON object
func keyOrder(t *testing.T, data []byte) []string {
	t.Helper()
	dec := json.NewDecoder(bytes.NewReader(data))
	tok, err := dec.Token()
	if err != nil {
		t.Fatalf("first token: %v", err)
	}
	if d, ok := tok.(json.Delim); !ok || d != '{' {
		t.Fatalf("expected top-level object, got %v", tok)
	}
	var keys []string
	for dec.More() {
		ktok, err := dec.Token()
		if err != nil {
			t.Fatalf("key token: %v", err)
		}
		k, ok := ktok.(string)
		if !ok {
			t.Fatalf("expected object key")
		}
		keys = append(keys, k)
		var v any
		if err := dec.Decode(&v); err != nil {
			t.Fatalf("decode value: %v", err)
		}
	}
	return keys
}
