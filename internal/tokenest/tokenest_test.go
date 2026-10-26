package tokenest

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// tokenizeServer serves the backend's POST /tokenize, returning n tokens and
// checking the prompt text it was given
func tokenizeServer(t *testing.T, n int, wantText string) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/tokenize" {
			http.NotFound(w, r)
			return
		}
		body, _ := io.ReadAll(r.Body)
		if wantText != "" && !strings.Contains(string(body), `"content":"`+wantText+`"`) {
			t.Errorf("/tokenize content = %s, want the prompt text %q", body, wantText)
		}
		var sb strings.Builder
		sb.WriteString(`{"tokens":[`)
		for i := 0; i < n; i++ {
			if i > 0 {
				sb.WriteString(",")
			}
			sb.WriteString("1")
		}
		sb.WriteString(`]}`)
		_, _ = w.Write([]byte(sb.String()))
	}))
}

func TestExtractPrompt(t *testing.T) {
	body := []byte(`{
		"system":"sys",
		"tools":[{"type":"function","function":{"name":"f","description":"d"}}],
		"messages":[
			{"role":"user","content":[{"type":"text","text":"hi"},{"type":"image_url","image_url":{"url":"x"}}]},
			{"role":"assistant","content":"ok","tool_calls":[{"function":{"name":"f","arguments":"{\"a\":1}"}}]},
			{"role":"user","content":"world"}
		]
	}`)
	text, images, nMsg := extractPrompt(body)
	if images != 1 {
		t.Errorf("images = %d, want 1", images)
	}
	if nMsg != 3 {
		t.Errorf("nMsg = %d, want 3", nMsg)
	}
	for _, want := range []string{"sys", "hi", "ok", "f", `{"a":1}`, "world", `"description":"d"`} {
		if !strings.Contains(text, want) {
			t.Errorf("extracted text %q missing %q", text, want)
		}
	}
}

// the count is the backend's own: the prompt text is sent to POST /tokenize and
// its token list is what the estimate reports (plus the fixed parts)
func TestEstimateExactFromBackend(t *testing.T) {
	srv := tokenizeServer(t, 5, "hello world")
	defer srv.Close()
	e := New(srv.URL, "")
	body := []byte(`{"messages":[{"role":"user","content":"hello world"}]}`)
	// 5 exact tokens + 0 images + 1 message * 4 overhead
	if got, err := e.Estimate(context.Background(), body); err != nil || got != 9 {
		t.Fatalf("Estimate = %d, %v; want 9, nil", got, err)
	}
}

func TestEstimateSendsTheBackendKey(t *testing.T) {
	var auth string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		auth = r.Header.Get("Authorization")
		_, _ = w.Write([]byte(`{"tokens":[1]}`))
	}))
	defer srv.Close()
	e := New(srv.URL, "sk-local")
	if _, err := e.Estimate(context.Background(), []byte(`{"messages":[{"role":"user","content":"x"}]}`)); err != nil {
		t.Fatal(err)
	}
	if auth != "Bearer sk-local" {
		t.Fatalf("/tokenize Authorization = %q, want the global key", auth)
	}
}

// no heuristic fallback: without a working tokenizer the estimate is an error
func TestEstimateFailsWithoutBackend(t *testing.T) {
	e := New("", "")
	body := []byte(`{"messages":[{"role":"user","content":"hello world"}]}`)
	if n, err := e.Estimate(context.Background(), body); err == nil {
		t.Fatalf("Estimate = %d, nil; want an error (no tokenizer configured)", n)
	}
}

func TestEstimateFailsWhenTokenizeFails(t *testing.T) {
	for name, status := range map[string]int{
		"server error":      http.StatusInternalServerError,
		"not found":         http.StatusNotFound,
		"bad request":       http.StatusBadRequest,
		"unavailable (429)": http.StatusTooManyRequests,
		"gateway (502)":     http.StatusBadGateway,
	} {
		t.Run(name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(status)
			}))
			defer srv.Close()
			e := New(srv.URL, "")
			body := []byte(`{"messages":[{"role":"user","content":"hello world"}]}`)
			if n, err := e.Estimate(context.Background(), body); err == nil {
				t.Fatalf("Estimate = %d, nil; want an error (tokenize answered %d)", n, status)
			}
		})
	}
}

// an empty token list means the backend could not tokenize the text: an error,
// never a silent zero
func TestEstimateFailsOnEmptyTokenList(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"tokens":[]}`))
	}))
	defer srv.Close()
	e := New(srv.URL, "")
	if n, err := e.Estimate(context.Background(), []byte(`{"messages":[{"role":"user","content":"hi"}]}`)); err == nil {
		t.Fatalf("Estimate = %d, nil; want an error", n)
	}
}

// the image parts and the per-message overhead are added to the backend's text
// count, and a body without text needs no tokenize call at all
func TestEstimateFixedParts(t *testing.T) {
	srv := tokenizeServer(t, 1, "")
	defer srv.Close()
	e := New(srv.URL, "")
	body := []byte(`{"messages":[{"role":"user","content":[{"type":"text","text":"hi"},{"type":"image_url","image_url":{"url":"x"}}]}]}`)
	// 1 exact token + 1 image * 256 + 1 message * 4 overhead
	if got, err := e.Estimate(context.Background(), body); err != nil || got != 1+defaultImageTokens+perMessageOverhead {
		t.Fatalf("Estimate = %d, %v; want %d, nil", got, err, 1+defaultImageTokens+perMessageOverhead)
	}

	noText := New("", "") // unreachable: no text means no tokenize call
	body = []byte(`{"messages":[{"role":"user","content":[{"type":"image_url","image_url":{"url":"x"}}]}]}`)
	if got, err := noText.Estimate(context.Background(), body); err != nil || got != defaultImageTokens+perMessageOverhead {
		t.Fatalf("Estimate = %d, %v; want %d, nil", got, err, defaultImageTokens+perMessageOverhead)
	}
	// a body without messages cannot be judged by the caller (0)
	if got, err := noText.Estimate(context.Background(), []byte(`{}`)); err != nil || got != 0 {
		t.Fatalf("Estimate({}) = %d, %v; want 0, nil", got, err)
	}
}
