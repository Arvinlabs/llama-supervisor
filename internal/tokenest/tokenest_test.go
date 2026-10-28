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
			{"role":"assistant","reasoning_content":"let me think","content":"ok","tool_calls":[{"function":{"name":"f","arguments":"{\"a\":1}"}}]},
			{"role":"user","content":"world"}
		]
	}`)
	text, images, nMsg, nCalls, toolsBytes := extractPrompt(body)
	if images != 1 {
		t.Errorf("images = %d, want 1", images)
	}
	if nMsg != 3 {
		t.Errorf("nMsg = %d, want 3", nMsg)
	}
	if nCalls != 1 {
		t.Errorf("nCalls = %d, want 1", nCalls)
	}
	// the tool call goes in as its raw JSON, not split into name/arguments
	if !strings.Contains(text, `{"function":{"name":"f","arguments":"{\"a\":1}"}}`) {
		t.Errorf("extracted text %q missing the raw tool call JSON", text)
	}
	if toolsBytes == 0 {
		t.Errorf("toolsBytes = 0, want the tools definitions' raw size")
	}
	for _, want := range []string{"sys", "hi", "ok", "f", "world", `"description":"d"`, "let me think"} {
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
	if got, err := e.Estimate(context.Background(), body); err != nil || got.Tokens != 9 || got.Text != 5 || got.NMsg != 1 {
		t.Fatalf("Estimate = %+v, %v; want Tokens 9, Text 5, NMsg 1, nil", got, err)
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

// no heuristic guess: without a working tokenizer the estimate is an error
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
	if got, err := e.Estimate(context.Background(), body); err != nil || got.Tokens != 1+defaultImageTokens+perMessageOverhead || got.Images != 1 {
		t.Fatalf("Estimate = %+v, %v; want Tokens %d, Images 1, nil", got, err, 1+defaultImageTokens+perMessageOverhead)
	}

	noText := New("", "") // unreachable: no text means no tokenize call
	body = []byte(`{"messages":[{"role":"user","content":[{"type":"image_url","image_url":{"url":"x"}}]}]}`)
	if got, err := noText.Estimate(context.Background(), body); err != nil || got.Tokens != defaultImageTokens+perMessageOverhead {
		t.Fatalf("Estimate = %+v, %v; want Tokens %d, nil", got, err, defaultImageTokens+perMessageOverhead)
	}
	// a body without messages cannot be judged by the caller (0)
	if got, err := noText.Estimate(context.Background(), []byte(`{}`)); err != nil || got.Tokens != 0 {
		t.Fatalf("Estimate({}) = %+v, %v; want Tokens 0, nil", got, err)
	}
}

// the tiktoken counter counts the text locally with the OpenAI family's BPE
// encoding (cl100k_base): no backend round-trip, so the estimate works even
// though the estimator is pointed at nothing, and the fixed parts still apply
func TestEstimateTiktokenNeedsNoBackend(t *testing.T) {
	e := NewTiktoken("")
	body := []byte(`{"messages":[{"role":"user","content":"hello world"}]}`)
	// cl100k_base encodes "hello world" as two tokens
	if got, err := e.Estimate(context.Background(), body); err != nil || got.Tokens != 2+perMessageOverhead || got.Text != 2 || got.NMsg != 1 {
		t.Fatalf("Estimate = %+v, %v; want Tokens %d, Text 2, NMsg 1, nil", got, err, 2+perMessageOverhead)
	}
}

// an empty model name, a name tiktoken does not know, and a known name whose
// encoding is cl100k_base must all yield the same count
func TestEstimateTiktokenModelFallsBackToCl100k(t *testing.T) {
	body := []byte(`{"messages":[{"role":"user","content":"hello world 你好"}]}`)
	var want int
	for i, model := range []string{"", "gpt-4", "no/such-model"} {
		got, err := NewTiktoken(model).Estimate(context.Background(), body)
		if err != nil || got.Text == 0 {
			t.Fatalf("%d: Estimate(%q) = %+v, %v; want a non-zero count", i, model, got, err)
		}
		if i == 0 {
			want = got.Text
		} else if got.Text != want {
			t.Fatalf("%d: Estimate(%q) Text = %d, want %d (cl100k_base)", i, model, got.Text, want)
		}
	}
}

func TestRealRequestBody(t *testing.T) {
	// the user's real request; the base64 payload is shortened - extractPrompt
	// only counts image parts, it never reads the URL value, so parsing is
	// byte-identical for the purposes of token counting
	body := []byte(`{
  "model": "qwen-local",
  "messages": [
    {"role": "system", "content": "You are a helpful assistant.\nCurrent date: 2026-10-26\n"},
    {"role": "user", "content": "hi"},
    {"role": "assistant", "content": "Hey! 👋 How's it going? What can I help you with today?",
     "reasoning_content": "The user said \"hi\" - a simple greeting. I should respond warmly and casually.\n"},
    {"role": "user", "content": [
      {"type": "text", "text": "你好"},
      {"type": "image_url", "image_url": {"url": "data:image/png;base64,iVBORw0KGgoshortened"}}
    ]}
  ],
  "stream": true,
  "tools": [
    {"type": "function", "function": {"name": "calc",
     "description": "Evaluate a math expression. Supports + - * / // % ( ), ^ or ** for power (e.g. 2^10, right-associative), sqrt(x) or √x for square root (e.g. √16, √(2+2)). Example: 2^10+√16",
     "parameters": {"properties": {"expr": {"type": "string"}}, "required": ["expr"], "type": "object"}}}
  ],
  "stream_options": {"include_usage": true},
  "reasoning_effort": "medium"
}`)

	text, images, nMsg, _, toolsBytes := extractPrompt(body)
	t.Logf("images=%d nMsg=%d toolsBytes=%d", images, nMsg, toolsBytes)
	t.Logf("extracted text (%d bytes):\n%s", len(text), text)

	if images != 1 {
		t.Errorf("images = %d, want 1", images)
	}
	if nMsg != 4 {
		t.Errorf("nMsg = %d, want 4", nMsg)
	}
	if toolsBytes <= 0 {
		t.Errorf("toolsBytes = %d, want > 0", toolsBytes)
	}
	// every piece of prompt text must be present exactly once
	for _, want := range []string{
		"You are a helpful assistant.",
		"Current date: 2026-10-26",
		"hi",
		`The user said "hi" - a simple greeting. I should respond warmly and casually.`, // the sent-back reasoning
		"How's it going?",
		"你好",
		"calc",
		"Evaluate a math expression",
	} {
		if !strings.Contains(text, want) {
			t.Errorf("extracted text missing %q", want)
		}
	}
	// nothing that is not prompt text may leak in
	for _, bad := range []string{
		`"role"`,           // JSON structure
		"qwen-local",       // model field
		"stream_options",   // request parameters
		"reasoning_effort", // request parameter
		"include_usage",    // request parameter
		"iVBORw0KGgo",      // image base64 (counted as a flat part, not texted)
	} {
		if strings.Contains(text, bad) {
			t.Errorf("extracted text wrongly contains %q", bad)
		}
	}

	// end-to-end estimate with a mock tokenizer
	var sawText string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		sawText = string(b)
		_, _ = w.Write([]byte(`{"tokens":[1,2,3]}`))
	}))
	defer srv.Close()
	e := New(srv.URL, "")
	got, err := e.Estimate(context.Background(), body)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("Result: %+v", got)
	if got.Text != 3 {
		t.Errorf("Text = %d, want the mock's 3", got.Text)
	}
	want := 3 + defaultImageTokens + perMessageOverhead*4
	if got.Tokens != want {
		t.Errorf("Tokens = %d, want %d", got.Tokens, want)
	}
	if !strings.Contains(sawText, "warmly") {
		t.Errorf("/tokenize payload missing the sent-back reasoning text")
	}
	if strings.Contains(sawText, "iVBORw0KGgo") {
		t.Errorf("/tokenize payload contains image base64")
	}
}

func TestToolsDefinitionsAreTexted(t *testing.T) {
	body := []byte(`{
  "model": "qwen-local",
  "messages": [
    {"role": "system", "content": "You are a helpful assistant.\nCurrent date: 2026-10-26\n"},
    {"role": "user", "content": "hi"}
  ],
  "stream": true,
  "tools": [
    {"type": "function", "function": {"name": "calc",
     "description": "Evaluate a math expression. Supports + - * / // % ( ), ^ or ** for power (e.g. 2^10, right-associative), sqrt(x) or \u221ax for square root (e.g. \u221a16, \u221a(2+2)). Example: 2^10+\u221a16",
     "parameters": {"properties": {"expr": {"type": "string"}}, "required": ["expr"], "type": "object"}}}
  ],
  "stream_options": {"include_usage": true},
  "reasoning_effort": "medium"
}`)

	text, _, nMsg, nCalls, toolsBytes := extractPrompt(body)
	if nMsg != 2 || nCalls != 0 {
		t.Fatalf("nMsg=%d nCalls=%d, want 2 0", nMsg, nCalls)
	}
	if toolsBytes != 381 {
		t.Errorf("toolsBytes = %d, want 381 (the raw tools array)", toolsBytes)
	}
	for _, want := range []string{`{"type": "function"`, "calc", "Evaluate a math expression", `"expr"`, `"required"`} {
		if !strings.Contains(text, want) {
			t.Errorf("extracted text missing the tools content %q", want)
		}
	}

	// end-to-end: the tools JSON must reach the /tokenize payload
	var saw string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		saw = string(b)
		_, _ = w.Write([]byte(`{"tokens":[1]}`))
	}))
	defer srv.Close()
	got, err := New(srv.URL, "").Estimate(context.Background(), body)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(saw, "Evaluate a math expression") {
		t.Errorf("/tokenize payload missing the tools definitions")
	}
	// 1 (text) + 0 images + 4 (nMsg 2) * 4 overhead
	if got.Tokens != 1+perMessageOverhead*2 {
		t.Errorf("Tokens = %d, want %d", got.Tokens, 1+perMessageOverhead*2)
	}
	t.Logf("Result: %+v", got)
}
