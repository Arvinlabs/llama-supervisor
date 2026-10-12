package anthropic

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
)

// sseEvents parses an Anthropic SSE stream into the ordered list of event
// names with their data payloads (as raw JSON)
func sseEvents(t *testing.T, body []byte) [][2]string {
	t.Helper()
	var events [][2]string
	name := ""
	for _, line := range strings.Split(string(body), "\n") {
		line = strings.TrimRight(line, "\r")
		switch {
		case strings.HasPrefix(line, "event: "):
			name = strings.TrimPrefix(line, "event: ")
		case strings.HasPrefix(line, "data: "):
			events = append(events, [2]string{name, strings.TrimPrefix(line, "data: ")})
		}
	}
	return events
}

func eventNames(events [][2]string) []string {
	names := make([]string, 0, len(events))
	for _, e := range events {
		names = append(names, e[0])
	}
	return names
}

// jsonField extracts a dotted field from a raw JSON payload (numeric path
// segments index into arrays); non-string values are rendered with %v
func jsonField(t *testing.T, raw, field string) string {
	t.Helper()
	var v any
	if err := json.Unmarshal([]byte(raw), &v); err != nil {
		t.Fatalf("parse %s: %v", raw, err)
	}
	cur := v
	for _, part := range strings.Split(field, ".") {
		switch m := cur.(type) {
		case map[string]any:
			cur = m[part]
		case []any:
			i, err := strconv.Atoi(part)
			if err != nil || i < 0 || i >= len(m) {
				return ""
			}
			cur = m[i]
		default:
			return ""
		}
	}
	switch s := cur.(type) {
	case nil:
		return ""
	case string:
		return s
	default:
		return fmt.Sprintf("%v", s)
	}
}

// claudeRequest builds a minimal Anthropic request body
func claudeRequest(stream bool) string {
	if stream {
		return `{"model":"default","max_tokens":100,"stream":true,"messages":[{"role":"user","content":"hi"}]}`
	}
	return `{"model":"default","max_tokens":100,"messages":[{"role":"user","content":"hi"}]}`
}

// newHandler builds a handler pointed at a fake self server
func newHandler(t *testing.T, self http.Handler, modelMap map[string]string) *Handler {
	t.Helper()
	ts := httptest.NewServer(self)
	t.Cleanup(ts.Close)
	return New(ts.URL, modelMap)
}

// post drives the handler with one Anthropic request and returns the recorder
func post(t *testing.T, h *Handler, key, body string) *httptest.ResponseRecorder {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, "http://supervisor.local/v1/messages", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	if key != "" {
		req.Header.Set("x-api-key", key)
	}
	rec := httptest.NewRecorder()
	if !h.Serve(rec, req) {
		t.Fatal("the handler must serve POST /v1/messages")
	}
	return rec
}

func TestHandleNonStream(t *testing.T) {
	var chatBody []byte
	h := newHandler(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		chatBody, _ = io.ReadAll(r.Body)
		if r.URL.Path != "/v1/chat/completions" {
			t.Errorf("unexpected self path %s", r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"id":"chatcmpl-1","object":"chat.completion","created":1,"model":"default",`+
			`"choices":[{"index":0,"message":{"role":"assistant","content":"hello"},"finish_reason":"stop"}],`+
			`"usage":{"prompt_tokens":5,"completion_tokens":2,"total_tokens":7}}`)
	}), nil)

	rec := post(t, h, "", claudeRequest(false))
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d", rec.Code)
	}
	body := rec.Body.Bytes()

	// the self request must be OpenAI chat completion JSON
	var oai struct {
		Model     string `json:"model"`
		MaxTokens *int   `json:"max_tokens"`
		Messages  []struct {
			Role    string `json:"role"`
			Content string `json:"content"`
		} `json:"messages"`
	}
	if err := json.Unmarshal(chatBody, &oai); err != nil {
		t.Fatalf("self request is not OpenAI JSON: %v\n%s", err, chatBody)
	}
	if oai.Model != "default" {
		t.Errorf("model = %q, want default", oai.Model)
	}
	if oai.MaxTokens == nil || *oai.MaxTokens != 100 {
		t.Errorf("max_tokens = %v, want 100", oai.MaxTokens)
	}
	if len(oai.Messages) != 1 || oai.Messages[0].Role != "user" || oai.Messages[0].Content != "hi" {
		t.Errorf("messages = %+v", oai.Messages)
	}

	// the client response must be an Anthropic message
	var msg map[string]any
	if err := json.Unmarshal(body, &msg); err != nil {
		t.Fatalf("response is not JSON: %v\n%s", err, body)
	}
	if msg["type"] != "message" || msg["role"] != "assistant" {
		t.Errorf("type/role = %v/%v", msg["type"], msg["role"])
	}
	if got := jsonField(t, string(body), "content.0.type"); got != "text" {
		t.Errorf("content[0].type = %q", got)
	}
	if got := jsonField(t, string(body), "content.0.text"); got != "hello" {
		t.Errorf("content[0].text = %q", got)
	}
	if got := jsonField(t, string(body), "stop_reason"); got != "end_turn" {
		t.Errorf("stop_reason = %q", got)
	}
	if got := jsonField(t, string(body), "usage.input_tokens"); got != "5" {
		t.Errorf("usage.input_tokens = %q", got)
	}
	if got := jsonField(t, string(body), "usage.output_tokens"); got != "2" {
		t.Errorf("usage.output_tokens = %q", got)
	}
}

func TestHandleStream(t *testing.T) {
	h := newHandler(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprint(w, "data: {\"id\":\"c1\",\"object\":\"chat.completion.chunk\",\"created\":1,\"model\":\"default\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"Hel\"},\"finish_reason\":null}]}\n\n")
		fmt.Fprint(w, "data: {\"id\":\"c1\",\"object\":\"chat.completion.chunk\",\"created\":1,\"model\":\"default\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"lo\"},\"finish_reason\":null}]}\n\n")
		fmt.Fprint(w, "data: {\"id\":\"c1\",\"object\":\"chat.completion.chunk\",\"created\":1,\"model\":\"default\",\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}],\"usage\":{\"prompt_tokens\":5,\"completion_tokens\":2,\"total_tokens\":7}}\n\n")
		fmt.Fprint(w, "data: [DONE]\n\n")
	}), nil)

	rec := post(t, h, "", claudeRequest(true))
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d", rec.Code)
	}
	body := rec.Body.Bytes()
	if ct := rec.Header().Get("Content-Type"); ct != "text/event-stream" {
		t.Errorf("content-type = %q", ct)
	}

	events := sseEvents(t, body)
	names := eventNames(events)
	want := []string{"message_start", "content_block_start", "content_block_delta", "content_block_delta",
		"content_block_stop", "message_delta", "message_stop"}
	if strings.Join(names, ",") != strings.Join(want, ",") {
		t.Fatalf("events = %v, want %v\nraw: %s", names, want, body)
	}
	if got := jsonField(t, events[1][1], "content_block.type"); got != "text" {
		t.Errorf("block type = %q", got)
	}
	if got := jsonField(t, events[2][1], "delta.text"); got != "Hel" {
		t.Errorf("first delta = %q", got)
	}
	if got := jsonField(t, events[5][1], "delta.stop_reason"); got != "end_turn" {
		t.Errorf("stop_reason = %q", got)
	}
	if got := jsonField(t, events[5][1], "usage.output_tokens"); got != "2" {
		t.Errorf("usage.output_tokens = %q", got)
	}
}

// TestHandleStreamEarlyClose covers a stream that ends with [DONE] before the
// final usage chunk: the terminal events must still be emitted
func TestHandleStreamEarlyClose(t *testing.T) {
	h := newHandler(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprint(w, "data: {\"id\":\"c1\",\"object\":\"chat.completion.chunk\",\"created\":1,\"model\":\"default\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"x\"},\"finish_reason\":null}]}\n\n")
		fmt.Fprint(w, "data: [DONE]\n\n")
	}), nil)

	rec := post(t, h, "", claudeRequest(true))
	body := rec.Body.Bytes()
	names := eventNames(sseEvents(t, body))
	if !strings.Contains(strings.Join(names, ","), "message_stop") {
		t.Errorf("missing terminal message_stop: %v\nraw: %s", names, body)
	}
}

// TestHandleStreamInjectedError covers the pipeline's injected stream error
// event: it must be relayed as an Anthropic error event
func TestHandleStreamInjectedError(t *testing.T) {
	h := newHandler(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprint(w, "data: {\"id\":\"c1\",\"object\":\"chat.completion.chunk\",\"created\":1,\"model\":\"default\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"x\"},\"finish_reason\":null}]}\n\n")
		// the exact event the pipeline's guardBody injects
		fmt.Fprint(w, "\n\ndata: {\"error\":{\"message\":\"stream interrupted\",\"type\":\"server_error\",\"code\":\"stream_interrupted\"}}\n\n")
	}), nil)

	rec := post(t, h, "", claudeRequest(true))
	body := rec.Body.Bytes()
	events := sseEvents(t, body)
	if got := eventNames(events); len(got) == 0 || got[len(got)-1] != "error" {
		t.Fatalf("last event = %v, want error\nraw: %s", got, body)
	}
	if got := jsonField(t, events[len(events)-1][1], "error.message"); got != "stream interrupted" {
		t.Errorf("error.message = %q", got)
	}
}

// TestHandleKeyPassthrough: the handler does no auth of its own; the client's
// key is passed through to the self request verbatim, so the internal
// pipeline's request policy is the single authentication point
func TestHandleKeyPassthrough(t *testing.T) {
	var gotAuth string
	h := newHandler(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"id":"c1","object":"chat.completion","created":1,"model":"default","choices":[{"index":0,"message":{"role":"assistant","content":"ok"},"finish_reason":"stop"}],"usage":{}}`)
	}), nil)

	// the Anthropic x-api-key header becomes an Authorization header
	rec := post(t, h, "vk-1", claudeRequest(false))
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d", rec.Code)
	}
	if gotAuth != "Bearer vk-1" {
		t.Errorf("self request auth = %q, want Bearer vk-1", gotAuth)
	}

	// a client Authorization header passes through unchanged
	gotAuth = ""
	req, _ := http.NewRequest(http.MethodPost, "http://supervisor.local/v1/messages", strings.NewReader(claudeRequest(false)))
	req.Header.Set("Authorization", "raw-key")
	h.Serve(httptest.NewRecorder(), req)
	if gotAuth != "raw-key" {
		t.Errorf("self request auth = %q, want raw-key (verbatim)", gotAuth)
	}

	// no key: the header is absent, the inner request policy decides
	gotAuth = "sentinel"
	req, _ = http.NewRequest(http.MethodPost, "http://supervisor.local/v1/messages", strings.NewReader(claudeRequest(false)))
	h.Serve(httptest.NewRecorder(), req)
	if gotAuth != "" {
		t.Errorf("self request auth = %q, want empty", gotAuth)
	}
}

func TestHandleModelMap(t *testing.T) {
	var model string
	h := newHandler(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var in struct {
			Model string `json:"model"`
		}
		_ = json.NewDecoder(r.Body).Decode(&in)
		model = in.Model
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"id":"c1","object":"chat.completion","created":1,"model":"backend-model","choices":[{"index":0,"message":{"role":"assistant","content":"ok"},"finish_reason":"stop"}],"usage":{}}`)
	}), map[string]string{"claude-sonnet-4-5": "backend-model"})

	rec := post(t, h, "", `{"model":"claude-sonnet-4-5","max_tokens":10,"messages":[{"role":"user","content":"hi"}]}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d", rec.Code)
	}
	if model != "backend-model" {
		t.Errorf("self request model = %q, want backend-model", model)
	}
}

func TestHandleUpstreamError(t *testing.T) {
	h := newHandler(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, `{"error":{"message":"backend unavailable","type":"server_error"}}`, http.StatusBadGateway)
	}), nil)

	rec := post(t, h, "", claudeRequest(false))
	body := rec.Body.Bytes()
	// the Anthropic API maps upstream failures to 500, not 502
	if rec.Code != http.StatusInternalServerError {
		t.Errorf("status = %d, want 500", rec.Code)
	}
	var errBody map[string]any
	_ = json.Unmarshal(body, &errBody)
	if errBody["type"] != "error" {
		t.Errorf("error body = %s", body)
	}
	if got := jsonField(t, string(body), "error.message"); got != "backend unavailable" {
		t.Errorf("error.message = %q", got)
	}
}

func TestHandleInvalidBody(t *testing.T) {
	h := newHandler(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Error("the self server must not be reached for an invalid body")
	}), nil)

	for _, body := range []string{`not json`, `{}`, `{"max_tokens":10,"messages":[]}`} {
		rec := post(t, h, "", body)
		if rec.Code != http.StatusBadRequest {
			t.Errorf("body %q: status %d, want 400", body, rec.Code)
		}
	}
}

// TestHandleWrongPathAndMethod ensures non-matching requests fall through
func TestHandleWrongPathAndMethod(t *testing.T) {
	h := newHandler(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}), nil)
	if h.Serve(httptest.NewRecorder(), mustGet(t, "http://supervisor.local/v1/messages")) {
		t.Error("GET must not be served by the anthropic handler")
	}
	if h.Serve(httptest.NewRecorder(), mustGet(t, "http://supervisor.local/other")) {
		t.Error("wrong path must not be served by the anthropic handler")
	}
}

func mustGet(t *testing.T, url string) *http.Request {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		t.Fatal(err)
	}
	return req
}
