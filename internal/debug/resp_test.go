package debug

import (
	"encoding/json"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// assembleCompletion merges the chunks into the non-streaming structure; a null content
// in the first delta is ignored, usage/timings come from the final chunk; a stream
// without any choice chunk (e.g. interrupted before generation) yields (false)
func TestAssembleCompletion(t *testing.T) {
	events := parseSSEEvents([]byte(
		`data: {"choices":[{"finish_reason":null,"index":0,"delta":{"role":"assistant","content":null}}],"created":1789557159,"id":"chatcmpl-x","model":"qwen-local","system_fingerprint":"b1","object":"chat.completion.chunk"}` + "\n\n" +
			`data: {"choices":[{"finish_reason":null,"index":0,"delta":{"content":"Hello"}}]}` + "\n\n" +
			`data: {"choices":[{"finish_reason":"stop","index":0,"delta":{}}]}` + "\n\n" +
			`data: {"choices":[],"usage":{"prompt_tokens":24,"completion_tokens":10,"total_tokens":34},"timings":{"draft_n":6,"draft_n_accepted":6}}` + "\n\n" +
			`data: [DONE]` + "\n\n",
	))
	out, ok := assembleCompletion(events)
	if !ok {
		t.Fatal("want the assembled completion")
	}
	m, _ := out.(map[string]any)
	if m["object"] != "chat.completion" || m["id"] != "chatcmpl-x" || m["model"] != "qwen-local" || m["system_fingerprint"] != "b1" || m["created"] != json.Number("1789557159") {
		t.Fatalf("envelope not merged: %v", m)
	}
	chs, _ := m["choices"].([]any)
	if len(chs) != 1 {
		t.Fatalf("want 1 choice, got %d", len(chs))
	}
	c, _ := chs[0].(map[string]any)
	msg, _ := c["message"].(map[string]any)
	if c["finish_reason"] != "stop" || msg["content"] != "Hello" || msg["role"] != "assistant" {
		t.Fatalf("choice not merged: %v", c)
	}
	if _, ok := m["usage"].(map[string]any); !ok {
		t.Fatalf("usage not merged: %v", m)
	}
	if _, ok := m["timings"].(map[string]any); !ok {
		t.Fatalf("timings not merged: %v", m)
	}
	if _, ok := m["object"]; !ok || m["object"] != "chat.completion" {
		t.Fatal("object must be chat.completion")
	}

	// interrupted before any choice chunk: no choices, no finish, no usage
	partial := parseSSEEvents([]byte("data: {}\n\ndata: [DONE]\n\n"))
	if _, ok := assembleCompletion(partial); ok {
		t.Fatal("a chunkless stream must not assemble")
	}
	if _, ok := assembleCompletion(nil); ok {
		t.Fatal("an empty event list must not assemble")
	}
}

// assembleCompletion merges the tool_calls deltas: id/type arrive in the first fragment,
// the name/arguments strings accumulate per call index (parallel calls interleaved)
func TestAssembleToolCalls(t *testing.T) {
	events := parseSSEEvents([]byte(
		`data: {"choices":[{"index":0,"delta":{"role":"assistant","content":null,"tool_calls":[{"index":0,"id":"call_1","type":"function","function":{"name":"get_","arguments":""}}]},"finish_reason":null}]}` + "\n\n" +
			`data: {"choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"function":{"name":"weather","arguments":"{\"lo"}},{"index":1,"id":"call_2","type":"function","function":{"name":"get_","arguments":""}}]},"finish_reason":null}]}` + "\n\n" +
			`data: {"choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"function":{"arguments":"cation\":\"SF\"}"}}]},"finish_reason":null}]}` + "\n\n" +
			`data: {"choices":[{"index":0,"delta":{"tool_calls":[{"index":1,"function":{"name":"time","arguments":"{\"zone\":\"UTC\"}"}}]},"finish_reason":null}]}` + "\n\n" +
			`data: {"choices":[{"index":0,"delta":{},"finish_reason":"tool_calls"}]}` + "\n\n" +
			`data: [DONE]` + "\n\n",
	))
	out, ok := assembleCompletion(events)
	if !ok {
		t.Fatal("want the assembled completion")
	}
	m, _ := out.(map[string]any)
	chs, _ := m["choices"].([]any)
	if len(chs) != 1 {
		t.Fatalf("want 1 choice, got %d", len(chs))
	}
	c, _ := chs[0].(map[string]any)
	if c["finish_reason"] != "tool_calls" {
		t.Fatalf("finish_reason = %v", c["finish_reason"])
	}
	msg, _ := c["message"].(map[string]any)
	if msg["role"] != "assistant" {
		t.Fatalf("role not merged: %v", msg)
	}
	if _, ok := msg["content"]; ok {
		t.Fatalf("a null content must not be set: %v", msg)
	}
	tcs, _ := msg["tool_calls"].([]any)
	if len(tcs) != 2 {
		t.Fatalf("want 2 tool calls, got %d: %v", len(tcs), msg)
	}
	wants := [][3]string{
		{"call_1", "get_weather", `{"location":"SF"}`},
		{"call_2", "get_time", `{"zone":"UTC"}`},
	}
	for i, want := range wants {
		tc, _ := tcs[i].(map[string]any)
		fn, _ := tc["function"].(map[string]any)
		if tc["id"] != want[0] || tc["type"] != "function" || tc["index"] != float64(i) ||
			fn["name"] != want[1] || fn["arguments"] != want[2] {
			t.Fatalf("tool call %d not merged: %v", i, tc)
		}
	}
}

// parseSSEEvents: JSON payloads are parsed, non-JSON payloads (e.g. [DONE]) are kept as
// raw strings, non-data lines (comments, other fields) are skipped
func TestParseSSEEvents(t *testing.T) {
	sse := ": keep-alive\nevent: message\ndata: {\"a\":1}\n\nid: 2\ndata:[DONE]\n\ndata:   \n\ndata: not-json\r\n"
	events := parseSSEEvents([]byte(sse))
	if len(events) != 3 {
		t.Fatalf("want 3 events, got %d: %v", len(events), events)
	}
	obj, ok := events[0].(map[string]any)
	if !ok || obj["a"] != json.Number("1") { // numbers are kept as json.Number (literal form)
		t.Fatalf("event 0 not parsed JSON: %v", events[0])
	}
	if events[1] != "[DONE]" {
		t.Fatalf("event 1 = %v, want the raw [DONE] string", events[1])
	}
	if events[2] != "not-json" {
		t.Fatalf("event 2 = %v, want the raw string", events[2])
	}
	if got := parseSSEEvents(nil); got != nil {
		t.Fatalf("empty input must yield no events: %v", got)
	}
}

// runResponse drives TapResponse over the given body: it reads every byte through the
// wrapped body (the client's view must be byte-identical) and closes it, returning the
// dumped file's content
func runResponse(t *testing.T, p *Policy, res *http.Response, body string) string {
	t.Helper()
	p.TapResponse(res)
	var got strings.Builder
	_, err := io.Copy(io.Writer(&got), res.Body)
	if err != nil {
		t.Fatal(err)
	}
	if err := res.Body.Close(); err != nil {
		t.Fatal(err)
	}
	if got.String() != body {
		t.Fatalf("client view altered:\n%s\n%s", body, got.String())
	}
	entries, err := os.ReadDir(p.respSavePath)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 {
		t.Fatalf("want 1 dump file, got %d", len(entries))
	}
	data, err := os.ReadFile(filepath.Join(p.respSavePath, entries[0].Name()))
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}

// a streaming (SSE) response is dumped as a single JSON document: the status, the
// headers and the stream chunks assembled into the non-streaming structure
func TestTapResponseStream(t *testing.T) {
	p := &Policy{respSavePath: t.TempDir()}
	sse := "data: {\"choices\":[{\"delta\":{\"role\":\"assistant\",\"content\":\"hi\"},\"index\":0,\"finish_reason\":null}],\"created\":1,\"id\":\"chatcmpl-x\",\"model\":\"m\"}\n\n" +
		"data: {\"choices\":[{\"delta\":{\"content\":\" there\"},\"index\":0,\"finish_reason\":null}]}\n\n" +
		"data: {\"choices\":[{\"delta\":{},\"index\":0,\"finish_reason\":\"stop\"}]}\n\n" +
		"data: {\"choices\":[],\"usage\":{\"prompt_tokens\":1,\"completion_tokens\":1,\"total_tokens\":2},\"timings\":{\"draft_n\":4,\"draft_n_accepted\":4}}\n\n" +
		"data: [DONE]\n\n"
	res := &http.Response{
		StatusCode: 200,
		Status:     "200 OK",
		Header:     http.Header{"Content-Type": []string{"text/event-stream"}},
		Body:       io.NopCloser(strings.NewReader(sse)),
	}
	s := runResponse(t, p, res, sse)

	var dump struct {
		Status string      `json:"status"`
		Header http.Header `json:"headers"`
		Stream bool        `json:"stream"`
		Body   any         `json:"body"`
		Events []any       `json:"events"`
	}
	if err := json.Unmarshal([]byte(s), &dump); err != nil {
		t.Fatalf("dump is not valid JSON: %v\n%s", err, s)
	}
	if dump.Status != "200 OK" || !dump.Stream || dump.Header.Get("Content-Type") != "text/event-stream" {
		t.Fatalf("unexpected dump: %s", s)
	}
	if dump.Events != nil {
		t.Fatalf("an assembled dump must not carry raw events:\n%s", s)
	}
	b, ok := dump.Body.(map[string]any)
	if !ok {
		t.Fatalf("body not an object:\n%s", s)
	}
	if b["object"] != "chat.completion" || b["id"] != "chatcmpl-x" || b["model"] != "m" {
		t.Fatalf("envelope not merged:\n%s", s)
	}
	chs, _ := b["choices"].([]any)
	if len(chs) != 1 {
		t.Fatalf("want 1 choice, got %d:\n%s", len(chs), s)
	}
	c, _ := chs[0].(map[string]any)
	msg, _ := c["message"].(map[string]any)
	if c["finish_reason"] != "stop" || msg["role"] != "assistant" || msg["content"] != "hi there" {
		t.Fatalf("choice not assembled:\n%s", s)
	}
	usage, _ := b["usage"].(map[string]any)
	if usage["total_tokens"] != float64(2) {
		t.Fatalf("usage not merged:\n%s", s)
	}
	if _, ok := b["timings"].(map[string]any); !ok {
		t.Fatalf("timings not merged:\n%s", s)
	}
}

// a streaming body that is not a chat completion (no chunks with choices) is stored as
// the raw SSE events
func TestTapResponseStreamNonChat(t *testing.T) {
	p := &Policy{respSavePath: t.TempDir()}
	sse := "data: [DONE]\n\n"
	res := &http.Response{
		StatusCode: 200,
		Status:     "200 OK",
		Header:     http.Header{"Content-Type": []string{"text/event-stream"}},
		Body:       io.NopCloser(strings.NewReader(sse)),
	}
	s := runResponse(t, p, res, sse)

	var dump struct {
		Body   any   `json:"body"`
		Events []any `json:"events"`
	}
	if err := json.Unmarshal([]byte(s), &dump); err != nil {
		t.Fatalf("dump is not valid JSON: %v\n%s", err, s)
	}
	if dump.Body != nil {
		t.Fatalf("an unassembled stream must not carry a body:\n%s", s)
	}
	if len(dump.Events) != 1 || dump.Events[0] != "[DONE]" {
		t.Fatalf("events not kept raw:\n%s", s)
	}
}

// a non-streaming response: a valid JSON body is stored parsed, an invalid one base64
func TestTapResponseNonStream(t *testing.T) {
	t.Run("json body", func(t *testing.T) {
		p := &Policy{respSavePath: t.TempDir()}
		res := &http.Response{
			StatusCode: 200,
			Status:     "200 OK",
			Header:     http.Header{"Content-Type": []string{"application/json"}},
			Body:       io.NopCloser(strings.NewReader(`{"ok":true}`)),
		}
		s := runResponse(t, p, res, `{"ok":true}`)
		var dump struct {
			Stream bool `json:"stream"`
			Body   any  `json:"body"`
		}
		if err := json.Unmarshal([]byte(s), &dump); err != nil {
			t.Fatalf("dump is not valid JSON: %v\n%s", err, s)
		}
		if dump.Stream {
			t.Fatalf("dump marked as streaming:\n%s", s)
		}
		obj, ok := dump.Body.(map[string]any)
		if !ok || obj["ok"] != true {
			t.Fatalf("body not stored parsed:\n%s", s)
		}
	})
	t.Run("non-json body", func(t *testing.T) {
		p := &Policy{respSavePath: t.TempDir()}
		res := &http.Response{
			StatusCode: 200,
			Status:     "200 OK",
			Header:     http.Header{"Content-Type": []string{"text/plain"}},
			Body:       io.NopCloser(strings.NewReader("plain")),
		}
		s := runResponse(t, p, res, "plain")
		var dump struct {
			BodyB64 string `json:"bodyBase64"`
		}
		if err := json.Unmarshal([]byte(s), &dump); err != nil {
			t.Fatalf("dump is not valid JSON: %v\n%s", err, s)
		}
		if dump.BodyB64 != "cGxhaW4=" { // base64("plain")
			t.Fatalf("bodyBase64 = %q\n%s", dump.BodyB64, s)
		}
	})
}

// the dump keeps &, < and > literal (no HTML \u0026-style escapes) for readability
func TestTapResponseNoHTMLEscapes(t *testing.T) {
	p := &Policy{respSavePath: t.TempDir()}
	sse := "data: {\"choices\":[{\"delta\":{\"tool_calls\":[{\"index\":0,\"id\":\"c1\",\"type\":\"function\",\"function\":{\"name\":\"cmd\",\"arguments\":\"{\\\"command\\\":\\\"a && b < c > d\\\"}\"}}]},\"index\":0}]}\n\n" +
		"data: [DONE]\n\n"
	res := &http.Response{
		StatusCode: 200,
		Status:     "200 OK",
		Header:     http.Header{"Content-Type": []string{"text/event-stream"}},
		Body:       io.NopCloser(strings.NewReader(sse)),
	}
	s := runResponse(t, p, res, sse)
	for _, want := range []string{"a && b < c > d"} {
		if !strings.Contains(s, want) {
			t.Fatalf("dump missing literal %q:\n%s", want, s)
		}
	}
	if strings.Contains(s, `\u0026`) || strings.Contains(s, `\u003c`) || strings.Contains(s, `\u003e`) {
		t.Fatalf("dump contains HTML escapes:\n%s", s)
	}
}

// number literals survive the dump unchanged (no float64 round-trip): big integers keep
// all digits, 1.10 and 1e5 keep their literal form, and the original key order is kept
func TestTapResponseRawLiterals(t *testing.T) {
	t.Run("non-streaming", func(t *testing.T) {
		p := &Policy{respSavePath: t.TempDir()}
		raw := `{"big":9007199254740993,"price":1.10,"exp":1e5,"s":"x"}`
		res := &http.Response{
			StatusCode: 200,
			Status:     "200 OK",
			Header:     http.Header{"Content-Type": []string{"application/json"}},
			Body:       io.NopCloser(strings.NewReader(raw)),
		}
		s := runResponse(t, p, res, raw)
		for _, want := range []string{`"big": 9007199254740993`, `"price": 1.10`, `"exp": 1e5`} {
			if !strings.Contains(s, want) {
				t.Fatalf("dump lost the literal %q:\n%s", want, s)
			}
		}
		// key order must be the original one (big before price)
		ib, ip := strings.Index(s, `"big"`), strings.Index(s, `"price"`)
		if ib < 0 || ip < 0 || ib > ip {
			t.Fatalf("key order not preserved:\n%s", s)
		}
	})
	t.Run("streaming", func(t *testing.T) {
		p := &Policy{respSavePath: t.TempDir()}
		sse := "data: {\"created\":9007199254740993,\"choices\":[{\"delta\":{\"content\":\"hi\"},\"index\":0,\"finish_reason\":null}]}\n\n" +
			"data: [DONE]\n\n"
		res := &http.Response{
			StatusCode: 200,
			Status:     "200 OK",
			Header:     http.Header{"Content-Type": []string{"text/event-stream"}},
			Body:       io.NopCloser(strings.NewReader(sse)),
		}
		s := runResponse(t, p, res, sse)
		if !strings.Contains(s, `"created": 9007199254740993`) {
			t.Fatalf("dump lost the big integer literal:\n%s", s)
		}
	})
}

// no respSavePath: TapResponse must leave the body untouched
func TestTapResponseNoop(t *testing.T) {
	p := &Policy{}
	inner := io.NopCloser(strings.NewReader("x"))
	res := &http.Response{Body: inner}
	p.TapResponse(res)
	if res.Body != inner {
		t.Fatal("body was replaced, want the original body untouched")
	}
	if err := res.Body.Close(); err != nil {
		t.Fatal(err)
	}
}
