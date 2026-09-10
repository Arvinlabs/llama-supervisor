package proxy

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Arvinlabs/llama-supervisor/internal/config"
	"github.com/Arvinlabs/llama-supervisor/internal/observe"
)

// when the client disconnects mid-stream, the proxied request context toward the backend
// must be canceled (abort the backend generation early instead of letting it run to completion)
func TestProxyClientDisconnectCancelsBackend(t *testing.T) {
	backendErr := make(chan error, 1)
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		go func() {
			<-r.Context().Done()
			backendErr <- r.Context().Err()
		}()
		fl := w.(http.Flusher)
		for i := 0; i < 200; i++ { // long streaming response, simulating continuous generation
			w.Write([]byte("data: x\n\n"))
			fl.Flush()
			select {
			case <-r.Context().Done():
				return
			case <-time.After(50 * time.Millisecond):
			}
		}
	}))
	defer backend.Close()

	sup := New(config.Config{Backend: backend.URL}, context.Background())
	srv := httptest.NewServer(sup)
	defer srv.Close()

	// client: read the first batch of streaming data, then disconnect
	conn, err := net.Dial("tcp", srv.Listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	fmt.Fprintf(conn, "GET / HTTP/1.1\r\nHost: x\r\n\r\n")
	buf := make([]byte, 256)
	if err := conn.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, err := io.ReadFull(conn, buf); err != nil {
		t.Fatalf("read stream: %v", err)
	}
	conn.Close()

	select {
	case err := <-backendErr:
		if err != context.Canceled {
			t.Fatalf("backend ctx err = %v, want context.Canceled", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("backend request not canceled after client disconnect")
	}
}

// when the client disconnects while the proxy is blocked reading the backend stream
// (the backend stays silent for a long time, e.g. the thinking phase):
// ctxBody must proactively close the backend connection, the proxy handler returns immediately,
// and the backend request is aborted
func TestProxyClientDisconnectAbortsBlockedBackend(t *testing.T) {
	backendDone := make(chan struct{})
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("partial")) // send only the first chunk, then stay silent for a long time (simulating thinking/slow generation)
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
		<-r.Context().Done() // wait for the proxy to disconnect the connection early
		close(backendDone)
	}))
	defer backend.Close()

	sup := New(config.Config{Backend: backend.URL}, context.Background())
	proxyDone := make(chan struct{})
	ctx, ctxCancel := context.WithCancel(context.Background())
	defer ctxCancel()
	req := httptest.NewRequest(http.MethodGet, "/stream", nil).WithContext(ctx)
	w := httptest.NewRecorder()
	go func() {
		sup.ServeHTTP(w, req)
		close(proxyDone)
	}()
	time.Sleep(100 * time.Millisecond) // wait for the first chunk to be forwarded and the proxy to block on the backend read
	ctxCancel()                        // simulate the client disconnect

	select {
	case <-proxyDone:
	case <-time.After(3 * time.Second):
		t.Fatal("client disconnect did not unblock the proxy handler")
	}
	select {
	case <-backendDone:
	case <-time.After(3 * time.Second):
		t.Fatal("backend request not aborted after client disconnect")
	}
}

// end-to-end: with the prefix cache modifier enabled the proxy sorts the tools but
// forwards every other byte exactly as the client sent it
func TestProxyForwardsNormalizedBody(t *testing.T) {
	var backendBody []byte
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		backendBody, _ = io.ReadAll(r.Body)
		w.Write([]byte(`{"ok":true}`))
	}))
	defer backend.Close()

	sup := New(config.Config{Backend: backend.URL, Request: &config.RequestGroup{Enable: true, PrefixCache: true}}, context.Background())
	body := `{"model":"m","temperature":0.700,"tools":[{"type":"function","function":{"name":"z","parameters":{"type":"object","properties":{"b":1.0,"a":2}}},"description":"z"},
	{"type":"function","function":{"name":"a","parameters":{"type":"object","properties":{"c":{}}},"description":"a"}}],"messages":[{"role":"user","content":"hi"}]}`
	r := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(body))
	r.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	sup.ServeHTTP(w, r)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d", w.Code)
	}
	// tools are sorted (a before z) and elements re-encoded canonically:
	// properties sorted, 1.0 -> 1
	if !strings.Contains(string(backendBody), `"a":2,"b":1`) {
		t.Fatalf("tool elements not canonicalized: %s", backendBody)
	}
	// number literal form outside the tools array stays exactly as sent
	if !strings.Contains(string(backendBody), `"temperature":0.700`) {
		t.Fatalf("number literal outside tools was altered: %s", backendBody)
	}
	// order check: the alpha tool object must appear before the zebra one
	s := string(backendBody)
	ia := strings.Index(s, `"name":"a"`)
	iz := strings.Index(s, `"name":"z"`)
	if ia < 0 || iz < 0 || ia > iz {
		t.Fatalf("backend received unsorted tools: %s", backendBody)
	}
}

// with debug enabled and savePath set, a proxied business request is dumped to a plain
// text file named by the request time (full request line, all headers, body), and the
// proxy still forwards the original body untouched
func TestProxySavesProxiedRequest(t *testing.T) {
	dir := t.TempDir()
	var backendBody []byte
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		backendBody, _ = io.ReadAll(r.Body)
		w.Write([]byte(`{"ok":true}`))
	}))
	defer backend.Close()

	cfg := config.Config{
		Backend: backend.URL,
		Debug:   &config.DebugGroup{Enable: true, SavePath: dir},
	}
	sup := New(cfg, context.Background())

	body := `{"model":"llama","messages":[{"role":"user","content":"hi"}]}`
	r := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(body))
	r.Header.Set("Content-Type", "application/json")
	r.Header.Set("Authorization", "Bearer sk-virtual-key-1")
	w := httptest.NewRecorder()
	sup.ServeHTTP(w, r)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d", w.Code)
	}
	// the proxy must forward the complete original body (the tee must not consume it)
	if string(backendBody) != body {
		t.Fatalf("backend body = %q, want %q", backendBody, body)
	}

	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 {
		t.Fatalf("expected 1 saved request file, got %d", len(entries))
	}
	name := entries[0].Name()
	if !strings.HasSuffix(name, ".txt") {
		t.Fatalf("file name %q has no .txt suffix", name)
	}
	data, err := os.ReadFile(filepath.Join(dir, name))
	if err != nil {
		t.Fatal(err)
	}
	s := string(data)
	if !strings.HasPrefix(s, "POST /v1/chat/completions HTTP/1.1\r\n") {
		t.Fatalf("request line missing or wrong: %q", s[:min(len(s), 50)])
	}
	for _, want := range []string{
		"Content-Type: application/json",
		"Authorization: Bearer sk-virtual-key-1",
	} {
		if !strings.Contains(s, want) {
			t.Fatalf("dump missing %q:\n%s", want, s)
		}
	}
	// the JSON body must be the last part of the dump, pretty-printed
	var expected bytes.Buffer
	if err := json.Indent(&expected, []byte(body), "", "  "); err != nil {
		t.Fatal(err)
	}
	if !strings.HasSuffix(s, expected.String()) {
		t.Fatalf("dump must end with the pretty-printed body:\n%s", s)
	}
}

// debug disabled: no debug policy at all; debug enabled without savePath: Tap is a no-op
func TestProxyNoSaveWithoutConfig(t *testing.T) {
	t.Run("debug disabled", func(t *testing.T) {
		sup := New(config.Config{Backend: "http://127.0.0.1:1", Debug: &config.DebugGroup{Enable: false}}, context.Background())
		if sup.debug != nil {
			t.Fatal("expected no debug policy")
		}
	})
	t.Run("debug enabled, savePath empty", func(t *testing.T) {
		sup := New(config.Config{Backend: "http://127.0.0.1:1", Debug: &config.DebugGroup{Enable: true}}, context.Background())
		if sup.debug == nil {
			t.Fatal("expected the debug policy")
		}
		// Tap must be a no-op: the body is left untouched
		r := httptest.NewRequest(http.MethodPost, "/x", strings.NewReader("hello"))
		before := r.Body
		sup.debug.Tap(r)
		if r.Body != before {
			t.Fatal("body was replaced, want the original body untouched")
		}
	})
	t.Run("debug disabled, outSavePath ignored", func(t *testing.T) {
		sup := New(config.Config{Backend: "http://127.0.0.1:1", Debug: &config.DebugGroup{Enable: false, OutSavePath: t.TempDir()}}, context.Background())
		if sup.debug != nil {
			t.Fatal("expected no debug policy")
		}
		if sup.proxy.Transport != nil {
			t.Fatal("expected no outbound save transport when debug.enable is false")
		}
	})
	t.Run("debug enabled, outSavePath empty", func(t *testing.T) {
		sup := New(config.Config{Backend: "http://127.0.0.1:1", Debug: &config.DebugGroup{Enable: true}}, context.Background())
		if sup.debug == nil {
			t.Fatal("expected the debug policy")
		}
		if sup.proxy.Transport != nil {
			t.Fatal("expected no outbound save transport when debug.outSavePath is empty")
		}
	})
}

// with debug.outSavePath set, the dumped request is the outbound request after the request
// policy has rewritten it: the virtual key is replaced by the backend key and the tools list
// is normalized, while the proxy still forwards the rewritten body
func TestProxySavesOutboundRequestAfterPolicyRewrite(t *testing.T) {
	dir := t.TempDir()
	var backendBody []byte
	var backendAuth string
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		backendBody, _ = io.ReadAll(r.Body)
		backendAuth = r.Header.Get("Authorization")
		w.Write([]byte(`{"ok":true}`))
	}))
	defer backend.Close()

	cfg := config.Config{
		Backend: backend.URL,
		ApiKey:  "sk-backend",
		Debug:   &config.DebugGroup{Enable: true, OutSavePath: dir},
		Request: &config.RequestGroup{
			Enable:      true,
			VirtualKeys: []string{"sk-virtual"},
			PrefixCache: true,
		},
	}
	sup := New(cfg, context.Background())
	if sup.debug == nil || sup.proxy.Transport == nil {
		t.Fatal("expected the debug policy and the outbound save transport")
	}

	body := `{"model":"m","tools":[` +
		`{"type":"function","function":{"name":"z","description":"z","parameters":{"type":"object","properties":{"b":1.0,"a":2}}}},` +
		`{"type":"function","function":{"name":"a","description":"a","parameters":{"type":"object","properties":{"c":{}}}}}` +
		`],"messages":[{"role":"user","content":"hi"}]}`
	r := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(body))
	r.Header.Set("Content-Type", "application/json")
	r.Header.Set("Authorization", "Bearer sk-virtual")
	w := httptest.NewRecorder()
	sup.ServeHTTP(w, r)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d", w.Code)
	}
	if backendAuth != "Bearer sk-backend" {
		t.Fatalf("backend Authorization = %q, want the re-signed backend key", backendAuth)
	}

	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 {
		t.Fatalf("expected 1 saved outbound request file, got %d", len(entries))
	}
	data, err := os.ReadFile(filepath.Join(dir, entries[0].Name()))
	if err != nil {
		t.Fatal(err)
	}
	s := string(data)
	// the dump must be the outbound request: backend host, re-signed key, virtual key never present
	if !strings.HasPrefix(s, "POST /v1/chat/completions HTTP/1.1\r\n") {
		t.Fatalf("request line missing or wrong: %q", s[:min(len(s), 60)])
	}
	for _, want := range []string{
		"Host: " + backend.Listener.Addr().String(),
		"Authorization: Bearer sk-backend",
		"Content-Type: application/json",
	} {
		if !strings.Contains(s, want) {
			t.Fatalf("outbound dump missing %q:\n%s", want, s)
		}
	}
	if strings.Contains(s, "sk-virtual") {
		t.Fatalf("outbound dump must not contain the virtual key:\n%s", s)
	}
	// the dumped JSON body must be the normalized outbound body (tools sorted a before z)
	idx := strings.Index(s, "\r\n\r\n{")
	if idx < 0 {
		t.Fatalf("dump has no JSON body:\n%s", s)
	}
	bodyStart := idx + len("\r\n\r\n")
	var parsed struct {
		Tools []struct {
			Function struct {
				Name string `json:"name"`
			} `json:"function"`
		} `json:"tools"`
	}
	if err := json.Unmarshal([]byte(s[bodyStart:]), &parsed); err != nil {
		t.Fatalf("dump body is not valid JSON: %v", err)
	}
	if len(parsed.Tools) != 2 || parsed.Tools[0].Function.Name != "a" || parsed.Tools[1].Function.Name != "z" {
		t.Fatalf("outbound dump tools not normalized:\n%s", s)
	}
	// the proxy must forward the same normalized body to the backend (compact form, not the dump's pretty-print)
	var forwarded struct {
		Tools []struct {
			Function struct {
				Name string `json:"name"`
			} `json:"function"`
		} `json:"tools"`
	}
	if err := json.Unmarshal(backendBody, &forwarded); err != nil {
		t.Fatalf("backend body is not valid JSON: %v", err)
	}
	if len(forwarded.Tools) != 2 || forwarded.Tools[0].Function.Name != "a" || forwarded.Tools[1].Function.Name != "z" {
		t.Fatalf("backend body tools not normalized:\n%s", backendBody)
	}
}

// debug.outSavePath is independent of the request policy: with request disabled the outbound
// request is still dumped (there is simply nothing to rewrite)
func TestProxySavesOutboundRequestWithoutRequestPolicy(t *testing.T) {
	dir := t.TempDir()
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`{"ok":true}`))
	}))
	defer backend.Close()

	sup := New(config.Config{Backend: backend.URL, Debug: &config.DebugGroup{Enable: true, OutSavePath: dir}}, context.Background())
	if sup.debug == nil || sup.proxy.Transport == nil {
		t.Fatal("expected the debug policy and the outbound save transport")
	}

	body := `{"model":"m"}`
	r := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(body))
	r.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	sup.ServeHTTP(w, r)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d", w.Code)
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 {
		t.Fatalf("expected 1 saved outbound request file, got %d", len(entries))
	}
	data, err := os.ReadFile(filepath.Join(dir, entries[0].Name()))
	if err != nil {
		t.Fatal(err)
	}
	s := string(data)
	for _, want := range []string{
		"POST /v1/chat/completions HTTP/1.1",
		"Content-Type: application/json",
	} {
		if !strings.Contains(s, want) {
			t.Fatalf("outbound dump missing %q:\n%s", want, s)
		}
	}
}

// obsRecorder collects observations delivered by the hub (a test consumer)
type obsRecorder struct {
	mu  sync.Mutex
	obs []observe.Observation
}

func (r *obsRecorder) OnCompletion(o observe.Observation) {
	r.mu.Lock()
	r.obs = append(r.obs, o)
	r.mu.Unlock()
}

func (r *obsRecorder) get() []observe.Observation {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]observe.Observation(nil), r.obs...)
}

// splitReader reads at most n bytes per Read (forces line-splitting) and records Close
type splitReader struct {
	r      *strings.Reader
	n      int
	closed bool
}

func (s *splitReader) Read(p []byte) (int, error) {
	if len(p) > s.n {
		p = p[:s.n]
	}
	return s.r.Read(p)
}

func (s *splitReader) Close() error {
	s.closed = true
	return nil
}

// the streaming completion tap: the usage/timings chunk may be split across reads,
// every byte is forwarded to the client, and the parsed observation reaches the consumer
func TestCompletionTapStream(t *testing.T) {
	sse := strings.Join([]string{
		`data: {"choices":[{"delta":{"content":"hi"},"index":0,"finish_reason":null}]}` + "\n\n",
		`data: {"choices":[],"usage":{"prompt_tokens":27,"completion_tokens":240,"total_tokens":267,"prompt_tokens_details":{"cached_tokens":23}},"timings":{"draft_n":1640,"draft_n_accepted":1270}}` + "\n\n",
		`data: [DONE]` + "\n\n",
	}, "")
	rec := &obsRecorder{}
	hub := &observe.Hub{}
	hub.Register(rec)
	inner := &splitReader{r: strings.NewReader(sse), n: 7}
	b := &completionTap{inner: inner, hub: hub, stream: true}

	var got bytes.Buffer
	for {
		buf := make([]byte, 13)
		n, err := b.Read(buf)
		if n > 0 {
			got.Write(buf[:n])
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
	}
	if got.String() != sse {
		t.Fatalf("stream bytes altered:\n%s\n%s", sse, got.String())
	}
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}
	if !inner.closed {
		t.Fatal("Close not forwarded to the inner body")
	}
	obs := rec.get()
	if len(obs) != 1 {
		t.Fatalf("want 1 observation, got %d: %+v", len(obs), obs)
	}
	o := obs[0]
	if o.Prompt != 27 || o.Cached != 23 || o.Completion != 240 || o.Total != 267 || o.DraftN != 1640 || o.DraftAccepted != 1270 {
		t.Fatalf("unexpected observation: %+v", o)
	}
}

// the non-stream completion tap: the JSON body is parsed at EOF and delivered
func TestCompletionTapNonStream(t *testing.T) {
	body := `{"object":"chat.completion","usage":{"prompt_tokens":10,"completion_tokens":5,"total_tokens":15},"timings":{"draft_n":6,"draft_n_accepted":6}}`
	rec := &obsRecorder{}
	hub := &observe.Hub{}
	hub.Register(rec)
	inner := &splitReader{r: strings.NewReader(body), n: 4}
	b := &completionTap{inner: inner, hub: hub, stream: false}

	var got bytes.Buffer
	for {
		buf := make([]byte, 9)
		n, err := b.Read(buf)
		if n > 0 {
			got.Write(buf[:n])
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
	}
	if got.String() != body {
		t.Fatalf("body altered:\n%s\n%s", body, got.String())
	}
	obs := rec.get()
	if len(obs) != 1 {
		t.Fatalf("want 1 observation, got %d: %+v", len(obs), obs)
	}
	if obs[0].Prompt != 10 || obs[0].Completion != 5 || obs[0].Total != 15 || obs[0].DraftN != 6 || obs[0].DraftAccepted != 6 {
		t.Fatalf("unexpected observation: %+v", obs[0])
	}
}

// a stream without a usage/timings chunk (e.g. interrupted) delivers nothing
func TestCompletionTapNoUsage(t *testing.T) {
	rec := &obsRecorder{}
	hub := &observe.Hub{}
	hub.Register(rec)
	sse := "data: {\"choices\":[{\"delta\":{\"content\":\"hi\"}}]}\n\n"
	b := &completionTap{inner: io.NopCloser(strings.NewReader(sse)), hub: hub, stream: true}
	io.Copy(io.Discard, b)
	if n := len(rec.get()); n != 0 {
		t.Fatalf("a usage-less stream must not deliver an observation, got %d: %+v", n, rec.get())
	}
}

// total falls back to prompt + completion; timings is a sibling of usage at the top level
func TestParseObservation(t *testing.T) {
	o := parseObservation([]byte(`{"usage":{"prompt_tokens":4,"completion_tokens":6},"timings":{"draft_n":100,"draft_n_accepted":90}}`))
	if o.Total != 10 || o.DraftN != 100 || o.DraftAccepted != 90 {
		t.Fatalf("unexpected observation: %+v", o)
	}
	if o.DraftRate() < 0.9-1e-9 || o.DraftRate() > 0.9+1e-9 {
		t.Fatalf("draft rate = %v, want 0.9", o.DraftRate())
	}
	if parseObservation([]byte(`{"id":"x"}`)) != (observe.Observation{}) {
		t.Fatal("an empty payload must yield the zero observation")
	}
}

// observationFromSSELine edge cases
func TestObservationFromSSELine(t *testing.T) {
	line := "data: {\"usage\":{\"prompt_tokens\":2,\"completion_tokens\":3,\"total_tokens\":5},\"timings\":{\"draft_n\":4,\"draft_n_accepted\":2}}"
	if o, ok := observationFromSSELine([]byte(line)); !ok || o.Prompt != 2 || o.Completion != 3 || o.Total != 5 || o.DraftN != 4 || o.DraftAccepted != 2 {
		t.Fatalf("usage line: ok=%v o=%+v", ok, o)
	}
	for _, l := range []string{
		"data: [DONE]",
		"data:",
		"data:   ",
		": keep-alive",
		"data: {\"choices\":[{\"delta\":{\"content\":\"x\"}}]}",
		"",
	} {
		if _, ok := observationFromSSELine([]byte(l)); ok {
			t.Fatalf("line must not yield an observation: %q", l)
		}
	}
}

// newCompletionsRequest builds a POST /v1/chat/completions request with the given body
func newCompletionsRequest(t *testing.T, body string) *http.Request {
	t.Helper()
	r := httptest.NewRequest(http.MethodPost, "http://backend/v1/chat/completions", strings.NewReader(body))
	r.Header.Set("Content-Type", "application/json")
	return r
}

// a streaming request without usage requested gets stream_options.include_usage injected
func TestInjectUsageStreaming(t *testing.T) {
	r := newCompletionsRequest(t, `{"model":"m","stream":true,"messages":[]}`)
	injectUsage(r)
	data, err := io.ReadAll(r.Body)
	if err != nil {
		t.Fatal(err)
	}
	out := string(data)
	if !strings.Contains(out, `"stream_options":{"include_usage":true}`) {
		t.Fatalf("include_usage not injected: %s", out)
	}
	if !strings.Contains(out, `"model":"m"`) || !strings.Contains(out, `"stream":true`) {
		t.Fatalf("original body fields lost: %s", out)
	}
	if r.ContentLength != int64(len(data)) || r.Header.Get("Content-Length") != fmt.Sprint(len(data)) {
		t.Fatalf("content length not updated: len=%d contentLength=%d header=%s", len(data), r.ContentLength, r.Header.Get("Content-Length"))
	}
}

// a request that already asks for usage is passed through byte-identical
func TestInjectUsageKeepsExisting(t *testing.T) {
	in := `{"model":"m","stream":true,"stream_options":{"include_usage":true},"messages":[]}`
	r := newCompletionsRequest(t, in)
	injectUsage(r)
	data, _ := io.ReadAll(r.Body)
	if string(data) != in {
		t.Fatalf("body must be byte-identical:\n%s\n%s", in, data)
	}
}

// non-stream, other paths, GETs and non-JSON bodies pass through unchanged
func TestInjectUsageOnlyEligible(t *testing.T) {
	stream := `{"model":"m","stream":true,"messages":[]}`
	cases := map[string]*http.Request{
		"non-stream": newCompletionsRequest(t, `{"model":"m","stream":false,"messages":[]}`),
		"other path": func() *http.Request {
			r := httptest.NewRequest(http.MethodPost, "http://backend/v1/completion", strings.NewReader(stream))
			r.Header.Set("Content-Type", "application/json")
			return r
		}(),
		"get method": func() *http.Request {
			r := httptest.NewRequest(http.MethodGet, "http://backend/v1/chat/completions", nil)
			r.Header.Set("Content-Type", "application/json")
			return r
		}(),
		"non-json": func() *http.Request {
			r := httptest.NewRequest(http.MethodPost, "http://backend/v1/chat/completions", strings.NewReader(stream))
			r.Header.Set("Content-Type", "text/plain")
			return r
		}(),
	}
	for name, req := range cases {
		t.Run(name, func(t *testing.T) {
			injectUsage(req)
			if req.Body == nil {
				return
			}
			data, _ := io.ReadAll(req.Body)
			if strings.Contains(string(data), "include_usage") {
				t.Fatalf("include_usage injected into a non-eligible request: %s", data)
			}
		})
	}
}

// end-to-end: the proxy taps a chat completion and fans the parsed observation out to every
// registered consumer (stats plus a test recorder); the client still gets the stream verbatim
func TestProxyFansObservationToConsumers(t *testing.T) {
	dir := t.TempDir()
	sse := strings.Join([]string{
		`data: {"choices":[{"delta":{"content":"hi"},"index":0,"finish_reason":null}]}` + "\n\n",
		`data: {"choices":[],"usage":{"prompt_tokens":27,"completion_tokens":240,"total_tokens":267,"prompt_tokens_details":{"cached_tokens":23}},"timings":{"draft_n":1640,"draft_n_accepted":1270}}` + "\n\n",
		`data: [DONE]` + "\n\n",
	}, "")
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.Write([]byte(sse))
	}))
	defer backend.Close()

	sup := New(config.Config{Backend: backend.URL, Stats: &config.StatsGroup{Enable: true, SavePath: dir}}, context.Background())
	if !sup.observe {
		t.Fatal("expected the completion tap to be active with a stats consumer")
	}
	rec := &obsRecorder{}
	sup.hub.Register(rec)

	r := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{"model":"m","stream":true,"messages":[]}`))
	r.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	sup.ServeHTTP(w, r)

	if w.Body.String() != sse {
		t.Fatalf("client did not receive the stream byte-identical:\n%s\n%s", sse, w.Body.String())
	}
	obs := rec.get()
	if len(obs) != 1 || obs[0].Total != 267 || obs[0].DraftN != 1640 || obs[0].DraftAccepted != 1270 {
		t.Fatalf("consumer did not receive the observation: %+v", obs)
	}
}

// with no consumer registered the tap stays off: the stream is not observed and the
// streaming request is forwarded without an injected usage option
func TestProxyTapOffWithoutConsumer(t *testing.T) {
	var sawIncludeUsage bool
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		sawIncludeUsage = strings.Contains(string(b), "include_usage")
		w.Header().Set("Content-Type", "text/event-stream")
		w.Write([]byte("data: x\n\ndata: [DONE]\n\n"))
	}))
	defer backend.Close()

	sup := New(config.Config{Backend: backend.URL}, context.Background())
	if sup.observe {
		t.Fatal("expected the completion tap to be off with no consumer")
	}
	r := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{"model":"m","stream":true,"messages":[]}`))
	r.Header.Set("Content-Type", "application/json")
	sup.ServeHTTP(httptest.NewRecorder(), r)
	if sawIncludeUsage {
		t.Fatal("include_usage must not be injected when no consumer observes completions")
	}
}
