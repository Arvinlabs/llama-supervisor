package main

import (
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// errBoom simulates the "unexpected EOF" the proxy hits when the backend connection is killed mid-stream
var errBoom = errors.New("unexpected EOF")

// ioReadCloser adapts a plain reader to io.ReadCloser
type ioReadCloser struct{ r io.Reader }

func (c *ioReadCloser) Read(p []byte) (int, error) { return c.r.Read(p) }
func (c *ioReadCloser) Close() error               { return nil }

// errReadCloser returns its remaining bytes then a fixed error (a stream killed mid-chunk)
type errReadCloser struct {
	remaining []byte
	err       error
}

func (r *errReadCloser) Read(p []byte) (int, error) {
	if len(r.remaining) > 0 {
		n := copy(p, r.remaining)
		r.remaining = r.remaining[n:]
		return n, nil
	}
	return 0, r.err
}
func (r *errReadCloser) Close() error { return nil }

// inner that returns one chunk and then the stream-killing error
type boomReader struct {
	first bool
}

func (r *boomReader) Read(p []byte) (int, error) {
	if r.first {
		r.first = false
		return copy(p, []byte("data: x\n\n")), nil
	}
	return 0, errBoom
}

func (r *boomReader) Close() error { return nil }

// armed guard + stream-killing read error: the SSE error event is injected after the data already sent
func TestGuardBodyInjectsOnErrorWhenArmed(t *testing.T) {
	g := newStreamGuard()
	g.arm()
	defer g.disarm()

	var out strings.Builder
	b := &guardBody{guard: g, inner: &boomReader{}, writer: &out}
	for {
		_, err := b.Read(make([]byte, 64))
		if err != nil {
			break
		}
	}
	if got := out.String(); !strings.HasSuffix(got, streamErrorEvent) {
		t.Fatalf("expected stream to end with the error event, got %q", got)
	}
}

// not armed: a stream-killing error ends the stream without any injection (no false alarms for ordinary failures)
func TestGuardBodyNoInjectWhenNotArmed(t *testing.T) {
	g := newStreamGuard()
	var out strings.Builder
	b := &guardBody{guard: g, inner: &boomReader{}, writer: &out}
	for {
		_, err := b.Read(make([]byte, 64))
		if err != nil {
			break
		}
	}
	if out.Len() != 0 {
		t.Fatalf("expected no injection when not armed, got %q", out.String())
	}
}

// clean EOF (the backend finished normally and sent [DONE]): no injection even when armed
func TestGuardBodyNoInjectOnCleanEOF(t *testing.T) {
	g := newStreamGuard()
	g.arm()
	defer g.disarm()

	var out strings.Builder
	inner := &ioReadCloser{r: strings.NewReader("data: [DONE]\n\n")}
	b := &guardBody{guard: g, inner: inner, writer: &out}
	for {
		_, err := b.Read(make([]byte, 64))
		if err != nil {
			break
		}
	}
	if strings.Contains(out.String(), "event: error") {
		t.Fatalf("clean stream end must not be injected, got %q", out.String())
	}
}

// a truncated final data line (the backend was killed mid-chunk, no trailing newline)
// must not merge into the error event's data field: the leading blank line of the
// injected event terminates it into its own (malformed) message event
func TestGuardBodyInjectsAfterTruncatedLine(t *testing.T) {
	g := newStreamGuard()
	g.arm()
	defer g.disarm()

	var out strings.Builder
	inner := &errReadCloser{remaining: []byte(`data: {"choices": partial`), err: errBoom}
	b := &guardBody{guard: g, inner: inner, writer: &out}
	// emulate the ReverseProxy copy loop: forwarded bytes are written by the copy, not by Read
	buf := make([]byte, 64)
	for {
		n, err := b.Read(buf)
		if n > 0 {
			out.Write(buf[:n])
		}
		if err != nil {
			break
		}
	}
	s := out.String()
	// the truncated line is sealed into its own event
	if !strings.Contains(s, `data: {"choices": partial`+"\n\n") {
		t.Fatalf("truncated line should be sealed into its own event, got %q", s)
	}
	// and the error event stays well-formed with a clean JSON data field
	idx := strings.Index(s, "event: error\n")
	if idx < 0 || !strings.HasPrefix(s[idx:], streamErrorEvent[len("\n\n"):]) {
		t.Fatalf("error event should be well-formed after the truncated line, got %q", s[idx:])
	}
}

// the error event is written at most once even if the reader keeps erroring
func TestGuardBodyInjectsOnce(t *testing.T) {
	g := newStreamGuard()
	g.arm()
	defer g.disarm()

	var out strings.Builder
	inner := &boomReader{}
	b := &guardBody{guard: g, inner: inner, writer: &out}
	for i := 0; i < 3; i++ {
		_, _ = b.Read(make([]byte, 64)) // keep reading past the error
	}
	if n := strings.Count(out.String(), streamErrorEvent); n != 1 {
		t.Fatalf("expected exactly one error event, got %d", n)
	}
}

// end to end: a stream whose backend connection is killed mid-flight gets the SSE error
// event before the stream ends, while the client-visible status stays 200 (already sent)
func TestProxyInjectsErrorEventWhenStreamKilled(t *testing.T) {
	kill := make(chan struct{})
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.Write([]byte("data: x\n\n"))
		w.(http.Flusher).Flush()
		<-kill // "generate" until the backend is killed
		// kill the connection mid-chunk (like supervisorctl stop llama): the reader
		// of the incomplete chunked body sees unexpected EOF
		if h, ok := w.(http.Hijacker); ok {
			if conn, _, err := h.Hijack(); err == nil {
				conn.Close()
				return
			}
		}
	}))
	defer backend.Close()

	sup := newBackendProxy(Config{Backend: backend.URL}, t.Context())
	srv := httptest.NewServer(sup)
	defer srv.Close()

	clientReq, _ := http.NewRequest("POST", srv.URL+"/v1/chat/completions", nil)
	resp, err := srv.Client().Do(clientReq)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}

	buf := make([]byte, 4096)
	n, _ := resp.Body.Read(buf)
	if n == 0 {
		t.Fatal("no initial stream data")
	}
	// the watchdog restart is in progress: arm the shared guard, then kill the backend connection
	sup.guard.arm()
	defer sup.guard.disarm()
	close(kill)

	// the stream ends abruptly here; everything remaining (including the injected event) comes first
	body := string(buf[:n])
	rest, rerr := io.ReadAll(resp.Body)
	body += string(rest)
	if !strings.Contains(body, streamErrorEvent) {
		t.Fatalf("expected the injected error event before the stream end, got %q (readall err=%v)", body, rerr)
	}
}

// only the SSE chat completion endpoint is eligible for the injection
func TestShouldInjectStreamError(t *testing.T) {
	mk := func(path, ct string) *http.Response {
		req, _ := http.NewRequest("POST", "http://x"+path, nil)
		res := &http.Response{Request: req, Header: http.Header{}}
		if ct != "" {
			res.Header.Set("Content-Type", ct)
		}
		return res
	}
	if !shouldInjectStreamError(mk("/v1/chat/completions", "text/event-stream")) {
		t.Fatal("SSE chat completion must be eligible")
	}
	if shouldInjectStreamError(mk("/v1/chat/completions", "application/json")) {
		t.Fatal("non-stream JSON chat completion must not be eligible")
	}
	if shouldInjectStreamError(mk("/v1/chat/completions", "")) {
		t.Fatal("missing content type must not be eligible")
	}
	if shouldInjectStreamError(mk("/completion", "text/event-stream")) {
		t.Fatal("legacy completion endpoint must not be eligible")
	}
	if shouldInjectStreamError(mk("/slots", "application/json")) {
		t.Fatal("other endpoints must not be eligible")
	}
}

// a watchdog trigger arms the shared guard, so streams killed by the restart command are flagged to the client
func TestWatchdogTriggerArmsGuard(t *testing.T) {
	h := &slotsHandler{}
	h.nDecoded.Store(100)
	h.processing.Store(true)
	srv := httptest.NewServer(h)
	defer srv.Close()

	g := newStreamGuard()
	p := newWatchdogPolicy(&WatchdogGroup{Enable: true, Interval: 1, MaxRate: 10, Times: 2, Command: "true"}, srv.URL, "", g)

	p.tick(t.Context())        // baseline
	h.nDecoded.Store(200)
	p.tick(t.Context())        // fast 1/2
	if g.armed() {
		t.Fatal("guard must not arm before the trigger")
	}
	h.nDecoded.Store(300)
	p.tick(t.Context()) // fast 2/2 -> trigger, command runs
	if !g.armed() {
		t.Fatal("expected the guard to be armed by the watchdog trigger")
	}
	// the window closes automatically after the grace period
	time.Sleep(streamErrorGrace + 50*time.Millisecond)
	if g.armed() {
		t.Fatal("expected the guard window to close after the grace period")
	}
}
