package proxy

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Arvinlabs/llama-supervisor/internal/config"
	"github.com/Arvinlabs/llama-supervisor/internal/errguard"
	"github.com/Arvinlabs/llama-supervisor/internal/observe"
)

// errorFromSSELine: only a data event whose payload carries a top-level error object matches
func TestErrorFromSSELine(t *testing.T) {
	line := `data: {"error":{"type":"server_error","message":"CUDA out of memory","code":"oom"}}`
	if e, ok := errorFromSSELine([]byte(line)); !ok || e.Type != "server_error" || e.Message != "CUDA out of memory" || e.Code != "oom" {
		t.Fatalf("error line: ok=%v e=%+v", ok, e)
	}
	for _, l := range []string{
		"data: [DONE]",
		"data:",
		"data:   ",
		": keep-alive",
		`data: {"choices":[{"delta":{"content":"x"}}]}`,
		`data: {"usage":{"total_tokens":5}}`,
		`data: {"error":"not an object"}`, // a non-object error yields nothing
		"",
	} {
		if _, ok := errorFromSSELine([]byte(l)); ok {
			t.Fatalf("line must not yield an error: %q", l)
		}
	}
}

// parseError: a non-stream JSON body carrying a top-level error object
func TestParseError(t *testing.T) {
	if e, ok := parseError([]byte(`{"error":{"type":"server_error","message":"model load failed"}}`)); !ok || e.Type != "server_error" || e.Message != "model load failed" {
		t.Fatalf("ok=%v e=%+v", ok, e)
	}
	if _, ok := parseError([]byte(`{"object":"chat.completion","usage":{"total_tokens":5}}`)); ok {
		t.Fatal("a normal completion body must not yield an error")
	}
	if _, ok := parseError([]byte(`{"error":{}}`)); ok {
		t.Fatal("an error object with neither type nor message yields nothing")
	}
}

// waitForPath polls until path exists (the guard command runs in a goroutine) or the deadline
func waitForPath(t *testing.T, path string, d time.Duration) bool {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(path); err == nil {
			return true
		}
		time.Sleep(10 * time.Millisecond)
	}
	return false
}

// end to end (streaming): a backend stream that carries an error event triggers the
// guard command, while the client still receives the stream byte-identical
func TestProxyErrGuardStreamError(t *testing.T) {
	dir := t.TempDir()
	hit := filepath.Join(dir, "hit")
	sse := strings.Join([]string{
		`data: {"choices":[{"delta":{"content":"hi"},"index":0,"finish_reason":null}]}` + "\n\n",
		`data: {"error":{"type":"server_error","message":"CUDA out of memory"}}` + "\n\n",
		`data: [DONE]` + "\n\n",
	}, "")
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.Write([]byte(sse))
	}))
	defer backend.Close()

	sup := New(config.Config{
		Backend:  backend.URL,
		ErrGuard: &config.ErrGuardGroup{Enable: true, Patterns: []string{`out of memory`}, Command: "touch " + hit},
	}, context.Background())

	r := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{"model":"m","stream":true,"messages":[]}`))
	r.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	sup.ServeHTTP(w, r)

	if w.Body.String() != sse {
		t.Fatalf("client did not receive the stream byte-identical:\n%s\n%s", sse, w.Body.String())
	}
	if !waitForPath(t, hit, 3*time.Second) {
		t.Fatal("guard command did not run for a matching stream error event")
	}
}

// end to end (non-streaming): a plain JSON error body triggers the guard command
func TestProxyErrGuardNonStreamError(t *testing.T) {
	dir := t.TempDir()
	hit := filepath.Join(dir, "hit")
	body := `{"error":{"type":"server_error","message":"model load failed"}}`
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(body))
	}))
	defer backend.Close()

	sup := New(config.Config{
		Backend:  backend.URL,
		ErrGuard: &config.ErrGuardGroup{Enable: true, Patterns: []string{`load failed`}, Command: "touch " + hit},
	}, context.Background())

	r := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{"model":"m","messages":[]}`))
	r.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	sup.ServeHTTP(w, r)

	if w.Body.String() != body {
		t.Fatalf("client did not receive the body verbatim:\n%s\n%s", body, w.Body.String())
	}
	if !waitForPath(t, hit, 3*time.Second) {
		t.Fatal("guard command did not run for a matching non-stream error body")
	}
}

// a normal completion (no error payload) never triggers the guard
func TestProxyErrGuardNoTriggerOnSuccess(t *testing.T) {
	dir := t.TempDir()
	hit := filepath.Join(dir, "hit")
	sse := strings.Join([]string{
		`data: {"choices":[{"delta":{"content":"hi"},"index":0,"finish_reason":null}]}` + "\n\n",
		`data: {"choices":[],"usage":{"total_tokens":5}}` + "\n\n",
		`data: [DONE]` + "\n\n",
	}, "")
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.Write([]byte(sse))
	}))
	defer backend.Close()

	sup := New(config.Config{
		Backend:  backend.URL,
		ErrGuard: &config.ErrGuardGroup{Enable: true, Patterns: []string{`out of memory`}, Command: "touch " + hit},
	}, context.Background())

	r := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{"model":"m","stream":true,"messages":[]}`))
	r.Header.Set("Content-Type", "application/json")
	sup.ServeHTTP(httptest.NewRecorder(), r)

	if waitForPath(t, hit, 300*time.Millisecond) {
		t.Fatal("guard command ran for a normal completion")
	}
}

// an empty pattern list matches nothing: even a backend error payload never triggers
func TestProxyErrGuardEmptyPatternsNeverTrigger(t *testing.T) {
	dir := t.TempDir()
	hit := filepath.Join(dir, "hit")
	sse := `data: {"error":{"type":"server_error","message":"CUDA out of memory"}}` + "\n\n"
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.Write([]byte(sse))
	}))
	defer backend.Close()

	sup := New(config.Config{
		Backend:  backend.URL,
		ErrGuard: &config.ErrGuardGroup{Enable: true, Command: "touch " + hit}, // no patterns
	}, context.Background())

	r := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{"model":"m","stream":true,"messages":[]}`))
	r.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	sup.ServeHTTP(w, r)

	if w.Body.String() != sse {
		t.Fatalf("stream altered:\n%s\n%s", sse, w.Body.String())
	}
	if waitForPath(t, hit, 300*time.Millisecond) {
		t.Fatal("empty pattern list must never trigger the command")
	}
}

// the supervisor's own injected stream-interruption event (an abnormal stream end) must
// NOT trigger the guard, even when a pattern matches its message: it is the supervisor
// synthesizing an event (dead connection), not a backend error payload
func TestCompletionTapIgnoresInjectedStreamError(t *testing.T) {
	dir := t.TempDir()
	hit := filepath.Join(dir, "hit")
	guard, err := errguard.New(&config.ErrGuardGroup{
		Enable:   true,
		Patterns: []string{`stream interrupted`, `server_error`},
		Command:  "touch " + hit,
	}, context.Background())
	if err != nil {
		t.Fatal(err)
	}
	inner := io.NopCloser(strings.NewReader(streamErrorEvent))
	b := &completionTap{inner: inner, hub: &observe.Hub{}, guard: guard, stream: true}
	io.Copy(io.Discard, b)
	if waitForPath(t, hit, 300*time.Millisecond) {
		t.Fatal("the injected stream-interruption event must not trigger the guard")
	}
}

// a real backend error payload still triggers even when it is followed by the injected
// event (backend returned an error and then the connection died): the real payload
// matches, the injected tail is ignored
func TestCompletionTapRealErrorThenInjected(t *testing.T) {
	dir := t.TempDir()
	hit := filepath.Join(dir, "hit")
	guard, err := errguard.New(&config.ErrGuardGroup{
		Enable:   true,
		Patterns: []string{`out of memory`},
		Command:  "touch " + hit,
	}, context.Background())
	if err != nil {
		t.Fatal(err)
	}
	sse := `data: {"error":{"type":"server_error","message":"CUDA out of memory"}}` + "\n\n" + streamErrorEvent
	b := &completionTap{inner: io.NopCloser(strings.NewReader(sse)), hub: &observe.Hub{}, guard: guard, stream: true}
	io.Copy(io.Discard, b)
	if !waitForPath(t, hit, 3*time.Second) {
		t.Fatal("a real backend error payload must trigger the guard")
	}
}

// a matching error but no command configured: the tap stays active (the stream is still
// forwarded verbatim) and nothing panics
func TestProxyErrGuardMatchWithoutCommand(t *testing.T) {
	sse := `data: {"error":{"type":"server_error","message":"boom"}}` + "\n\n"
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.Write([]byte(sse))
	}))
	defer backend.Close()

	sup := New(config.Config{
		Backend:  backend.URL,
		ErrGuard: &config.ErrGuardGroup{Enable: true, Patterns: []string{`boom`}},
	}, context.Background())

	r := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{"model":"m","stream":true,"messages":[]}`))
	r.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	sup.ServeHTTP(w, r)
	if w.Body.String() != sse {
		t.Fatalf("stream altered:\n%s\n%s", sse, w.Body.String())
	}
}

// the guard tap is active on its own: with no stats/watchdog consumer the completion tap
// still runs (to look for error payloads) but usage is NOT injected into the outbound request
func TestProxyErrGuardActiveWithoutUsageInjection(t *testing.T) {
	var sawIncludeUsage bool
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		sawIncludeUsage = strings.Contains(string(b), "include_usage")
		w.Header().Set("Content-Type", "text/event-stream")
		w.Write([]byte("data: x\n\ndata: [DONE]\n\n"))
	}))
	defer backend.Close()

	sup := New(config.Config{
		Backend:  backend.URL,
		ErrGuard: &config.ErrGuardGroup{Enable: true, Patterns: []string{`never`}},
	}, context.Background())
	if sup.observe {
		t.Fatal("the observation tap must stay off with no stats/watchdog consumer")
	}
	if sup.errguard == nil {
		t.Fatal("expected the errguard policy")
	}
	r := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{"model":"m","stream":true,"messages":[]}`))
	r.Header.Set("Content-Type", "application/json")
	sup.ServeHTTP(httptest.NewRecorder(), r)
	if sawIncludeUsage {
		t.Fatal("include_usage must not be injected when only the error guard is enabled")
	}
}
