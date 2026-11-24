package lazy

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/Arvinlabs/llama-supervisor/internal/config"
)

// healthState is a mutable stand-in for the supervisor's backend liveness tracker
type healthState struct {
	mu    sync.Mutex
	alive bool
}

func (h *healthState) up() bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.alive
}

func (h *healthState) set(v bool) {
	h.mu.Lock()
	h.alive = v
	h.mu.Unlock()
}

// waitFor polls fn until it is true or the deadline passes
func waitFor(t *testing.T, what string, d time.Duration, fn func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if fn() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func newTestPolicy(t *testing.T, backend string, h *healthState) *Policy {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	g := &config.LazyGroup{Enable: true, Interval: 1, StartCommand: "true", StopCommand: "true"}
	return New(ctx, backend, g, time.Second, h.up, h.set)
}

// the start is async: the call returns immediately, the start command runs, and
// once the backend reports ready the health tracker is marked up at once
func TestStartAsyncRunsStartCommandAndMarksUp(t *testing.T) {
	health := &healthState{} // down
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/health" {
			w.WriteHeader(http.StatusOK)
			return
		}
		http.NotFound(w, r)
	}))
	defer backend.Close()

	marker := filepath.Join(t.TempDir(), "started")
	p := newTestPolicy(t, backend.URL, health)
	p.startCmd = "touch " + marker

	start := time.Now()
	if !p.StartAsync() {
		t.Fatal("StartAsync must trigger a start on a down backend")
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("StartAsync blocked the request path for %s, it must return immediately", elapsed)
	}
	waitFor(t, "the start command to run", 5*time.Second, func() bool {
		_, err := os.Stat(marker)
		return err == nil
	})
	waitFor(t, "the health tracker to be marked up", 5*time.Second, health.up)
}

// a start is skipped when the backend is already up
func TestStartAsyncSkipsWhenUp(t *testing.T) {
	health := &healthState{alive: true}
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	defer backend.Close()

	marker := filepath.Join(t.TempDir(), "started")
	p := newTestPolicy(t, backend.URL, health)
	p.startCmd = "touch " + marker

	if p.StartAsync() {
		t.Fatal("StartAsync must not start an up backend")
	}
	time.Sleep(100 * time.Millisecond)
	if _, err := os.Stat(marker); err == nil {
		t.Fatal("the start command ran although the backend is up")
	}
}

// concurrent requests stack only one start command (deduplication is on the
// command itself, not on the readiness wait): once the command finished the
// next request can retry, even while the first readiness wait is still running
// because the backend did not load in time
func TestStartAsyncDedupesConcurrentCalls(t *testing.T) {
	health := &healthState{} // down
	// /health exists but the backend never reports ready (e.g. the model did
	// not load): the readiness wait keeps running until the ctx is canceled
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/health" {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		http.NotFound(w, r)
	}))
	defer backend.Close()

	marker := filepath.Join(t.TempDir(), "started")
	p := newTestPolicy(t, backend.URL, health)
	p.startCmd = "sleep 0.3 && touch " + marker

	if !p.StartAsync() {
		t.Fatal("the first call must trigger a start")
	}
	if p.StartAsync() {
		t.Fatal("a concurrent call while the start command is running must not stack another start")
	}
	waitFor(t, "the start command to finish and clear the flag", 5*time.Second, func() bool {
		_, err := os.Stat(marker)
		if err != nil {
			return false
		}
		p.mu.Lock()
		defer p.mu.Unlock()
		return !p.starting
	})
	// still down and the first readiness wait is still running: the next
	// request retries the start command
	if !p.StartAsync() {
		t.Fatal("once the start command finished the next call must be able to start again")
	}
}

// a failed start command leaves the in-flight flag cleared, so a later request
// retries (it does not get stuck in a "starting" state forever)
func TestStartAsyncFailedCommandRetries(t *testing.T) {
	health := &healthState{}
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	defer backend.Close()

	p := newTestPolicy(t, backend.URL, health)
	p.startCmd = "exit 1"

	if !p.StartAsync() {
		t.Fatal("the first call must trigger a start")
	}
	waitFor(t, "the failed start to finish", 5*time.Second, func() bool {
		p.mu.Lock()
		defer p.mu.Unlock()
		return !p.starting
	})
	if !p.StartAsync() {
		t.Fatal("after a failed start the next request must retry")
	}
}

// the stop is lazy too: nothing happens before the first request (even after a
// long silence), the first request arms the window, the crossing runs the stop
// command once, and the timer pauses until the next request
func TestConsumeIdleStopsBackendAfterIdleWindow(t *testing.T) {
	health := &healthState{alive: true}
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	defer backend.Close()

	marker := filepath.Join(t.TempDir(), "stopped")
	p := newTestPolicy(t, backend.URL, health)
	p.stopCmd = "touch " + marker

	// no traffic at all, far beyond the window: the lazy timer never started
	time.Sleep(50 * time.Millisecond)
	p.ConsumeIdle()
	if _, err := os.Stat(marker); err == nil {
		t.Fatal("the stop command ran without any prior request")
	}

	// the first request arms the window; inside the window nothing happens
	p.OnHTTPRequest()
	p.ConsumeIdle()
	if _, err := os.Stat(marker); err == nil {
		t.Fatal("the stop command ran inside the idle window")
	}

	// the window crosses: the stop command runs and marks the tracker down
	p.tracker.ForceDue()
	if !p.ConsumeIdle() {
		t.Fatal("ConsumeIdle must run the stop command when the window is crossed")
	}
	if _, err := os.Stat(marker); err != nil {
		t.Fatal("the stop command did not run")
	}
	waitFor(t, "the health tracker to be marked down", 2*time.Second, func() bool {
		return !health.up()
	})

	// paused after the trigger: silence alone must not run it again
	p.ConsumeIdle()
	if _, err := os.Stat(marker); err != nil {
		t.Fatal("the timer must stay paused after a trigger until the next request")
	}
}

// a request after a stop re-arms the window, so the cycle can repeat
func TestConsumeIdleRearmsAfterNextRequest(t *testing.T) {
	health := &healthState{alive: true}
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	defer backend.Close()

	p := newTestPolicy(t, backend.URL, health)
	p.stopCmd = "true"

	p.OnHTTPRequest()
	p.tracker.ForceDue()
	if !p.ConsumeIdle() {
		t.Fatal("the first crossing must trigger")
	}
	p.ConsumeIdle()
	// a new request resumes the timing, and the next crossing triggers again
	p.OnHTTPRequest()
	p.tracker.ForceDue()
	if !p.ConsumeIdle() {
		t.Fatal("after a new request the next crossing must trigger again")
	}
}
