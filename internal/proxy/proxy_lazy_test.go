package proxy

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/Arvinlabs/llama-supervisor/internal/config"
)

// lazyTestConfig builds a minimal config with the lazy lifecycle enabled and
// (un)reachable backend markers as the start/stop commands
func lazyTestConfig(backend, dir string) (config.Config, string, string) {
	cfg := config.Config{
		Backend: backend,
		Lazy: &config.LazyGroup{
			Enable:       true,
			Interval:     1, // seconds
			StartCommand: "touch " + filepath.Join(dir, "started"),
			StopCommand:  "touch " + filepath.Join(dir, "stopped"),
		},
	}
	return cfg, filepath.Join(dir, "started"), filepath.Join(dir, "stopped")
}

func waitForFile(t *testing.T, path string, d time.Duration) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(path); err == nil {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", path)
}

// the lazy start: a request that finds the backend down does not block for the
// start command - it gets a 502 (no cloud configured) and the start command
// runs asynchronously in the background
func TestLazyStartAsyncWhenBackendDown(t *testing.T) {
	// a backend that is immediately unreachable
	dead := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	deadURL := dead.URL
	dead.Close()

	dir := t.TempDir()
	cfg, started, _ := lazyTestConfig(deadURL, dir)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sup := New(cfg, ctx)
	sup.StartBackground(ctx)
	srv := httptest.NewServer(sup)
	defer srv.Close()

	// the tracker is optimistic (up) until the first poll: force the down verdict
	sup.health.set(false)

	start := time.Now()
	resp, err := http.Post(srv.URL+completionsPath, "application/json",
		bytes.NewBufferString(`{"model":"m","messages":[{"role":"user","content":"hi"}]}`))
	if err != nil {
		t.Fatal(err)
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	elapsed := time.Since(start)
	if resp.StatusCode != http.StatusBadGateway {
		t.Fatalf("status = %d, want 502 (no cloud fallback configured)", resp.StatusCode)
	}
	if elapsed > time.Second {
		t.Fatalf("the request waited %s for the backend, it must not be blocked by the lazy start", elapsed)
	}
	waitForFile(t, started, 5*time.Second)
}

// the full lazy cycle: the first local request arms the idle-stop window; after
// the interval the stop command runs even though no further request arrives; a
// later request that finds the backend down starts it again
func TestLazyIdleStopAndRestartCycle(t *testing.T) {
	dead := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	deadURL := dead.URL
	dead.Close()

	dir := t.TempDir()
	cfg, started, stopped := lazyTestConfig(deadURL, dir)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sup := New(cfg, ctx)
	sup.StartBackground(ctx)
	srv := httptest.NewServer(sup)
	defer srv.Close()

	post := func() {
		resp, err := http.Post(srv.URL+completionsPath, "application/json",
			bytes.NewBufferString(`{"model":"m","messages":[{"role":"user","content":"hi"}]}`))
		if err != nil {
			t.Fatal(err)
		}
		_, _ = io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
	}

	// backend down: the request 502s and starts the backend asynchronously;
	// it also counts as local activity, arming the idle-stop window
	sup.health.set(false)
	post()
	waitForFile(t, started, 5*time.Second)

	// idle for the whole interval (no more requests): the stop command runs
	waitForFile(t, stopped, 6*time.Second)

	// a new request finds the backend down again: it starts it once more
	os.Remove(started)
	post()
	waitForFile(t, started, 5*time.Second)
}

// a cloud-routed request that finds the backend down must also trigger the lazy
// start (it is still a request for the local service) but never wait for it
func TestLazyStartTriggeredByCloudRoutedRequest(t *testing.T) {
	dead := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	deadURL := dead.URL
	dead.Close()
	cloud := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"choices":[{"message":{"content":"ok"}}],` +
			`"usage":{"prompt_tokens":1,"completion_tokens":1,"total_tokens":2}}`))
	}))
	defer cloud.Close()

	dir := t.TempDir()
	cfg, started, _ := lazyTestConfig(deadURL, dir)
	cfg.Cloud = &config.CloudGroup{
		Enable:         true,
		BaseURL:        cloud.URL,
		APIKey:         "sk-cloud",
		LocalMaxTokens: 4096,
		Threshold:      0.0001, // route every chat completion to the cloud
		Tokenizer:      "tiktoken",
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sup := New(cfg, ctx)
	sup.StartBackground(ctx)
	srv := httptest.NewServer(sup)
	defer srv.Close()

	sup.health.set(false)
	start := time.Now()
	resp, err := http.Post(srv.URL+completionsPath, "application/json",
		bytes.NewBufferString(`{"model":"m","messages":[{"role":"user","content":"hi"}]}`))
	if err != nil {
		t.Fatal(err)
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200 (served by the cloud)", resp.StatusCode)
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("the cloud-routed request waited %s, it must not be blocked by the lazy start", elapsed)
	}
	waitForFile(t, started, 5*time.Second)
}
