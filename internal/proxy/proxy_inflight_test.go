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

// slowLocalBackend serves a completion after sleeping d, so an idle window
// shorter than d is crossed while the request is still in flight
func slowLocalBackend(d time.Duration) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(d)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"choices":[{"message":{"content":"ok"}}],` +
			`"usage":{"prompt_tokens":1,"completion_tokens":1,"total_tokens":2}}`))
	}))
}

// postAndCheckInFlight posts a chat completion, then - d later, while the (slow)
// backend is still serving it - runs fn, and finally reports the response status
func postAndCheckInFlight(t *testing.T, srvURL string, d time.Duration, fn func()) int {
	t.Helper()
	statusCh := make(chan int, 1)
	go func() {
		resp, err := http.Post(srvURL+completionsPath, "application/json",
			bytes.NewBufferString(`{"model":"m","messages":[{"role":"user","content":"hi"}]}`))
		if err != nil {
			t.Error(err)
			return
		}
		_, _ = io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		statusCh <- resp.StatusCode
	}()
	time.Sleep(d)
	fn()
	select {
	case status := <-statusCh:
		return status
	case <-time.After(10 * time.Second):
		t.Fatal("the request did not complete")
		return 0
	}
}

// the restart command must not run while a local request is still in flight:
// the idle window that expired while the slow request was arriving only triggers
// after the request has finished (deferred to the next background tick)
func TestRestartDeferredWhileRequestInFlight(t *testing.T) {
	backend := slowLocalBackend(1500 * time.Millisecond)
	defer backend.Close()

	marker := filepath.Join(t.TempDir(), "restarted")
	cfg := config.Config{
		Backend: backend.URL,
		Restart: &config.RestartGroup{
			Enable:   true,
			Interval: 1, // seconds: crossed while the 1.5s request is still in flight
			Command:  "touch " + marker,
		},
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sup := New(cfg, ctx)
	sup.StartBackground(ctx)
	srv := httptest.NewServer(sup)
	defer srv.Close()

	status := postAndCheckInFlight(t, srv.URL, 1200*time.Millisecond, func() {
		// the idle window has been crossed (1s < 1.2s) but the request is still
		// being served: the restart command must not have run
		if _, err := os.Stat(marker); err == nil {
			t.Fatal("the restart command ran while a request was still in flight")
		}
	})
	if status != http.StatusOK {
		t.Fatalf("status = %d, want 200 (the in-flight request must not be interrupted)", status)
	}
	// once the request finished, the deferred trigger runs on the next tick
	waitForFile(t, marker, 5*time.Second)
}

// the lazy stop must not run while a local request is still in flight, for the
// same reason as the restart above
func TestLazyStopDeferredWhileRequestInFlight(t *testing.T) {
	backend := slowLocalBackend(1500 * time.Millisecond)
	defer backend.Close()

	dir := t.TempDir()
	cfg, _, stopped := lazyTestConfig(backend.URL, dir)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sup := New(cfg, ctx)
	sup.StartBackground(ctx)
	srv := httptest.NewServer(sup)
	defer srv.Close()

	status := postAndCheckInFlight(t, srv.URL, 1200*time.Millisecond, func() {
		// the idle window has been crossed (1s < 1.2s) but the request is still
		// being served: the stop command must not have run
		if _, err := os.Stat(stopped); err == nil {
			t.Fatal("the lazy stop command ran while a request was still in flight")
		}
	})
	if status != http.StatusOK {
		t.Fatalf("status = %d, want 200 (the in-flight request must not be interrupted)", status)
	}
	// once the request finished, the deferred stop runs on the next tick
	waitForFile(t, stopped, 5*time.Second)
}

// a cloud-routed request is not local activity: it does not hold the in-flight
// counter, so a crossed idle window still triggers on the next tick while a
// (slow) cloud request is in flight
func TestCloudRequestDoesNotHoldInFlightGuard(t *testing.T) {
	local := &completionRecorder{}
	localSrv := httptest.NewServer(local.handler())
	defer localSrv.Close()
	cloud := slowLocalBackend(3 * time.Second) // in flight past the tick that triggers
	defer cloud.Close()

	marker := filepath.Join(t.TempDir(), "restarted")
	cfg := config.Config{
		Backend: localSrv.URL,
		Restart: &config.RestartGroup{
			Enable:   true,
			Interval: 1, // seconds
			Command:  "touch " + marker,
		},
		Cloud: &config.CloudGroup{
			Enable:         true,
			BaseURL:        cloud.URL,
			APIKey:         "sk-cloud",
			LocalMaxTokens: 4096,
			Threshold:      0.9, // the small prompt below stays local, the big one goes to the cloud
			Tokenizer:      "tiktoken",
		},
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sup := New(cfg, ctx)
	sup.StartBackground(ctx)
	srv := httptest.NewServer(sup)
	defer srv.Close()

	// a small local request arms the idle window (deadline T+1s)
	resp, err := http.Post(srv.URL+completionsPath, "application/json",
		bytes.NewBufferString(`{"model":"m","messages":[{"role":"user","content":"hi"}]}`))
	if err != nil {
		t.Fatal(err)
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("small request status = %d, want 200 (local)", resp.StatusCode)
	}

	// a big (slow) cloud request in flight for 3s: the window crosses at T+1s and
	// the next tick (at most T+2s) must run the restart command - while the
	// cloud request is still in flight (it ends at T+3s)
	statusCh := make(chan int, 1)
	go func() {
		body := bigChatBody('x', 20000)
		resp, err := http.Post(srv.URL+completionsPath, "application/json", bytes.NewReader(body))
		if err != nil {
			t.Error(err)
			return
		}
		_, _ = io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		statusCh <- resp.StatusCode
	}()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(marker); err == nil {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if _, err := os.Stat(marker); err != nil {
		t.Fatal("the restart command did not run while the (cloud) request was in flight")
	}
	select {
	case status := <-statusCh:
		if status != http.StatusOK {
			t.Fatalf("cloud request status = %d, want 200 (the guard must not affect cloud traffic)", status)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the cloud request did not complete")
	}
}
