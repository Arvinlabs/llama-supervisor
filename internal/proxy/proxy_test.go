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
	"testing"
	"time"

	"github.com/Arvinlabs/llama-supervisor/internal/config"
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
}
