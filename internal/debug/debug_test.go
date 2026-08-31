package debug

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Arvinlabs/llama-supervisor/internal/config"
)

func TestDebugHandleRunsCommand(t *testing.T) {
	p := New(&config.DebugGroup{Enable: true, Command: "true"})

	req := httptest.NewRequest(http.MethodGet, p.path, nil)
	w := httptest.NewRecorder()
	if !p.Handle(w, req) {
		t.Fatal("expected the debug endpoint to handle the request")
	}
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", w.Code)
	}
	var resp debugResponse
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if resp.Status != "ok" || resp.Elapsed == "" {
		t.Fatalf("unexpected response: %+v", resp)
	}
}

func TestDebugHandlePost(t *testing.T) {
	p := New(&config.DebugGroup{Enable: true, Command: "true"})
	req := httptest.NewRequest(http.MethodPost, p.path, nil)
	w := httptest.NewRecorder()
	handled := p.Handle(w, req)
	if !handled || w.Code != http.StatusOK {
		t.Fatalf("POST should be accepted, handled=%v code=%d", handled, w.Code)
	}
}

func TestDebugHandlePathMiss(t *testing.T) {
	p := New(&config.DebugGroup{Enable: true, Command: "true"})
	req := httptest.NewRequest(http.MethodGet, "/v1/chat/completions", nil)
	w := httptest.NewRecorder()
	if p.Handle(w, req) {
		t.Fatal("non-debug paths must not be handled")
	}
	if w.Body.Len() != 0 {
		t.Fatalf("non-debug paths must not write anything, got %q", w.Body.String())
	}
}

func TestDebugHandleMethodNotAllowed(t *testing.T) {
	p := New(&config.DebugGroup{Enable: true, Command: "true"})
	req := httptest.NewRequest(http.MethodPut, p.path, nil)
	w := httptest.NewRecorder()
	if !p.Handle(w, req) {
		t.Fatal("the debug path must be handled even for other methods")
	}
	if w.Code != http.StatusMethodNotAllowed {
		t.Fatalf("status = %d, want 405", w.Code)
	}
}

func TestDebugHandleNoCommand(t *testing.T) {
	p := New(&config.DebugGroup{Enable: true})
	req := httptest.NewRequest(http.MethodGet, p.path, nil)
	w := httptest.NewRecorder()
	if !p.Handle(w, req) || w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", w.Code)
	}
}

func TestDebugHandleFailingCommand(t *testing.T) {
	p := New(&config.DebugGroup{Enable: true, Command: "false"})
	req := httptest.NewRequest(http.MethodGet, p.path, nil)
	w := httptest.NewRecorder()
	if !p.Handle(w, req) {
		t.Fatal("expected the debug endpoint to handle the request")
	}
	if w.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500", w.Code)
	}
	var resp debugResponse
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if resp.Status != "failed" {
		t.Fatalf("status = %q, want failed", resp.Status)
	}
}

func TestDebugHandleDefaultPath(t *testing.T) {
	p := New(&config.DebugGroup{Enable: true, Command: "true"})
	if p.path != DefaultDebugPath {
		t.Fatalf("path = %q, want %q", p.path, DefaultDebugPath)
	}
	p = New(&config.DebugGroup{Enable: true, Path: "/x", Command: "true"})
	if p.path != "/x" {
		t.Fatalf("path = %q, want /x", p.path)
	}
}

// Tap dumps the request line and all headers immediately, and the JSON body is stored
// pretty-printed (2-space indent) once the body is fully read and closed
func TestPolicyTapSavesJSONBodyPrettyPrinted(t *testing.T) {
	dir := t.TempDir()
	p := New(&config.DebugGroup{Enable: true, SavePath: dir})

	body := `{"prompt":"hi","model":"llama","stream":true}`
	r := httptest.NewRequest(http.MethodPost, "/v1/chat/completions?stream=true", strings.NewReader(body))
	r.Host = "llama.example.com:8080"
	r.Header.Set("Content-Type", "application/json")
	p.Tap(r)

	// the body is still fully readable (buffered, not consumed)
	got, err := io.ReadAll(r.Body)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != body {
		t.Fatalf("body = %q, want %q", got, body)
	}
	if err := r.Body.Close(); err != nil {
		t.Fatal(err)
	}

	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 {
		t.Fatalf("expected 1 saved file, got %d", len(entries))
	}
	data, err := os.ReadFile(filepath.Join(dir, entries[0].Name()))
	if err != nil {
		t.Fatal(err)
	}
	s2 := string(data)
	if !strings.HasPrefix(s2, "POST /v1/chat/completions?stream=true HTTP/1.1\r\n") {
		t.Fatalf("request line missing: %q", s2[:min(len(s2), 60)])
	}
	for _, want := range []string{
		"Host: llama.example.com:8080",
		"Content-Type: application/json",
	} {
		if !strings.Contains(s2, want) {
			t.Fatalf("dump missing %q:\n%s", want, s2)
		}
	}
	// the body must be the last part of the dump, pretty-printed with 2-space indent
	var expected bytes.Buffer
	if err := json.Indent(&expected, []byte(body), "", "  "); err != nil {
		t.Fatal(err)
	}
	if !strings.HasSuffix(s2, expected.String()) {
		t.Fatalf("dump must end with the pretty-printed body:\n%s\nwant suffix:\n%s", s2, expected.String())
	}
}

// non-JSON bodies (plain text and binary) are stored base64-encoded so the dump
// stays a plain text file
func TestPolicyTapSavesNonJSONBodyAsBase64(t *testing.T) {
	for name, body := range map[string][]byte{
		"plain text": []byte("not json at all"),
		"binary":     []byte{0x00, 0x01, 0xff, 0xfe, 0x7f},
	} {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			p := New(&config.DebugGroup{Enable: true, SavePath: dir})

			r := httptest.NewRequest(http.MethodPost, "/x", bytes.NewReader(body))
			p.Tap(r)
			// the body is still fully readable (buffered, not consumed)
			if got, err := io.ReadAll(r.Body); err != nil || !bytes.Equal(got, body) {
				t.Fatalf("body = %q, want %q (err=%v)", got, body, err)
			}
			if err := r.Body.Close(); err != nil {
				t.Fatal(err)
			}

			entries, err := os.ReadDir(dir)
			if err != nil {
				t.Fatal(err)
			}
			if len(entries) != 1 {
				t.Fatalf("expected 1 saved file, got %d", len(entries))
			}
			data, err := os.ReadFile(filepath.Join(dir, entries[0].Name()))
			if err != nil {
				t.Fatal(err)
			}
			want := base64.StdEncoding.EncodeToString(body)
			if !strings.HasSuffix(string(data), "\r\n\r\n"+want) {
				t.Fatalf("body must be stored base64-encoded as the last part:\n%s\nwant suffix: %s", data, want)
			}
		})
	}
}

// closing the body twice (the proxy may do this) must not fail or double-write the dump
func TestPolicyTapDoubleClose(t *testing.T) {
	dir := t.TempDir()
	p := New(&config.DebugGroup{Enable: true, SavePath: dir})

	body := `{"prompt":"hi"}`
	r := httptest.NewRequest(http.MethodPost, "/x", strings.NewReader(body))
	p.Tap(r)
	if _, err := io.ReadAll(r.Body); err != nil {
		t.Fatal(err)
	}
	if err := r.Body.Close(); err != nil {
		t.Fatal(err)
	}
	if err := r.Body.Close(); err != nil {
		t.Fatalf("second close: %v", err)
	}

	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 {
		t.Fatalf("expected 1 saved file, got %d", len(entries))
	}
	data, err := os.ReadFile(filepath.Join(dir, entries[0].Name()))
	if err != nil {
		t.Fatal(err)
	}
	var expected bytes.Buffer
	if err := json.Indent(&expected, []byte(body), "", "  "); err != nil {
		t.Fatal(err)
	}
	if !strings.HasSuffix(string(data), expected.String()) {
		t.Fatalf("dump must end with the pretty-printed body:\n%s", data)
	}
}
