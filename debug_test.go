package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestDebugHandleRunsCommand(t *testing.T) {
	p := newDebugPolicy(&DebugGroup{Enable: true, Command: "true"})

	req := httptest.NewRequest(http.MethodGet, p.path, nil)
	w := httptest.NewRecorder()
	if !p.handle(w, req) {
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
	p := newDebugPolicy(&DebugGroup{Enable: true, Command: "true"})
	req := httptest.NewRequest(http.MethodPost, p.path, nil)
	w := httptest.NewRecorder()
	handled := p.handle(w, req)
	if !handled || w.Code != http.StatusOK {
		t.Fatalf("POST should be accepted, handled=%v code=%d", handled, w.Code)
	}
}

func TestDebugHandlePathMiss(t *testing.T) {
	p := newDebugPolicy(&DebugGroup{Enable: true, Command: "true"})
	req := httptest.NewRequest(http.MethodGet, "/v1/chat/completions", nil)
	w := httptest.NewRecorder()
	if p.handle(w, req) {
		t.Fatal("non-debug paths must not be handled")
	}
	if w.Body.Len() != 0 {
		t.Fatalf("non-debug paths must not write anything, got %q", w.Body.String())
	}
}

func TestDebugHandleMethodNotAllowed(t *testing.T) {
	p := newDebugPolicy(&DebugGroup{Enable: true, Command: "true"})
	req := httptest.NewRequest(http.MethodPut, p.path, nil)
	w := httptest.NewRecorder()
	if !p.handle(w, req) {
		t.Fatal("the debug path must be handled even for other methods")
	}
	if w.Code != http.StatusMethodNotAllowed {
		t.Fatalf("status = %d, want 405", w.Code)
	}
}

func TestDebugHandleNoCommand(t *testing.T) {
	p := newDebugPolicy(&DebugGroup{Enable: true})
	req := httptest.NewRequest(http.MethodGet, p.path, nil)
	w := httptest.NewRecorder()
	if !p.handle(w, req) || w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", w.Code)
	}
}

func TestDebugHandleFailingCommand(t *testing.T) {
	p := newDebugPolicy(&DebugGroup{Enable: true, Command: "false"})
	req := httptest.NewRequest(http.MethodGet, p.path, nil)
	w := httptest.NewRecorder()
	if !p.handle(w, req) {
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
	p := newDebugPolicy(&DebugGroup{Enable: true, Command: "true"})
	if p.path != defaultDebugPath {
		t.Fatalf("path = %q, want %q", p.path, defaultDebugPath)
	}
	p = newDebugPolicy(&DebugGroup{Enable: true, Path: "/x", Command: "true"})
	if p.path != "/x" {
		t.Fatalf("path = %q, want /x", p.path)
	}
}
