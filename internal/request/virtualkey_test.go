package request

import (
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
)

func newTestVirtualKeyPolicy(backendKey string) *virtualKeyPolicy {
	return newVirtualKeyPolicy([]string{"key-a", "key-b", ""}, backendKey)
}

// the client key is extracted from the OpenAI-style Authorization header, the raw
// llama.cpp header value and the api_key query parameter
func TestVirtualKeyClientKeyExtraction(t *testing.T) {
	p := newTestVirtualKeyPolicy("bk")
	cases := []struct {
		url  string
		auth string
		want string
	}{
		{"/v1/chat/completions", "Bearer key-a", "key-a"},
		{"/v1/chat/completions", "key-a", "key-a"},
		{"/v1/chat/completions?api_key=key-b", "", "key-b"},
		{"/v1/chat/completions", "", ""},
	}
	for _, c := range cases {
		r := httptest.NewRequest(http.MethodPost, c.url, nil)
		if c.auth != "" {
			r.Header.Set("Authorization", c.auth)
		}
		if got := p.clientKey(r); got != c.want {
			t.Errorf("clientKey(%q, auth=%q) = %q, want %q", c.url, c.auth, got, c.want)
		}
	}
}

// Authorize accepts configured keys only
func TestVirtualKeyAuthorize(t *testing.T) {
	p := newTestVirtualKeyPolicy("bk")
	mk := func(auth string) *http.Request {
		r := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil)
		if auth != "" {
			r.Header.Set("Authorization", auth)
		}
		return r
	}
	for _, auth := range []string{"Bearer key-a", "Bearer key-b", "key-a"} {
		if !p.Authorize(mk(auth)) {
			t.Errorf("Authorize(%q) = false, want true", auth)
		}
	}
	for _, auth := range []string{"Bearer nope", "", "Bearer "} {
		if p.Authorize(mk(auth)) {
			t.Errorf("Authorize(%q) = true, want false", auth)
		}
	}
}

// ModifyRequest re-signs the outbound request: the virtual key is replaced by the
// backend key, or dropped when no backend key is configured; the api_key query
// parameter is always dropped
func TestVirtualKeyModifyRequest(t *testing.T) {
	for _, tc := range []struct {
		backendKey string
		inAuth     string
		inQuery    string
		wantAuth   string
		wantQuery  string
	}{
		{"bk", "Bearer key-a", "", "Bearer bk", ""},
		{"bk", "key-a", "api_key=key-a", "Bearer bk", ""},
		{"", "Bearer key-a", "", "", ""},
	} {
		p := newTestVirtualKeyPolicy(tc.backendKey)
		u := "/v1/chat/completions"
		if tc.inQuery != "" {
			u += "?" + tc.inQuery
		}
		r := httptest.NewRequest(http.MethodPost, u, nil)
		r.Header.Set("Authorization", tc.inAuth)
		p.ModifyRequest(r)
		if got := r.Header.Get("Authorization"); got != tc.wantAuth {
			t.Errorf("ModifyRequest(backend=%q): Authorization = %q, want %q", tc.backendKey, got, tc.wantAuth)
		}
		if got := r.URL.RawQuery; got != tc.wantQuery {
			t.Errorf("ModifyRequest(backend=%q): RawQuery = %q, want %q", tc.backendKey, got, tc.wantQuery)
		}
	}
}

// the policy writes an OpenAI-format 401 JSON error for a rejected request
func TestWriteUnauthorized(t *testing.T) {
	w := httptest.NewRecorder()
	(&Policy{}).writeUnauthorized(w)
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401", w.Code)
	}
	if ct := w.Header().Get("Content-Type"); ct != "application/json" {
		t.Errorf("Content-Type = %q, want application/json", ct)
	}
	body, _ := io.ReadAll(w.Body)
	want := `{"error":{"code":"invalid_api_key","message":"invalid or missing api key","type":"invalid_request_error"}}`
	if string(body) != want+"\n" {
		t.Errorf("body = %s, want %s", body, want)
	}
}
