package localctx

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestProviderFromProps(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/props":
			_, _ = w.Write([]byte(`{"n_ctx":8192,"n_gpu_layers":99}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()
	p := New(srv.URL, "", 0)
	if v, err := p.ContextWindow(context.Background()); err != nil || v != 8192 {
		t.Fatalf("ContextWindow = %d, %v; want 8192, nil", v, err)
	}
}

func TestProviderFromModelsSecondSource(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/props":
			_, _ = w.Write([]byte(`{"n_gpu_layers":99}`)) // no n_ctx field
		case "/v1/models":
			_, _ = w.Write([]byte(`{"data":[{"id":"m.gguf [n_ctx=4096]"}]}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()
	p := New(srv.URL, "", 0)
	if v, err := p.ContextWindow(context.Background()); err != nil || v != 4096 {
		t.Fatalf("ContextWindow = %d, %v; want 4096, nil", v, err)
	}
}

func TestProviderFromPropsNested(t *testing.T) {
	// the llama-swarm build nests n_ctx under default_generation_settings
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/props" {
			_, _ = w.Write([]byte(`{"default_generation_settings":{"n_ctx":204800}}`))
			return
		}
		http.NotFound(w, r)
	}))
	defer srv.Close()
	p := New(srv.URL, "", 0)
	if v, err := p.ContextWindow(context.Background()); err != nil || v != 204800 {
		t.Fatalf("ContextWindow = %d, %v; want 204800, nil", v, err)
	}
}

func TestProviderFromModelsMeta(t *testing.T) {
	// the structured meta.n_ctx field (no n_ctx= suffix in the id)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/props":
			http.NotFound(w, r)
		case "/v1/models":
			_, _ = w.Write([]byte(`{"data":[{"id":"qwen-local","meta":{"n_ctx":204800}}]}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()
	p := New(srv.URL, "", 0)
	if v, err := p.ContextWindow(context.Background()); err != nil || v != 204800 {
		t.Fatalf("ContextWindow = %d, %v; want 204800, nil", v, err)
	}
}

func TestProviderOverrideSkipsProbing(t *testing.T) {
	// an unreachable backend: the override must be served without any probe
	p := New("http://127.0.0.1:1", "", 1234)
	if v, err := p.ContextWindow(context.Background()); err != nil || v != 1234 {
		t.Fatalf("ContextWindow = %d, %v; want 1234, nil", v, err)
	}
}

func TestProviderCachesWithinTTL(t *testing.T) {
	var probes int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		probes++
		_, _ = w.Write([]byte(`{"n_ctx":8192}`))
	}))
	defer srv.Close()
	p := New(srv.URL, "", 0)
	for i := 0; i < 3; i++ {
		if v, err := p.ContextWindow(context.Background()); err != nil || v != 8192 {
			t.Fatalf("call %d: ContextWindow = %d, %v", i, v, err)
		}
	}
	if probes != 1 {
		t.Fatalf("probes = %d, want 1 (TTL cache)", probes)
	}
}

func TestProviderStaleValueOnFailure(t *testing.T) {
	var down bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if down {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		_, _ = w.Write([]byte(`{"n_ctx":4096}`))
	}))
	p := New(srv.URL, "", 0)
	if v, _ := p.ContextWindow(context.Background()); v != 4096 {
		t.Fatalf("first ContextWindow = %d, want 4096", v)
	}
	down = true
	p.expires = p.expires.Add(-1) // force the probe (the backoff state is clean after a success)
	if v, err := p.ContextWindow(context.Background()); err != nil || v != 4096 {
		t.Fatalf("ContextWindow after failure = %d, %v; want the stale 4096, nil", v, err)
	}
	srv.Close()
}

func TestProviderNoValue(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()
	p := New(srv.URL, "", 0)
	if _, err := p.ContextWindow(context.Background()); err != ErrNoValue {
		t.Fatalf("err = %v, want ErrNoValue", err)
	}
}
