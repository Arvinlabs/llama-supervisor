// Package localctx detects the local llama.cpp backend's context window (n_ctx):
// the threshold the cloud router compares request sizes against. The
// value is cached with a TTL because the supervisor itself restarts the backend
// (n_ctx may change with the startup parameters), and a failing probe falls back
// to the stale value rather than blinding the router
package localctx

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"strconv"
	"sync"
	"time"

	"github.com/tidwall/gjson"
)

const (
	ttl          = 5 * time.Minute
	fetchTimeout = 5 * time.Second
	minBackoff   = 5 * time.Second
	maxBackoff   = 5 * time.Minute
)

// nctxInID matches the "n_ctx=<n>" suffix newer llama.cpp builds append to the
// model ids served by GET /v1/models
var nctxInID = regexp.MustCompile(`n_ctx=(\d+)`)

// ErrNoValue no context window value is known (no override, detection never succeeded)
var ErrNoValue = fmt.Errorf("local context window unknown")

// Provider resolves the local context window on demand, caching the last
// successful probe for ttl
type Provider struct {
	backend  string
	apiKey   string // the global backend key: key-protected deployments require it on /props and /v1/models too
	override int    // >0: used directly, no probing
	client   *http.Client

	mu      sync.Mutex
	fetchMu sync.Mutex // at most one probe in flight
	value   int
	expires time.Time
	nextTry time.Time
	backoff time.Duration
}

// New builds a provider for the backend base URL; apiKey is the global backend
// key (sent on the probe requests, empty when the backend has no key); override > 0
// disables probing
func New(backend, apiKey string, override int) *Provider {
	return &Provider{
		backend:  backend,
		apiKey:   apiKey,
		override: override,
		client:   &http.Client{Timeout: fetchTimeout},
	}
}

// ContextWindow returns the current local context window in tokens. A failing
// probe returns the stale value when one is cached; when no value has ever
// been fetched it reports ErrNoValue (the caller keeps the request local)
func (p *Provider) ContextWindow(ctx context.Context) (int, error) {
	if p.override > 0 {
		return p.override, nil
	}
	p.mu.Lock()
	fresh := p.value > 0 && time.Now().Before(p.expires)
	backingOff := !p.nextTry.IsZero() && time.Now().Before(p.nextTry)
	p.mu.Unlock()
	if fresh || backingOff {
		p.mu.Lock()
		v := p.value
		p.mu.Unlock()
		if v > 0 {
			return v, nil
		}
		return 0, ErrNoValue
	}
	if v, err := p.probe(ctx); err == nil {
		return v, nil
	}
	p.mu.Lock()
	stale := p.value
	p.mu.Unlock()
	if stale > 0 {
		return stale, nil
	}
	return 0, ErrNoValue
}

// probe runs one fetch (serialized) and updates the cache/backoff state
func (p *Provider) probe(ctx context.Context) (int, error) {
	p.fetchMu.Lock()
	defer p.fetchMu.Unlock()
	v, err := p.fetch(ctx)
	p.mu.Lock()
	defer p.mu.Unlock()
	if err == nil && v > 0 {
		p.value = v
		p.expires = time.Now().Add(ttl)
		p.nextTry = time.Time{}
		p.backoff = 0
		return v, nil
	}
	wait := p.backoff
	if wait <= 0 {
		wait = minBackoff
	}
	p.backoff = wait * 2
	if p.backoff > maxBackoff {
		p.backoff = maxBackoff
	}
	p.nextTry = time.Now().Add(wait)
	return 0, err
}

// fetch probes the backend once: GET /props (the n_ctx field, in a few
// build-specific locations), falling back to GET /v1/models (the meta.n_ctx
// field, or the n_ctx= suffix of the model ids)
func (p *Provider) fetch(ctx context.Context) (int, error) {
	var lastErr error
	for _, path := range []string{"/props", "/v1/models"} {
		if v, err := p.getNctx(ctx, path); err == nil {
			return v, nil
		} else {
			lastErr = err
		}
	}
	return 0, lastErr
}

func (p *Provider) getNctx(ctx context.Context, path string) (int, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, p.backend+path, nil)
	if err != nil {
		return 0, err
	}
	if p.apiKey != "" {
		req.Header.Set("Authorization", "Bearer "+p.apiKey)
	}
	resp, err := p.client.Do(req)
	if err != nil {
		return 0, fmt.Errorf("GET %s: %w", path, err)
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return 0, err
	}
	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("GET %s: %d", path, resp.StatusCode)
	}
	if path == "/props" {
		// the plain server puts n_ctx at the top level; the llama-swarm build
		// nests it under default_generation_settings
		for _, field := range []string{"n_ctx", "default_generation_settings.n_ctx"} {
			if v := gjson.GetBytes(data, field).Int(); v > 0 {
				return int(v), nil
			}
		}
		return 0, fmt.Errorf("GET %s: no n_ctx field", path)
	}
	// /v1/models: the structured meta.n_ctx field first, then the n_ctx=
	// suffix some builds append to the model ids
	for _, m := range gjson.GetBytes(data, "data").Array() {
		if v := m.Get("meta.n_ctx").Int(); v > 0 {
			return int(v), nil
		}
		id := m.Get("id").String()
		if loc := nctxInID.FindStringSubmatchIndex(id); loc != nil {
			if v, err := strconv.Atoi(id[loc[2]:loc[3]]); err == nil && v > 0 {
				return v, nil
			}
		}
	}
	return 0, fmt.Errorf("GET %s: no n_ctx in the model ids", path)
}
