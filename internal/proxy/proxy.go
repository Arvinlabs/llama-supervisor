package proxy

import (
	"context"
	"io"
	"log"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/Arvinlabs/llama-supervisor/internal/config"
	"github.com/Arvinlabs/llama-supervisor/internal/debug"
	"github.com/Arvinlabs/llama-supervisor/internal/probe"
	"github.com/Arvinlabs/llama-supervisor/internal/request"
	"github.com/Arvinlabs/llama-supervisor/internal/restart"
	"github.com/Arvinlabs/llama-supervisor/internal/watchdog"
)

// Supervisor is the reverse proxy with idle restart and probe logic attached
type Supervisor struct {
	proxy       *httputil.ReverseProxy
	backendURL  string
	backendAddr string // backend host:port (validated at startup, must carry an explicit port)

	restart  *restart.Policy  // restart policy (nil when restart is disabled)
	probe    *probe.Policy    // probe policy (nil when probe is disabled)
	watchdog *watchdog.Policy // watchdog policy (nil when watchdog is disabled)
	request  *request.Policy  // request policy (nil when no request sub-feature is enabled)
	debug    *debug.Policy    // debug policy (nil when debug is disabled); Tap/TapOutbound dump inbound/outbound requests when the save paths are set
}

// New creates the reverse proxy to the backend; ctx is the server-level ctx (probes do not follow user requests)
func New(cfg config.Config, ctx context.Context) *Supervisor {
	backend, err := url.Parse(cfg.Backend)
	if err != nil {
		log.Fatalf("backend %q: %v", cfg.Backend, err)
	}
	if backend.Hostname() == "" || backend.Port() == "" {
		log.Fatalf("backend %q: host and explicit port are required", cfg.Backend)
	}
	p := &Supervisor{
		backendURL:  cfg.Backend,
		backendAddr: backend.Host,
	}
	var rp *httputil.ReverseProxy
	if cfg.Request.Enabled() {
		// with a request policy use the Rewrite API (Director has no request hook):
		// SetURL routes to the backend, then the policy modifiers run on the outbound request
		p.request = request.New(cfg.Request, cfg.ApiKey)
		rp = &httputil.ReverseProxy{
			Rewrite: func(pr *httputil.ProxyRequest) {
				pr.SetURL(backend)
				p.request.ModifyRequest(pr.Out)
			},
		}
	} else {
		rp = httputil.NewSingleHostReverseProxy(backend)
	}
	if cfg.Debug.Enabled() {
		p.debug = debug.New(cfg.Debug)
	}
	// outbound request dump: the transport taps the request as it is about to be sent, i.e. after
	// the request policy has rewritten it (when the request policy is enabled)
	if p.debug != nil && cfg.Debug.OutSavePath != "" {
		rp.Transport = &outboundTapTransport{inner: http.DefaultTransport, policy: p.debug}
	}
	// FlushInterval=0: flush after every Write so SSE/streaming output reaches the client unbuffered
	rp.FlushInterval = 0
	// When the client disconnects (request ctx canceled), close the backend response body proactively:
	// Go's HTTP/1.1 response body reads are not governed by the request ctx, otherwise the proxy
	// would stay blocked on the backend stream read and the backend would keep generating
	rp.ModifyResponse = func(res *http.Response) error {
		cb := &ctxBody{ctx: res.Request.Context(), rc: res.Body, quit: make(chan struct{})}
		// only SSE chat completion streams can receive the injected error event; any other
		// response (non-stream JSON, /completion, /slots, ...) keeps the plain body
		if res.Body != nil && shouldInjectStreamError(res) {
			res.Body = &guardBody{inner: cb, ctx: res.Request.Context()}
		} else {
			res.Body = cb
		}
		cb.watch()
		return nil
	}
	p.proxy = rp
	p.proxy.ErrorHandler = func(w http.ResponseWriter, r *http.Request, err error) {
		if cerr := r.Context().Err(); cerr != nil {
			// The client already disconnected (ctx canceled): the backend request was aborted early, not a backend failure
			log.Printf("[proxy] client disconnected, backend aborted: %s %s (%v)",
				r.Method, r.URL.Path, err)
		} else {
			log.Printf("proxy %s %s: %v", r.Method, r.URL.Path, err)
		}
		http.Error(w, "backend unavailable", http.StatusBadGateway)
	}

	if cfg.Restart.Enabled() {
		p.restart = restart.New(cfg.Restart, config.RestartInterval(cfg))
	}
	if cfg.Probe.Enabled() {
		p.probe = probe.New(ctx, cfg.Backend, cfg.Probe, config.ProbeInterval(cfg), cfg.ApiKey)
	}
	if cfg.Watchdog.Enabled() {
		p.watchdog = watchdog.New(cfg.Watchdog, cfg.Backend, cfg.ApiKey)
	}
	if p.restart == nil && p.probe == nil && p.watchdog == nil && p.request == nil && p.debug == nil {
		log.Print("[config] restart, probe, watchdog, request and debug all disabled, proxying only")
	}
	return p
}

// OnHTTPRequest refreshes the idle trackers of the enabled policies
func (p *Supervisor) OnHTTPRequest() {
	if p.restart != nil {
		p.restart.OnHTTPRequest()
	}
	if p.probe != nil {
		p.probe.OnHTTPRequest()
	}
}

// HandleDebug reports whether the request was the debug endpoint and, if so, serves it;
// the debug endpoint is served by the supervisor itself and never proxied
func (p *Supervisor) HandleDebug(w http.ResponseWriter, r *http.Request) bool {
	return p.debug != nil && p.debug.Handle(w, r)
}

// statusRecorder wraps ResponseWriter to record the response status code for the access log
type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (s *statusRecorder) WriteHeader(code int) {
	s.status = code
	s.ResponseWriter.WriteHeader(code)
}

// Flush passes flush through: ReverseProxy only streams by FlushInterval when the ResponseWriter implements http.Flusher
func (s *statusRecorder) Flush() {
	if f, ok := s.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

// outboundTapTransport wraps the default transport and dumps every outbound request (after the
// request policy has rewritten it, when the request policy is enabled) before it is sent to the
// backend
type outboundTapTransport struct {
	inner  http.RoundTripper
	policy *debug.Policy
}

func (t *outboundTapTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	if t.policy != nil {
		t.policy.TapOutbound(r)
	}
	return t.inner.RoundTrip(r)
}

// ctxBody wraps the backend response body: when ctx (the client request context) is canceled
// (client disconnected), the underlying body is closed proactively to interrupt a possibly
// blocked read, so the backend connection is terminated early.
// Go's HTTP/1.1 client response body reads are not governed by the request ctx (only http2
// aborts automatically); without this, after a client disconnect the proxy would still wait
// for the backend stream to finish and the backend would keep running idle
type ctxBody struct {
	ctx  context.Context
	rc   io.ReadCloser
	quit chan struct{}
	once sync.Once
}

func (b *ctxBody) Read(p []byte) (int, error) {
	if err := b.ctx.Err(); err != nil {
		return 0, err
	}
	return b.rc.Read(p)
}

func (b *ctxBody) Close() error {
	b.once.Do(func() {
		close(b.quit)
		// Close before the body is fully read: the http client closes the TCP connection
		// (instead of returning it to the pool), so the backend notices the disconnect
		// and stops processing the current request
		_ = b.rc.Close()
	})
	return nil
}

// watch starts a companion goroutine: on ctx cancel (client disconnect) log and Close the body;
// on normal completion (Close) it exits, no leak.
// Note: the log must be printed here, because a streaming copy error in ReverseProxy does not
// go through ErrorHandler but panics with http.ErrAbortHandler, silently recovered by the server (Issue 23643)
func (b *ctxBody) watch() {
	go func() {
		select {
		case <-b.ctx.Done():
			log.Printf("[proxy] client disconnected, aborting backend stream: %v", b.ctx.Err())
			b.Close()
		case <-b.quit:
		}
	}()
}

// ServeHTTP handles an incoming request: if the probe idle timeout was crossed, probe first
// (on unhealthy run the command and wait for the backend to be ready), then forward, and log the access
func (p *Supervisor) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	start := time.Now()
	// inbound auth: the policy rejects missing/unknown keys (writing the 401 response
	// itself) before the request is proxied
	if p.request != nil && !p.request.Authorize(w, r) {
		logAccess(http.StatusUnauthorized, r, start)
		return
	}
	if p.debug != nil {
		p.debug.Tap(r) // no-op when debug.savePath is not set
	}
	rec := &statusRecorder{ResponseWriter: w}
	// probe idle timeout: probe first; if the command actually ran (backend restarted), wait for the backend to be ready.
	// restart timing needs no manual reset: this request's OnHTTPRequest already refreshed it
	if p.probe != nil && p.probe.ConsumeIdle(ctx) {
		p.waitBackendReady(ctx)
	}
	// On client disconnect: the request ctx is canceled -> ctxBody closes the backend connection ->
	// the backend read is interrupted, and "client disconnected" is logged from ctxBody/ErrorHandler.
	// Note: r.Context().Err() must not be used here to detect this, because net/http also cancels
	// the ctx after the handler returns normally, which would be a false positive.
	p.proxy.ServeHTTP(rec, r)
	logAccess(rec.status, r, start)
}

// StartBackground starts the background checkers:
// restart checks once per second and runs restart.command when the idle deadline is crossed, periodically repeatable;
// watchdog samples /slots at the configured interval and runs watchdog.command when the speed keeps exceeding the limit
func (p *Supervisor) StartBackground(ctx context.Context) {
	if p.restart != nil {
		go func() {
			ticker := time.NewTicker(time.Second)
			defer ticker.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					p.restart.ConsumeIdle(ctx)
				}
			}
		}()
	}
	if p.watchdog != nil {
		go func() {
			ticker := time.NewTicker(p.watchdog.Interval())
			defer ticker.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					p.watchdog.Tick(ctx)
				}
			}
		}()
	}
}

// streamErrorPath is the only proxied path eligible for the injected SSE error event:
// the OpenAI-compatible chat completion endpoint
const streamErrorPath = "/v1/chat/completions"

// shouldInjectStreamError reports whether the response is an SSE chat completion stream
// that can receive the injected error event: the path must be the chat completion endpoint
// and the response must actually be a stream (stream:false responses are plain JSON, as are
// all the other endpoints)
func shouldInjectStreamError(res *http.Response) bool {
	if res.Request.URL.Path != streamErrorPath {
		return false
	}
	return strings.HasPrefix(res.Header.Get("Content-Type"), "text/event-stream")
}

// logAccess logs one access line: client, method, path, status code, elapsed time
func logAccess(status int, r *http.Request, start time.Time) {
	if status == 0 {
		status = http.StatusOK
	}
	log.Printf("[access] %s %s %s %d %s", r.RemoteAddr, r.Method, r.URL.RequestURI(), status, time.Since(start).Round(time.Microsecond))
}

// waitBackendReady polls the backend /health after a command ran, until the service is truly ready (2xx) before forwarding
func (p *Supervisor) waitBackendReady(ctx context.Context) {
	log.Printf("[proxy] waiting for backend %s to be ready", p.backendAddr)
	client := &http.Client{Timeout: 5 * time.Second}
	for {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, p.backendURL+"/health", nil)
		ok := false
		if err == nil {
			resp, err := client.Do(req)
			if err == nil {
				resp.Body.Close()
				ok = resp.StatusCode >= http.StatusOK && resp.StatusCode < http.StatusMultipleChoices
			}
		}
		if ok {
			log.Print("[proxy] backend ready")
			return
		}
		select {
		case <-time.After(500 * time.Millisecond):
		case <-ctx.Done():
			log.Printf("[proxy] wait canceled: %v", ctx.Err())
			return
		}
	}
}
