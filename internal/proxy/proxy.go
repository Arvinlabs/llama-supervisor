package proxy

import (
	"context"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Arvinlabs/llama-supervisor/internal/anthropic"
	"github.com/Arvinlabs/llama-supervisor/internal/config"
	"github.com/Arvinlabs/llama-supervisor/internal/debug"
	"github.com/Arvinlabs/llama-supervisor/internal/errguard"
	"github.com/Arvinlabs/llama-supervisor/internal/observe"
	"github.com/Arvinlabs/llama-supervisor/internal/probe"
	"github.com/Arvinlabs/llama-supervisor/internal/request"
	"github.com/Arvinlabs/llama-supervisor/internal/restart"
	"github.com/Arvinlabs/llama-supervisor/internal/stats"
	"github.com/Arvinlabs/llama-supervisor/internal/watchdog"
)

// Supervisor is the reverse proxy with idle restart and probe logic attached
type Supervisor struct {
	proxy       *httputil.ReverseProxy
	backendURL  string
	backendAddr string // backend host:port (validated at startup, must carry an explicit port)

	cloudRoute *cloudRouter           // cloud fallback router (nil when the cloud is disabled)
	cloudProxy *httputil.ReverseProxy // the second reverse proxy, to the cloud endpoint

	hub     *observe.Hub // completion observation hub; stats and watchdog subscribe to it
	observe bool         // whether the local completion tap is active (at least one consumer is registered)
	// observeCloud whether cloud completions must be observed too: only the
	// consumers that opted in to cloud traffic (stats) need it, the watchdog
	// never sees cloud completions (see observe.CloudConsumer)
	observeCloud bool

	restart   *restart.Policy    // restart policy (nil when restart is disabled)
	probe     *probe.Policy      // probe policy (nil when probe is disabled)
	watchdog  *watchdog.Policy   // watchdog policy (nil when watchdog is disabled)
	request   *request.Policy    // request policy (nil when no request sub-feature is enabled)
	stats     *stats.Policy      // stats consumer (nil when stats is disabled); accounts /v1/chat/completions token usage per day
	debug     *debug.Policy      // debug policy (nil when debug is disabled); Tap/TapOutbound dump inbound/outbound requests when the save paths are set
	errguard  *errguard.Policy   // error guard policy (nil when disabled); runs the command when a tapped backend error message matches a pattern
	anthropic *anthropic.Handler // Anthropic /v1/messages handler (nil when the anthropic protocol is disabled)
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
		hub:         &observe.Hub{},
	}
	if cfg.Request.Enabled() {
		p.request = request.New(cfg.Request, cfg.ApiKey)
	}
	if cfg.Stats.Enabled() {
		p.stats = stats.New(cfg.Stats)
		p.hub.Register(p.stats)
	}
	// the completion tap is always wired via the Rewrite API (Director has no request hook):
	// SetURL routes to the backend, the request policy rewrites it, then injectUsage forces
	// stream_options.include_usage so the backend always reports usage (and the timings that
	// carry the MTP draft stats). injectUsage only runs when at least one consumer subscribed
	rp := &httputil.ReverseProxy{
		Rewrite: func(pr *httputil.ProxyRequest) {
			pr.SetURL(backend)
			if p.request != nil {
				p.request.ModifyRequest(pr.Out)
			}
			if p.observe {
				injectUsage(pr.Out)
			}
			negotiateCompression(pr.Out)
		},
	}
	if cfg.Debug.Enabled() {
		p.debug = debug.New(cfg.Debug)
	}
	if cfg.ErrGuard.Enabled() {
		eg, err := errguard.New(cfg.ErrGuard, ctx)
		if err != nil {
			log.Fatalf("errguard: %v", err)
		}
		p.errguard = eg
	}
	// the Anthropic handler proxies /v1/messages to the supervisor itself: the
	// loopback request runs the full OpenAI pipeline above (the client's API key
	// is passed through, so the inner request policy is the single auth point)
	if cfg.Anthropic.Enabled() {
		p.anthropic = anthropic.New(selfBaseURL(cfg.Host, cfg.Port), cfg.Anthropic.ModelMap)
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
		return p.wrapResponse(res, p.errguard, false)
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
	// cloud fallback: a second reverse proxy to the cloud endpoint. It runs the same
	// outbound pipeline as the local one (request policy, usage injection, response
	// wraps) plus the cloud-specific rewrites (auth, model, sanitize), and its
	// response tap carries a nil error guard: a cloud error payload must not trigger
	// the local self-heal command. Usage is only injected when a consumer actually
	// accounts cloud traffic (stats)
	if cfg.Cloud.Enabled() {
		cloud, cerr := url.Parse(strings.TrimRight(cfg.Cloud.BaseURL, "/"))
		if cerr != nil {
			log.Fatalf("cloud baseUrl %q: %v", cfg.Cloud.BaseURL, cerr)
		}
		p.cloudRoute = newCloudRouter(cfg, cfg.Backend)
		crp := &httputil.ReverseProxy{
			Rewrite: func(pr *httputil.ProxyRequest) {
				pr.SetURL(cloud)
				if p.request != nil {
					p.request.ModifyRequest(pr.Out)
				}
				if p.observeCloud {
					injectUsage(pr.Out)
				}
				p.cloudRoute.rewriteModel(pr.Out)
				sanitizeCloudBody(pr.Out)
				p.cloudRoute.setAuth(pr.Out)
				negotiateCompression(pr.Out)
			},
			FlushInterval: 0,
		}
		// connection-level timeouts (dial/TLS/response headers), so a black-holed
		// cloud endpoint fails instead of hanging the request forever
		var cloudRT http.RoundTripper = newCloudTransport()
		if p.debug != nil && cfg.Debug.OutSavePath != "" {
			cloudRT = &outboundTapTransport{inner: cloudRT, policy: p.debug}
		}
		crp.Transport = cloudRT
		crp.ModifyResponse = func(res *http.Response) error {
			return p.wrapResponse(res, nil, true)
		}
		crp.ErrorHandler = func(w http.ResponseWriter, r *http.Request, err error) {
			p.cloudErrorHandler(w, r, err)
		}
		p.cloudProxy = crp
	}

	if cfg.Restart.Enabled() {
		p.restart = restart.New(cfg.Restart, config.RestartInterval(cfg))
	}
	if cfg.Probe.Enabled() {
		p.probe = probe.New(ctx, cfg.Backend, cfg.Probe, config.ProbeInterval(cfg), cfg.ApiKey)
	}
	if cfg.Watchdog.Enabled() {
		p.watchdog = watchdog.New(cfg.Watchdog, cfg.Backend, cfg.ApiKey)
		p.hub.Register(p.watchdog)
		p.hub.RegisterStream(p.watchdog) // the over-speed trigger judges in-flight completions by their kind and generated content
	}
	// the local completion tap is active whenever any consumer (stats or watchdog)
	// subscribed; cloud completions are only observed for the consumers that opted
	// in to cloud traffic (stats), so the watchdog never touches cloud traffic
	p.observe = p.hub.Len() > 0
	p.observeCloud = p.hub.CloudLen() > 0
	if p.restart == nil && p.probe == nil && p.watchdog == nil && p.request == nil && p.cloudRoute == nil && p.anthropic == nil && p.stats == nil && p.debug == nil && p.errguard == nil {
		log.Print("[config] restart, probe, watchdog, request, cloud, anthropic, stats, debug and errguard all disabled, proxying only")
	}
	return p
}

// OnHTTPRequest refreshes the idle trackers of the enabled policies: it is called
// for the requests that actually use the local backend (see ServeHTTP), never for
// a cloud-routed one
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

// HandleStats reports whether the request was the stats page (/stats) or its data
// (/stats/data) and, if so, serves it; both are served by the supervisor itself
// and never proxied
func (p *Supervisor) HandleStats(w http.ResponseWriter, r *http.Request) bool {
	return p.stats != nil && p.stats.Handle(w, r)
}

// HandleAnthropic reports whether the request was the Anthropic Messages endpoint
// (/v1/messages) and, if so, serves it; the endpoint is served by the supervisor
// itself (via a self request through the OpenAI pipeline) and never proxied
func (p *Supervisor) HandleAnthropic(w http.ResponseWriter, r *http.Request) bool {
	return p.anthropic != nil && p.anthropic.Serve(w, r)
}

// SetSelfURL replaces the Anthropic handler's self request base URL; used by tests
// that listen on an ephemeral port instead of the configured one
func (p *Supervisor) SetSelfURL(u string) {
	if p.anthropic != nil {
		p.anthropic.SetSelfURL(u)
	}
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

// ServeHTTP handles an incoming request: route first (cloud fallback when the request
// does not fit the local context, local otherwise), then - for a local request only -
// refresh the idle trackers and run the probe when the idle timeout was crossed (on an
// unhealthy verdict it runs the command and waits for the backend to be ready), forward,
// and log the access
func (p *Supervisor) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	start := time.Now()
	// inbound auth: the policy rejects missing/unknown keys (writing the 401 response
	// itself) before the request is proxied
	if p.request != nil && !p.request.Authorize(w, r) {
		logAccess(http.StatusUnauthorized, "-", r, start)
		return
	}
	if p.debug != nil {
		p.debug.Tap(r) // no-op when debug.savePath is not set
	}
	rec := &statusRecorder{ResponseWriter: w}
	// cloud fallback: the router buffers the body (reinstalled either way) and reports
	// whether the request does not fit the local context window
	routeToCloud := false
	if p.cloudRoute != nil {
		routeToCloud = p.cloudRoute.Route(ctx, r)
	}
	// only a request that actually goes to the local model is backend activity: it
	// refreshes the restart/probe idle timers, and if the probe idle timeout was crossed
	// the backend is probed first (when the command actually ran, wait for the backend to
	// be ready before forwarding). A cloud-routed request must do neither: it leaves the
	// local model alone, so it can neither keep it from being restarted/unloaded as idle
	// nor make the request wait for a local restart it does not depend on
	if !routeToCloud {
		p.OnHTTPRequest()
		if p.probe != nil && p.probe.ConsumeIdle(ctx) {
			p.waitBackendReady(ctx)
			// the backend may have just been (re)started: the routing decision above was
			// made against an unreachable or stale backend, so re-evaluate it before
			// forwarding - an oversized prompt must not hit a fresh, empty context window
			if p.cloudRoute != nil {
				routeToCloud = p.cloudRoute.Route(ctx, r)
			}
		}
	}
	// On client disconnect: the request ctx is canceled -> ctxBody closes the backend connection ->
	// the backend read is interrupted, and "client disconnected" is logged from ctxBody/ErrorHandler.
	// Note: r.Context().Err() must not be used here to detect this, because net/http also cancels
	// the ctx after the handler returns normally, which would be a false positive.
	// watchdog probing is request-driven: a forwarded chat completion request starts the
	// /slots sampling loop (first fetch a fixed 1s later, then per interval) and the last
	// one in flight to end stops it; other paths never trigger sampling. A cloud-routed
	// request never samples: /slots is a local-only endpoint
	if p.watchdog != nil && r.URL.Path == completionsPath && !routeToCloud {
		p.watchdog.OnRequestStart()
		defer p.watchdog.OnRequestEnd()
	}
	if routeToCloud {
		// cloud.maxConcurrency: wait for a free slot (or the client to go away)
		if !p.cloudRoute.acquire(ctx) {
			log.Printf("[cloud] client gone while waiting for a cloud slot: %s %s", r.Method, r.URL.Path)
			writeCloudError(rec, http.StatusBadGateway)
			logAccess(rec.status, targetName(true), r, start)
			return
		}
		defer p.cloudRoute.release()
		p.cloudProxy.ServeHTTP(rec, r)
	} else {
		p.proxy.ServeHTTP(rec, r)
	}
	logAccess(rec.status, targetName(routeToCloud), r, start)
}

// StartBackground starts the background checkers:
// restart checks once per second and runs restart.command when the idle deadline is crossed, periodically repeatable.
// (the watchdog's /slots sampling is request-driven and runs inside the watchdog policy itself)
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
}

// selfBaseURL computes the supervisor's own loopback base URL for the Anthropic
// self requests: the configured host when it is a specific address, the
// loopback interface otherwise
func selfBaseURL(host string, port int) string {
	switch host {
	case "", "0.0.0.0", "::":
		host = "127.0.0.1"
	}
	return "http://" + net.JoinHostPort(host, strconv.Itoa(port))
}

// wrapResponse wraps the backend response body with the shared response pipeline
// (used by both the local and the cloud proxy): early abort on client disconnect
// (ctxBody), the injected stream error event on abnormal ends (guardBody), the
// completion observation tap (the error guard is passed per proxy: the cloud
// responses use a nil guard, so a cloud error payload never triggers the local
// self-heal command), and the response dump when debug is enabled.
// cloud tells whether the response comes from the cloud backend: its observation
// is tagged Cloud and only delivered to the consumers that opted in (stats), and
// the local consumers (watchdog) never see it
func (p *Supervisor) wrapResponse(res *http.Response, guard *errguard.Policy, cloud bool) error {
	cb := &ctxBody{ctx: res.Request.Context(), rc: res.Body, quit: make(chan struct{})}
	// only SSE chat completion streams can receive the injected error event; any other
	// response (non-stream JSON, /completion, /slots, ...) keeps the plain body
	if res.Body != nil && shouldInjectStreamError(res) {
		res.Body = &guardBody{inner: cb, ctx: res.Request.Context()}
	} else {
		res.Body = cb
	}
	// completion tap: parse the completion's observation out of the response and fan it
	// out to the consumers (outermost wrap, so an injected stream error event still
	// flows through the scanner); active whenever at least one consumer is registered,
	// or the error guard needs the tap to look for backend error payloads (the guard
	// alone does not turn on usage injection). A cloud response is tapped only when a
	// consumer accounts cloud traffic: without one, cloud traffic stays untouched
	tapped := p.observe || guard != nil
	if cloud {
		tapped = p.observeCloud
	}
	if tapped && res.Body != nil && res.Request.URL.Path == completionsPath {
		stream := strings.HasPrefix(res.Header.Get("Content-Type"), "text/event-stream")
		res.Body = &completionTap{
			inner:  res.Body,
			hub:    p.hub,
			guard:  guard,
			stream: stream,
			ctype:  res.Header.Get("Content-Type"),
			cenc:   res.Header.Get("Content-Encoding"),
			cloud:  cloud,
			id:     p.hub.StreamStart(stream, cloud),
		}
	}
	// backend response dump (no-op when debug.respSavePath is not set): the outermost
	// wrap, so it sees exactly what the client receives (including an injected stream
	// error event) and buffers it for the dump when the body is closed
	if p.debug != nil {
		p.debug.TapResponse(res)
	}
	cb.watch()
	return nil
}

// shouldInjectStreamError reports whether the response is an SSE chat completion stream
// that can receive the injected error event: the path must be the chat completion endpoint
// and the response must actually be a stream (stream:false responses are plain JSON, as are
// all the other endpoints)
func shouldInjectStreamError(res *http.Response) bool {
	if res.Request.URL.Path != completionsPath {
		return false
	}
	return strings.HasPrefix(res.Header.Get("Content-Type"), "text/event-stream")
}

// logAccess logs one access line: client, method, path, status code, elapsed time,
// and the backend the request went to (local, cloud, or "-" when it never reached one)
func logAccess(status int, target string, r *http.Request, start time.Time) {
	if status == 0 {
		status = http.StatusOK
	}
	log.Printf("[access] %s %s %s %d %s %s", r.RemoteAddr, r.Method, r.URL.RequestURI(), status, time.Since(start).Round(time.Microsecond), target)
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
