package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"os/exec"
	"os/signal"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

// Version information set during build via -ldflags (see Makefile)
var (
	Version   = "dev"
	BuildTime = "unknown"
)

var (
	cfgPath = flag.String("config", "config.yaml", "path to the config file")
	showVer = flag.Bool("version", false, "print version information and exit")
)

// printVersion prints the build version information
func printVersion() {
	fmt.Printf("llama-supervisor %s\n", Version)
	fmt.Printf("Build Time: %s\n", BuildTime)
	fmt.Printf("Go Version: %s\n", runtime.Version())
	fmt.Printf("Platform: %s/%s\n", runtime.GOOS, runtime.GOARCH)
}

// proxy is the reverse proxy with idle restart and probe logic attached
type proxy struct {
	proxy       *httputil.ReverseProxy
	backendURL  string
	backendAddr string // backend host:port (validated at startup, must carry an explicit port)

	restart  *restartPolicy  // restart policy (nil when restart is disabled)
	probe    *probePolicy    // probe policy (nil when probe is disabled)
	watchdog *watchdogPolicy // watchdog policy (nil when watchdog is disabled)
	request  *requestPolicy  // request policy (nil when no request sub-feature is enabled)
	debug    *debugPolicy    // debug policy (nil when debug is disabled)
}

// newBackendProxy creates the reverse proxy to the backend; ctx is the server-level ctx (probes do not follow user requests)
func newBackendProxy(cfg Config, ctx context.Context) *proxy {
	backend, err := url.Parse(cfg.Backend)
	if err != nil {
		log.Fatalf("backend %q: %v", cfg.Backend, err)
	}
	if backend.Hostname() == "" || backend.Port() == "" {
		log.Fatalf("backend %q: host and explicit port are required", cfg.Backend)
	}
	p := &proxy{
		backendURL:  cfg.Backend,
		backendAddr: backend.Host,
	}
	var rp *httputil.ReverseProxy
	if cfg.Request.Enabled() {
		// with a request policy use the Rewrite API (Director has no request hook):
		// SetURL routes to the backend, then the policy modifiers run on the outbound request
		p.request = newRequestPolicy(cfg.Request)
		rp = &httputil.ReverseProxy{
			Rewrite: func(pr *httputil.ProxyRequest) {
				pr.SetURL(backend)
				p.request.modifyRequest(pr.Out)
			},
		}
	} else {
		rp = httputil.NewSingleHostReverseProxy(backend)
	}
	// FlushInterval=0: flush after every Write so SSE/streaming output reaches the client unbuffered
	rp.FlushInterval = 0
	// When the client disconnects (request ctx canceled), close the backend response body proactively:
	// Go's HTTP/1.1 response body reads are not governed by the request ctx, otherwise the proxy
	// would stay blocked on the backend stream read and the backend would keep generating
	rp.ModifyResponse = func(res *http.Response) error {
		cb := &ctxBody{ctx: res.Request.Context(), rc: res.Body, quit: make(chan struct{})}
		// only SSE chat completion streams can receive the injected error event; any other
		// response (non-stream JSON, /completion, /slots, ...) keeps the plain body.
		// the client ResponseWriter was attached to the request context in ServeHTTP
		if res.Body != nil && shouldInjectStreamError(res) {
			var writer io.Writer
			if w, ok := res.Request.Context().Value(clientWriterKey).(http.ResponseWriter); ok {
				writer = w
			}
			res.Body = &guardBody{inner: cb, writer: writer, ctx: res.Request.Context()}
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
		p.restart = newRestartPolicy(cfg.Restart, restartInterval(cfg))
	}
	if cfg.Probe.Enabled() {
		p.probe = newProbePolicy(ctx, cfg.Backend, cfg.Probe, probeInterval(cfg), cfg.ApiKey)
	}
	if cfg.Watchdog.Enabled() {
		p.watchdog = newWatchdogPolicy(cfg.Watchdog, cfg.Backend, cfg.ApiKey)
	}
	if cfg.Debug.Enabled() {
		p.debug = newDebugPolicy(cfg.Debug)
	}
	if p.restart == nil && p.probe == nil && p.watchdog == nil && p.request == nil && p.debug == nil {
		log.Print("[config] restart, probe, watchdog, request and debug all disabled, proxying only")
	}
	return p
}

func (p *proxy) onHTTPRequest() {
	if p.restart != nil {
		p.restart.onHTTPRequest()
	}
	if p.probe != nil {
		p.probe.onHTTPRequest()
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
func (p *proxy) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	start := time.Now()
	rec := &statusRecorder{ResponseWriter: w}
	// probe idle timeout: probe first; if the command actually ran (backend restarted), wait for the backend to be ready.
	// restart timing needs no manual reset: this request's onHTTPRequest already refreshed it
	if p.probe != nil && p.probe.consumeIdle(ctx) {
		p.waitBackendReady(ctx)
	}
	// On client disconnect: the request ctx is canceled -> ctxBody closes the backend connection ->
	// the backend read is interrupted, and "client disconnected" is logged from ctxBody/ErrorHandler.
	// Note: r.Context().Err() must not be used here to detect this, because net/http also cancels
	// the ctx after the handler returns normally, which would be a false positive.
	// Attach rec to the request context so the proxy hooks can reach the client ResponseWriter
	// (ReverseProxy does not expose it through the response)
	p.proxy.ServeHTTP(rec, r.WithContext(context.WithValue(ctx, clientWriterKey, rec)))
	logAccess(rec.status, r, start)
}

// startBackground starts the background checkers:
// restart checks once per second and runs restart.command when the idle deadline is crossed, periodically repeatable;
// watchdog samples /slots at the configured interval and runs watchdog.command when the speed keeps exceeding the limit
func (p *proxy) startBackground(ctx context.Context) {
	if p.restart != nil {
		go func() {
			ticker := time.NewTicker(time.Second)
			defer ticker.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					p.restart.consumeIdle(ctx)
				}
			}
		}()
	}
	if p.watchdog != nil {
		go func() {
			ticker := time.NewTicker(p.watchdog.config.interval)
			defer ticker.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					p.watchdog.tick(ctx)
				}
			}
		}()
	}
}

// clientWriterKey is the context key under which the ResponseWriter serving the request is
// exposed to the proxy hooks (the guard uses it to inject the SSE error event into the stream)
type clientWriterKeyT int

const clientWriterKey clientWriterKeyT = 0

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
func (p *proxy) waitBackendReady(ctx context.Context) {
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

// runCommand runs a command and waits for it to finish; the command is terminated when ctx is canceled
func runCommand(ctx context.Context, label, cmdStr string) bool {
	if cmdStr == "" {
		return false
	}
	log.Printf("[%s] running: %s", label, cmdStr)
	c := exec.CommandContext(ctx, "sh", "-c", cmdStr)
	c.Stdout = os.Stdout
	c.Stderr = os.Stderr
	if err := c.Run(); err != nil {
		if ctx.Err() != nil {
			log.Printf("[%s] canceled: %v", label, ctx.Err())
		} else {
			log.Printf("[%s] failed: %v", label, err)
		}
		return false
	}
	log.Printf("[%s] completed", label)
	return true
}

func main() {
	flag.Parse()
	if *showVer {
		printVersion()
		return
	}
	cfg, err := loadConfig(*cfgPath)
	if err != nil {
		log.Fatalf("load config: %v", err)
	}

	// Root ctx: signal-driven, inherited globally (startup command, probes and request contexts all derive from it)
	ctx, stopSignal := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stopSignal()

	log.Printf("llama-supervisor %s (built %s, %s)", Version, BuildTime, runtime.Version())
	log.Print("[config] apiKey=" + secretMask(cfg.ApiKey))

	if cfg.Restart.Enabled() {
		log.Printf("[config] restart enabled: interval=%ds",
			int(restartInterval(cfg).Seconds()))
		log.Print("[config] restart command: " + cfg.Restart.Command)
	} else {
		log.Print("[config] restart disabled")
	}
	if cfg.Probe.Enabled() {
		pc := buildProbeConfig(cfg.Probe, cfg.ApiKey)
		log.Printf("[config] probe enabled: interval=%ds model=%q maxTokens=%d repeatLimit=%d successLimit=%d timeout=%ds",
			int(probeInterval(cfg).Seconds()), pc.model, pc.maxTokens, pc.repeatLimit, pc.successLimit, int(pc.timeout.Seconds()))
		log.Print("[config] probe command: " + cfg.Probe.Command)
	} else {
		log.Print("[config] probe disabled")
	}
	if cfg.Watchdog.Enabled() {
		wc := buildWatchdogConfig(cfg.Watchdog)
		log.Printf("[config] watchdog enabled: interval=%ds maxRate=%gt/s times=%d pause=%ds",
			int(wc.interval.Seconds()), wc.maxRate, wc.times, int(wc.pause.Seconds()))
		log.Print("[config] watchdog command: " + cfg.Watchdog.Command)
	} else {
		log.Print("[config] watchdog disabled")
	}
	if cfg.Request.Enabled() {
		var feats []string
		if cfg.Request.PrefixCache {
			feats = append(feats, "prefixCache")
		}
		log.Printf("[config] request enabled: %s", strings.Join(feats, ", "))
	} else {
		log.Print("[config] request disabled")
	}
	if cfg.Debug.Enabled() {
		d := cfg.Debug
		if d.Path == "" {
			d.Path = defaultDebugPath
		}
		log.Printf("[config] debug enabled: path=%s", d.Path)
		log.Print("[config] debug command: " + d.Command)
	} else {
		log.Print("[config] debug disabled")
	}

	// startup command
	if cfg.StartupCommand != "" {
		runCommand(ctx, "startup", cfg.StartupCommand)
	}

	// create the proxy and start the restart background idle checker
	sup := newBackendProxy(cfg, ctx)
	sup.startBackground(ctx)

	ln, err := net.Listen("tcp", net.JoinHostPort(cfg.Host, strconv.Itoa(cfg.Port)))
	if err != nil {
		log.Fatalf("listen %s:%d failed: %v", cfg.Host, cfg.Port, err)
	}
	log.Printf("supervisor listening on http://%s:%d -> %s", cfg.Host, cfg.Port, cfg.Backend)

	srv := &http.Server{
		// request contexts inherit the root ctx
		BaseContext: func(net.Listener) context.Context { return ctx },
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// the debug endpoint is served by the supervisor itself and never proxied
			if sup.debug != nil && sup.debug.handle(w, r) {
				return
			}
			sup.onHTTPRequest()
			sup.ServeHTTP(w, r)
		}),
	}

	// close the server on exit signal
	go func() {
		<-ctx.Done()
		log.Print("received exit signal, shutting down")
		_ = srv.Close()
	}()

	_ = srv.Serve(ln)
}
