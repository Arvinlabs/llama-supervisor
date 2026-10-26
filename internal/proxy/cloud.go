package proxy

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log"
	"mime"
	"net"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"

	"github.com/Arvinlabs/llama-supervisor/internal/config"
	"github.com/Arvinlabs/llama-supervisor/internal/localctx"
	"github.com/Arvinlabs/llama-supervisor/internal/reqbody"
	"github.com/Arvinlabs/llama-supervisor/internal/tokenest"
)

// maxCloudRouteBody bodies larger than this are never routed (kept local):
// they already far exceed any local context and buffering them buys nothing.
// It is a variable only so the tests can exercise the boundary with a small body
var maxCloudRouteBody = 32 << 20

// llamaCppOnlyParams are request fields specific to the llama.cpp server that
// OpenAI-compatible cloud endpoints may reject with a 400; they are stripped
// from bodies routed to the cloud. All of them are optional sampling parameters
// - removing them changes the generation distribution on the cloud, never the
// correctness of the request. Standard OpenAI fields (logit_bias, seed, ...) are
// deliberately kept: the cloud endpoint is expected to honor them
var llamaCppOnlyParams = []string{
	"cache_prompt", "penalty_last_n", "min_p", "typical_p", "repeat_penalty",
	"mirostat", "mirostat_tau", "mirostat_eta", "n_keep", "top_k", "penalty_alpha",
}

// cloudTransportTimeouts are the cloud HTTP transport's connection-level
// timeouts: a black-holed cloud endpoint then fails (and can be failed over)
// instead of hanging the request forever. The body transfer itself is not
// time-limited (a long generation may legitimately take minutes)
const (
	cloudDialTimeout           = 10 * time.Second
	cloudTLSHandshakeTimeout   = 10 * time.Second
	cloudResponseHeaderTimeout = 2 * time.Minute
	cloudIdleConnTimeout       = 90 * time.Second
)

// newCloudTransport builds the HTTP transport for the cloud endpoint: a clone of
// the default transport with explicit dial/TLS/response-header timeouts
func newCloudTransport() *http.Transport {
	t := http.DefaultTransport.(*http.Transport).Clone()
	t.DialContext = (&net.Dialer{Timeout: cloudDialTimeout, KeepAlive: 30 * time.Second}).DialContext
	t.TLSHandshakeTimeout = cloudTLSHandshakeTimeout
	t.ResponseHeaderTimeout = cloudResponseHeaderTimeout
	t.IdleConnTimeout = cloudIdleConnTimeout
	return t
}

// cloudRouter decides per request whether a chat completion goes to the cloud:
// it buffers the request body (always reinstalled, so whichever proxy is
// picked can read it), estimates the prompt size with the tokenest estimator,
// and routes to the cloud when the estimate plus the reserved output does not
// fit in the local context window (localctx) times the threshold
type cloudRouter struct {
	cfg           config.CloudGroup
	threshold     float64
	outputReserve int
	localCtx      *localctx.Provider
	estimator     *tokenest.Estimator
	sem           chan struct{} // cloud concurrency slots; nil means unlimited
	warnThrottle  atomic.Int64  // seconds of the last "context window unknown" warning (rate limit)
}

func newCloudRouter(cfg config.Config, backend string) *cloudRouter {
	cr := &cloudRouter{
		cfg:           *cfg.Cloud,
		threshold:     config.CloudThreshold(cfg),
		outputReserve: config.CloudOutputReserve(cfg),
		localCtx:      localctx.New(backend, cfg.ApiKey, cfg.Cloud.LocalMaxTokens),
		estimator:     tokenest.New(backend, cfg.ApiKey),
	}
	if n := cfg.Cloud.MaxConcurrency; n > 0 {
		cr.sem = make(chan struct{}, n)
	}
	return cr
}

// acquire waits for a free cloud concurrency slot, reporting false when the
// client went away (ctx canceled) while waiting; it is a no-op when the limit is
// not configured (cloud.maxConcurrency 0)
func (cr *cloudRouter) acquire(ctx context.Context) bool {
	if cr.sem == nil {
		return true
	}
	select {
	case cr.sem <- struct{}{}:
		return true
	case <-ctx.Done():
		return false
	}
}

// release frees a cloud concurrency slot acquired by acquire
func (cr *cloudRouter) release() {
	if cr.sem != nil {
		<-cr.sem
	}
}

// Route buffers the request body and reports whether the request should be
// routed to the cloud. It returns no routing for every request that is not a
// JSON POST /v1/chat/completions or that cannot be judged (oversized body,
// estimation or context window lookup failed): those go to the local backend,
// the status quo. The body is reinstalled in all cases
func (cr *cloudRouter) Route(ctx context.Context, r *http.Request) bool {
	if r.Method != http.MethodPost || r.URL.Path != completionsPath {
		return false
	}
	mediaType, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil || mediaType != "application/json" {
		return false
	}
	orig := r.Body
	body, err := io.ReadAll(io.LimitReader(orig, int64(maxCloudRouteBody)+1))
	if err != nil || len(body) > maxCloudRouteBody {
		// not judgeable: keep the request local with its body fully intact. The
		// oversized prefix was buffered, the unread remainder is still in the
		// original reader, so replaying both reproduces the client's body exactly
		// (Content-Length is untouched, so the backend reads the whole request)
		reqbody.Replay(r, body, orig)
		return false
	}
	_ = orig.Close()
	defer restoreBody(r, body)
	// the size comes from the local backend's tokenizer and nowhere else: when it
	// cannot be obtained the request is not judged and stays local
	n, estErr := cr.estimator.Estimate(ctx, body)
	if estErr != nil {
		log.Printf("[cloud] prompt estimation failed (%v), keeping the request local: %s", estErr, r.URL.Path)
		return false
	}
	nctx, ctxErr := cr.localCtx.ContextWindow(ctx)
	if ctxErr != nil || nctx <= 0 {
		if cr.warnAllowed() {
			log.Printf("[cloud] local context window unknown (%v), keeping the request local", ctxErr)
		}
		return false
	}
	reserve := cr.outputReserve
	// the client's own output cap dominates the decision when it is larger than the default reserve
	for _, field := range []string{"max_tokens", "max_completion_tokens"} {
		if mt := int(gjson.GetBytes(body, field).Int()); mt > reserve {
			reserve = mt
		}
	}
	limit := int(float64(nctx) * cr.threshold)
	route := n+reserve > limit
	log.Printf("[cloud] est=%d (reserve %d) limit=%d (n_ctx=%d) -> %s",
		n+reserve, reserve, limit, nctx, targetName(route))
	return route
}

// warnAllowed reports whether the rate-limited warning may be logged now (at
// most one per 30s, so a permanently unknown context window does not flood the log)
func (cr *cloudRouter) warnAllowed() bool {
	now := time.Now().Unix()
	last := cr.warnThrottle.Load()
	if now-last < 30 {
		return false
	}
	return cr.warnThrottle.CompareAndSwap(last, now)
}

// restoreBody reinstalls a (possibly replaced) request body, updating
// ContentLength (the old reader was drained, so even the unchanged case must be
// reinstalled or the proxy would forward an empty body)
func restoreBody(r *http.Request, body []byte) {
	reqbody.Restore(r, body)
}

// setAuth overrides the outbound Authorization header with the cloud key; it
// runs after the request policy's virtual-key rewrite, so client keys (virtual
// or real) never reach the cloud
func (cr *cloudRouter) setAuth(r *http.Request) {
	r.Header.Set("Authorization", "Bearer "+cr.cfg.APIKey)
}

// rewriteModel rewrites the model field to the configured cloud model name
// (no-op when the name is unset); everything else passes through untouched
func (cr *cloudRouter) rewriteModel(r *http.Request) {
	if cr.cfg.Model == "" || r.Method != http.MethodPost || r.URL.Path != completionsPath {
		return
	}
	body, ok := reqbody.Read(r)
	if !ok {
		return // the body was reinstalled untouched: never forward it empty
	}
	out := body
	if len(bytes.TrimSpace(body)) > 0 {
		if updated, serr := sjson.SetBytes(body, "model", cr.cfg.Model); serr == nil {
			out = updated
		}
	}
	restoreBody(r, out)
}

// sanitizeCloudBody strips the llama.cpp-specific fields (llamaCppOnlyParams)
// from the request body before it is sent to the cloud; a body carrying none
// of them passes through byte-identical
func sanitizeCloudBody(r *http.Request) {
	if r.Method != http.MethodPost || r.URL.Path != completionsPath {
		return
	}
	body, ok := reqbody.Read(r)
	if !ok {
		return // the body was reinstalled untouched: never forward it empty
	}
	for _, f := range llamaCppOnlyParams {
		if gjson.GetBytes(body, f).Exists() {
			if updated, derr := sjson.DeleteBytes(body, f); derr == nil {
				body = updated
			}
		}
	}
	restoreBody(r, body)
}

// targetName is the access-log tag of the backend a request went to
func targetName(cloud bool) string {
	if cloud {
		return "cloud"
	}
	return "local"
}

// cloudErrorHandler handles the transport-level failures of the cloud proxy
// (connect/timeout/reset before any response): the request is never retried
// anywhere, the client gets a 502 with an OpenAI-format error object. A response
// the cloud actually sent (any status) is always passed through to the client
func (p *Supervisor) cloudErrorHandler(w http.ResponseWriter, r *http.Request, err error) {
	if cerr := r.Context().Err(); cerr != nil {
		// the client already disconnected: the cloud request was aborted early, not a cloud failure
		log.Printf("[cloud] client disconnected, cloud aborted: %s %s (%v)", r.Method, r.URL.Path, err)
		writeCloudError(w, http.StatusBadGateway)
		return
	}
	log.Printf("[cloud] %s %s: %v", r.Method, r.URL.Path, err)
	writeCloudError(w, http.StatusBadGateway)
}

// writeCloudError replies with an OpenAI-format error object: clients are
// OpenAI-compatible (SDKs expect {"error":{...}} on failures, not a plain-text
// body), and the shape matches the request policy's own 401 rejection
func writeCloudError(w http.ResponseWriter, status int) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"error": map[string]any{
			"message": "cloud unavailable",
			"type":    "server_error",
			"code":    "cloud_unavailable",
		},
	})
}
