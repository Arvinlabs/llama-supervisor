// Package lazy implements the lazy backend lifecycle: the backend is started
// asynchronously by the first request that finds it down (the request itself is
// never blocked for the model load - it goes to the cloud fallback or a 502),
// and it is stopped when it has been idle for the configured time
package lazy

import (
	"context"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/Arvinlabs/llama-supervisor/internal/command"
	"github.com/Arvinlabs/llama-supervisor/internal/config"
	"github.com/Arvinlabs/llama-supervisor/internal/idle"
)

// readyWaitTimeout bounds the readiness wait after a start: a very slow model
// load just defers the health settle to the regular poller
const readyWaitTimeout = 10 * time.Minute

// Policy manages the backend lifecycle around traffic: an asynchronous start
// when the backend is down, and a stop when it has been idle for interval
type Policy struct {
	// ctx is the server-level ctx (driven by the exit signal); the start/stop
	// commands and the readiness wait do not follow any user request ctx, so a
	// user disconnect cannot cancel them
	ctx      context.Context
	tracker  *idle.Tracker // lazy idle tracker: timing starts only after the first local request
	backend  string        // backend base URL (for the readiness wait)
	startCmd string
	stopCmd  string
	up       func() bool // current backend liveness verdict (the supervisor health tracker)
	set      func(bool)  // force the liveness state once it is known (after a start/stop)

	mu       sync.Mutex
	starting bool // a start command is in flight: concurrent requests do not stack start commands
}

// New creates the policy; interval is the idle-stop threshold, up/set are the
// supervisor's health state accessors
func New(ctx context.Context, backend string, g *config.LazyGroup, interval time.Duration, up func() bool, set func(bool)) *Policy {
	l := &Policy{
		ctx:      ctx,
		backend:  backend,
		startCmd: g.StartCommand,
		stopCmd:  g.StopCommand,
		up:       up,
		set:      set,
	}
	l.tracker = idle.NewLazy(interval, func(ctx context.Context) bool {
		return l.stopBackend()
	})
	return l
}

// OnHTTPRequest re-arms or extends the idle-stop window; it is called for the
// requests that actually use the local backend (never for a cloud-routed one)
func (l *Policy) OnHTTPRequest() {
	l.tracker.OnHTTPRequest()
}

// ConsumeIdle is called by the background ticker (once per second): when the
// idle-stop window has been crossed it runs the stop command and reports whether
// it did. Timing starts only after the first request and pauses after a
// trigger, so the stop never happens without prior traffic
func (l *Policy) ConsumeIdle() bool {
	return l.tracker.ConsumeIdle(l.ctx)
}

// StartAsync starts the backend asynchronously when the liveness tracker
// reports it down; it is called from the request path and returns immediately.
// The current request must not wait for the (possibly long) model load - it is
// routed to the cloud fallback or 502 as usual, and the requests after the
// backend is up go local again. Concurrent calls stack only one start command:
// the flag is held only while the command itself is running (not during the
// readiness wait, so a request after the command finished can always retry)
func (l *Policy) StartAsync() bool {
	l.mu.Lock()
	if l.up() || l.starting {
		l.mu.Unlock()
		return false
	}
	l.starting = true
	l.mu.Unlock()
	go l.startBackend()
	return true
}

// stopBackend runs the stop command (from the background ticker, off the
// request path) and marks the backend down as soon as it has run
func (l *Policy) stopBackend() bool {
	log.Printf("[lazy] idle timeout crossed, stopping the backend")
	if !command.RunCommand(l.ctx, "lazy-stop", l.stopCmd) {
		return false
	}
	l.set(false)
	log.Print("[lazy] backend stopped")
	return true
}

// startBackend runs the start command and then waits (bounded) for the backend
// to be truly ready, marking the health tracker up as soon as it is - so the
// routing decision does not have to wait for a full health-poll interval. The
// in-flight flag is released before the wait, so once the command finished the
// next request that still finds the backend down can retry
func (l *Policy) startBackend() {
	log.Print("[lazy] backend down, starting it asynchronously (the current requests are not waiting for it)")
	success := command.RunCommand(l.ctx, "lazy-start", l.startCmd)
	// release the flag before the readiness wait: once the command finished, the next
	// request that still finds the backend down must be able to retry
	l.mu.Lock()
	l.starting = false
	l.mu.Unlock()
	if !success {
		log.Print("[lazy] start command did not succeed; a later request will retry")
		return
	}
	l.waitReady()
}

// waitReady polls the backend /health every 0.5s until the service is truly
// ready (2xx), then marks the tracker up; bounded by readTimeout, so a very
// slow model load just defers the settle to the regular health poller
func (l *Policy) waitReady() {
	ctx, cancel := context.WithTimeout(l.ctx, readyWaitTimeout)
	defer cancel()
	log.Printf("[lazy] waiting for backend %s to be ready", l.backend)
	client := &http.Client{Timeout: 5 * time.Second}
	for {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, l.backend+"/health", nil)
		ok := false
		if err == nil {
			resp, err := client.Do(req)
			if err == nil {
				resp.Body.Close()
				ok = resp.StatusCode >= http.StatusOK && resp.StatusCode < http.StatusMultipleChoices
			}
		}
		if ok {
			log.Print("[lazy] backend ready")
			l.set(true)
			return
		}
		select {
		case <-time.After(500 * time.Millisecond):
		case <-ctx.Done():
			log.Printf("[lazy] readiness wait ended (%v); the health poller will settle the state", ctx.Err())
			return
		}
	}
}
