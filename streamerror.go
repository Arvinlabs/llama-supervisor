package main

import (
	"context"
	"errors"
	"io"
	"log"
	"net/http"
	"sync"
	"time"
)

// streamErrorEvent is the SSE error event injected into streams that are interrupted
// by a backend restart. `event: error` is recognized by OpenAI-compatible SSE clients;
// clients that only parse `data:` lines still see a distinct error object and never
// receive the usual `[DONE]` marker, so the interruption is not indistinguishable
// from a normal completion.
//
// The leading extra blank line seals off a possibly truncated final `data:` line left
// when the backend is killed mid-chunk: per the SSE spec it terminates that line into
// its own (malformed) message event, keeping this error event's data field clean JSON.
// An empty event is ignored by spec-compliant parsers.
const streamErrorEvent = "\n\nevent: error\ndata: {\"error\":{\"message\":\"generation interrupted: backend restarted\",\"type\":\"server_error\",\"code\":\"backend_restarted\"}}\n\n"

// streamErrorGrace is how long the guard stays armed after a restart command starts:
// the interrupted streams surface their copy error right after the backend stops
// (the first step of the restart command), so the window only needs to outlive the
// stop plus a margin
const streamErrorGrace = 5 * time.Second

// streamGuard marks the window in which in-flight streams are being killed by a
// supervisor-initiated backend restart (watchdog trigger, idle restart or probe
// command). During the window a stream whose backend connection ends abnormally
// gets an SSE error event injected before it closes, so the client can tell the
// interruption apart from a normal stream end
type streamGuard struct {
	mu        sync.Mutex
	armedFlag bool
}

// newStreamGuard creates the guard; one is shared by all policies and the proxy
func newStreamGuard() *streamGuard { return &streamGuard{} }

// arm opens the window; it closes automatically after the grace period
func (g *streamGuard) arm() {
	g.mu.Lock()
	g.armedFlag = true
	g.mu.Unlock()
	time.AfterFunc(streamErrorGrace, g.disarm)
}

// disarm closes the window
func (g *streamGuard) disarm() {
	g.mu.Lock()
	g.armedFlag = false
	g.mu.Unlock()
}

// armed reports whether the window is open
func (g *streamGuard) armed() bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.armedFlag
}

// armRestart arms the guard before running a backend-restarting command (no-op when g is nil)
func armRestart(g *streamGuard) {
	if g != nil {
		g.arm()
	}
}

// guardBody wraps the streaming backend response body. When the backend connection
// ends abnormally mid-stream (Read error) and the guard window is open (a supervisor
// restart is running and its grace period has not expired), it injects the SSE error
// event into the client stream before it is closed. A normal stream end (EOF, after
// the backend sent [DONE]) never triggers the injection
type guardBody struct {
	guard  *streamGuard
	inner  io.ReadCloser
	writer io.Writer
	ctx    context.Context // the request context; a canceled ctx means the client already left
	once   sync.Once
}

func (b *guardBody) Read(p []byte) (int, error) {
	n, err := b.inner.Read(p)
	if err != nil && !errors.Is(err, io.EOF) {
		b.once.Do(b.maybeInject)
	}
	return n, err
}

// maybeInject writes the error event to the client stream when the guard window is open
func (b *guardBody) maybeInject() {
	if b.writer == nil || !b.guard.armed() {
		return
	}
	if b.ctx != nil && b.ctx.Err() != nil {
		return // client disconnected: the aborted stream is not a backend restart
	}
	if _, werr := io.WriteString(b.writer, streamErrorEvent); werr != nil {
		log.Printf("[streamguard] inject error event failed: %v", werr)
		return
	}
	// flush so the event reaches the client immediately, without waiting for the handler to return
	if f, ok := b.writer.(http.Flusher); ok {
		f.Flush()
	}
	log.Print("[streamguard] stream interrupted by backend restart, error event sent to client")
}

// Close forwards to the inner body; the http client closes the backend TCP connection
// when the body is closed before full read, so the backend stops processing early
func (b *guardBody) Close() error { return b.inner.Close() }
