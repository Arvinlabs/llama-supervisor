package proxy

import (
	"context"
	"errors"
	"io"
	"log"
	"sync"
)

// streamErrorEvent is the SSE error event injected into streams that end abnormally
// (backend killed, connection reset, etc.). Clients see a distinct error object in
// the `data:` field and never receive the usual `[DONE]` marker, so the interruption
// is not indistinguishable from a normal completion.
//
// The leading extra blank line seals off a possibly truncated final `data:` line left
// when the backend is killed mid-chunk: per the SSE spec it terminates that line into
// its own (malformed) message event, keeping this error event's data field clean JSON.
// An empty event is ignored by spec-compliant parsers.
const streamErrorEvent = "\n\ndata: {\"error\":{\"message\":\"stream interrupted\",\"type\":\"server_error\",\"code\":\"stream_interrupted\"}}\n\n"

// guardBody wraps the streaming backend response body. When the backend connection
// ends abnormally mid-stream (Read error) and the client is still connected, it serves
// the SSE error event as the data of the following Reads, so the ReverseProxy copies
// it into the client stream through its own (synchronized) write path, and only then
// propagates the original error. A normal stream end (EOF, after the backend sent
// [DONE]) never triggers the injection.
//
// The event must NOT be written straight to the client ResponseWriter from Read: that
// writer is concurrently flushed by the proxy's maxLatencyWriter timer goroutine
// (SSE streams flush immediately, so the flush timer is always armed), and a direct
// write would race with it (detected by -race).
type guardBody struct {
	inner   io.ReadCloser
	ctx     context.Context // the request context; a canceled ctx means the client already left
	once    sync.Once
	pending []byte // error event bytes not yet handed out to the reader
	failed  error  // the first non-EOF read error; returned once pending is drained
}

func (b *guardBody) Read(p []byte) (int, error) {
	if len(b.pending) > 0 {
		n := copy(p, b.pending)
		b.pending = b.pending[n:]
		return n, nil
	}
	// the stream is already failed: the event was served, now end the stream with the
	// remembered error (the inner read error must not be swallowed, so the failure is
	// propagated deterministically even if the underlying body would recover)
	if b.failed != nil {
		return 0, b.failed
	}
	n, err := b.inner.Read(p)
	if err != nil && !errors.Is(err, io.EOF) {
		b.once.Do(func() {
			b.failed = err
			if b.ctx != nil && b.ctx.Err() != nil {
				return // client disconnected: nothing to inject into
			}
			log.Print("[streamguard] stream interrupted, injecting error event")
			b.pending = []byte(streamErrorEvent)
		})
		if len(b.pending) > 0 {
			// park the error in failed for now: the event is served on the following
			// reads, after which failed is returned and the proxy aborts the stream
			return n, nil
		}
	}
	return n, err
}

// Close forwards to the inner body; the http client closes the backend TCP connection
// when the body is closed before full read, so the backend stops processing early
func (b *guardBody) Close() error { return b.inner.Close() }
