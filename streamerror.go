package main

import (
	"context"
	"errors"
	"io"
	"log"
	"net/http"
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
// ends abnormally mid-stream (Read error) and the client is still connected, it injects
// the SSE error event into the client stream before it is closed. A normal stream end
// (EOF, after the backend sent [DONE]) never triggers the injection
type guardBody struct {
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

// maybeInject writes the error event to the client stream when the stream ends
// abnormally and the client is still connected
func (b *guardBody) maybeInject() {
	if b.writer == nil {
		return
	}
	if b.ctx != nil && b.ctx.Err() != nil {
		return // client disconnected: nothing to inject into
	}
	if _, werr := io.WriteString(b.writer, streamErrorEvent); werr != nil {
		log.Printf("[streamguard] inject error event failed: %v", werr)
		return
	}
	// flush so the event reaches the client immediately, without waiting for the handler to return
	if f, ok := b.writer.(http.Flusher); ok {
		f.Flush()
	}
	log.Print("[streamguard] stream interrupted, error event sent to client")
}

// Close forwards to the inner body; the http client closes the backend TCP connection
// when the body is closed before full read, so the backend stops processing early
func (b *guardBody) Close() error { return b.inner.Close() }
