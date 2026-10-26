// Package reqbody reads and rewrites HTTP request bodies without ever losing
// them. A request body is a one-shot reader: a rewrite that reads only a prefix
// (oversized body) or fails part-way must reinstall a body that replays exactly
// what the client sent, or the proxy would forward an empty or truncated
// request to the backend.
package reqbody

import (
	"bytes"
	"io"
	"net/http"
	"strconv"
)

// Read drains r.Body and returns its bytes. On a read failure it reinstalls the
// bytes read so far followed by the unread remainder (the reader is not closed,
// so nothing is lost) and reports ok=false: the caller must then leave the body
// alone. On success r.Body is closed and the caller installs the (possibly
// rewritten) bytes with Restore.
func Read(r *http.Request) (body []byte, ok bool) {
	orig := r.Body
	body, err := io.ReadAll(orig)
	if err != nil {
		Replay(r, body, orig)
		return body, false
	}
	_ = orig.Close()
	return body, true
}

// Replay installs a body that serves head and then continues with rest. The
// ContentLength and Content-Length header are left untouched: the replayed body
// is the whole client body, so the declared length still holds.
func Replay(r *http.Request, head []byte, rest io.ReadCloser) {
	r.Body = &replayBody{Reader: io.MultiReader(bytes.NewReader(head), rest), closer: rest}
}

// Restore installs body as the whole request body, updating ContentLength and
// the Content-Length header. The previous reader was drained and closed, so even
// an unchanged body must be reinstalled.
func Restore(r *http.Request, body []byte) {
	r.Body = io.NopCloser(bytes.NewReader(body))
	r.ContentLength = int64(len(body))
	r.Header.Set("Content-Length", strconv.Itoa(len(body)))
}

// replayBody serves an already-read prefix and then the original reader
type replayBody struct {
	io.Reader
	closer io.Closer
}

func (b *replayBody) Close() error { return b.closer.Close() }
