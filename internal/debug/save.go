package debug

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"time"
)

// Tap dumps the inbound request to a plain text file named by the request time
// (YYYYMMDD_HHMMSS.mmm.txt) under the configured SavePath, for later inspection and
// replay. It is a no-op when SavePath is not set. The inbound dump is generated from
// exactly what the client sent, untouched by the request policy.
func (p *Policy) Tap(r *http.Request) {
	p.saveRequest(r, p.savePath, "inbound")
}

// TapOutbound dumps the outbound request (after the request policy has rewritten it, when
// enabled) to a plain text file named by the request time under the configured OutSavePath.
// It is a no-op when OutSavePath is not set.
func (p *Policy) TapOutbound(r *http.Request) {
	p.saveRequest(r, p.outSavePath, "outbound")
}

// saveRequest implements Tap and TapOutbound: the dump is written as the request is served:
// the request line and all headers are emitted immediately, then the body is buffered and
// written to the file when the body is closed (the proxy closes it after forwarding). If the
// body is valid JSON it is stored pretty-printed (2-space indent) for readability, otherwise
// base64-encoded so the dump stays a plain text file. Failures are logged but never affect
// the caller.
func (p *Policy) saveRequest(r *http.Request, dir, tag string) {
	if dir == "" {
		return
	}
	name := time.Now().Format("20060102_150405.000") + ".txt"
	if err := os.MkdirAll(dir, 0o755); err != nil {
		log.Printf("[debug] save %s request: mkdir %s: %v", tag, dir, err)
		return
	}
	f, err := os.Create(filepath.Join(dir, name))
	if err != nil {
		log.Printf("[debug] save %s request: create %s: %v", tag, name, err)
		return
	}
	// the request line and all headers are written directly (r.Write cannot be used
	// here: it consumes and closes the body, which the proxy still needs); the body
	// itself is buffered and written by the tee when the proxy closes it
	if _, err := fmt.Fprintf(f, "%s %s HTTP/1.1\r\n", r.Method, r.URL.RequestURI()); err != nil {
		log.Printf("[debug] save %s request: write %s: %v", tag, name, err)
		_ = f.Close()
		return
	}
	if r.Host != "" {
		_, _ = fmt.Fprintf(f, "Host: %s\r\n", r.Host)
	} else if r.URL != nil && r.URL.Host != "" {
		// outbound requests (debug.outSavePath) carry the backend address in URL.Host
		// while Request.Host is empty
		_, _ = fmt.Fprintf(f, "Host: %s\r\n", r.URL.Host)
	}
	if err := r.Header.Write(f); err != nil {
		log.Printf("[debug] save %s request: write headers of %s: %v", tag, name, err)
		_ = f.Close()
		return
	}
	if _, err := f.WriteString("\r\n"); err != nil {
		log.Printf("[debug] save %s request: write %s: %v", tag, name, err)
		_ = f.Close()
		return
	}
	// one log line per saved request, at the moment the dump is complete: here for
	// body-less requests, in the tee's Close for requests with a body
	path := filepath.Join(dir, name)
	if r.Body == nil {
		_ = f.Close()
		log.Printf("[debug] saved %s request to %s", tag, path)
	} else {
		r.Body = &teeBody{body: r.Body, f: f, path: path, tag: tag}
	}
}

// teeBody accumulates the wrapped body and, when it is closed, writes it to the dump
// file - pretty-printed if it is valid JSON, base64-encoded otherwise; an early close
// on a canceled request leaves a partial base64 dump, which is expected. Buffering the
// body is acceptable here: saving is an opt-in debug feature
type teeBody struct {
	body   io.ReadCloser
	f      *os.File
	path   string // full path of the dump file, used in the log line
	tag    string // "inbound" or "outbound", used in the log line
	buf    bytes.Buffer
	closed bool // guard: the proxy may close the body more than once
}

func (b *teeBody) Read(p []byte) (int, error) {
	n, err := b.body.Read(p)
	if n > 0 {
		_, _ = b.buf.Write(p[:n])
	}
	return n, err
}

func (b *teeBody) Close() error {
	if b.closed {
		return nil
	}
	b.closed = true
	// one log line per saved request: success or the failure that stopped it
	if err := b.dump(); err != nil {
		log.Printf("[debug] save %s request: write body to %s: %v", b.tag, b.path, err)
	} else {
		log.Printf("[debug] saved %s request to %s", b.tag, b.path)
	}
	err := b.body.Close()
	if cerr := b.f.Close(); err == nil {
		err = cerr
	}
	return err
}

// dump writes the buffered body to the file: pretty-printed if it is valid JSON,
// base64-encoded otherwise (binary, form, multipart, ...) so the dump always stays a
// plain text file
func (b *teeBody) dump() error {
	data := b.buf.Bytes()
	if len(data) == 0 {
		return nil
	}
	if json.Valid(data) {
		var out bytes.Buffer
		if err := json.Indent(&out, data, "", "  "); err == nil {
			data = out.Bytes()
		}
	} else {
		data = []byte(base64.StdEncoding.EncodeToString(data))
	}
	_, err := b.f.Write(data)
	return err
}
