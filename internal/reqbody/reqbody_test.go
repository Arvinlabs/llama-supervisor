package reqbody

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
)

// failingBody serves data and then fails: it simulates a body read that breaks
// part-way (a truncated upload, a reset connection)
type failingBody struct {
	data []byte
	err  error
}

func (b *failingBody) Read(p []byte) (int, error) {
	if len(b.data) > 0 {
		n := copy(p, b.data)
		b.data = b.data[n:]
		return n, nil
	}
	return 0, b.err
}

func (b *failingBody) Close() error { return nil }

// a successful read returns the whole body and the caller can reinstall a
// rewritten one
func TestReadAndRestore(t *testing.T) {
	r := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", bytes.NewReader([]byte(`{"a":1}`)))
	r.Header.Set("Content-Length", "7")
	body, ok := Read(r)
	if !ok || string(body) != `{"a":1}` {
		t.Fatalf("Read = %q, %v", body, ok)
	}
	Restore(r, []byte(`{"a":2}`))
	got, err := io.ReadAll(r.Body)
	if err != nil || string(got) != `{"a":2}` {
		t.Fatalf("restored body = %q, %v", got, err)
	}
	if r.ContentLength != 7 || r.Header.Get("Content-Length") != "7" {
		t.Fatalf("ContentLength = %d, header = %q", r.ContentLength, r.Header.Get("Content-Length"))
	}
	if err := r.Body.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
}

// a failed read must not swallow the body: the bytes read so far are replayed
// and the unread remainder is still available, and the caller is told to leave
// the body alone
func TestReadFailureReplaysTheBody(t *testing.T) {
	r := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil)
	r.Body = &failingBody{data: []byte("hello "), err: io.ErrUnexpectedEOF}
	r.ContentLength = 11
	body, ok := Read(r)
	if ok {
		t.Fatal("a failed read must report ok=false")
	}
	if string(body) != "hello " {
		t.Fatalf("partial body = %q, want %q", body, "hello ")
	}
	// the reinstalled body still replays the whole client body (and still fails
	// where the original reader failed)
	got, err := io.ReadAll(r.Body)
	if string(got) != "hello " || err != io.ErrUnexpectedEOF {
		t.Fatalf("replayed body = %q, %v", got, err)
	}
	if r.ContentLength != 11 {
		t.Fatalf("ContentLength = %d, want the original 11", r.ContentLength)
	}
}

// Replay serves an already-read prefix followed by the rest of the original body
func TestReplay(t *testing.T) {
	rest := &failingBody{data: []byte("world"), err: io.EOF}
	r := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil)
	r.Body = rest
	Replay(r, []byte("hello "), rest)
	got, err := io.ReadAll(r.Body)
	if string(got) != "hello world" || err != nil {
		t.Fatalf("replayed body = %q, %v", got, err)
	}
	_ = r.Body.Close()
}
