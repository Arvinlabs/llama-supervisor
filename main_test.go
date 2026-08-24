package main

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// when the client disconnects mid-stream, the proxied request context toward the backend
// must be canceled (abort the backend generation early instead of letting it run to completion)
func TestProxyClientDisconnectCancelsBackend(t *testing.T) {
	backendErr := make(chan error, 1)
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		go func() {
			<-r.Context().Done()
			backendErr <- r.Context().Err()
		}()
		fl := w.(http.Flusher)
		for i := 0; i < 200; i++ { // long streaming response, simulating continuous generation
			w.Write([]byte("data: x\n\n"))
			fl.Flush()
			select {
			case <-r.Context().Done():
				return
			case <-time.After(50 * time.Millisecond):
			}
		}
	}))
	defer backend.Close()

	sup := newBackendProxy(Config{Backend: backend.URL}, context.Background())
	srv := httptest.NewServer(sup)
	defer srv.Close()

	// client: read the first batch of streaming data, then disconnect
	conn, err := net.Dial("tcp", srv.Listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	fmt.Fprintf(conn, "GET / HTTP/1.1\r\nHost: x\r\n\r\n")
	buf := make([]byte, 256)
	if err := conn.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, err := io.ReadFull(conn, buf); err != nil {
		t.Fatalf("read stream: %v", err)
	}
	conn.Close()

	select {
	case err := <-backendErr:
		if err != context.Canceled {
			t.Fatalf("backend ctx err = %v, want context.Canceled", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("backend request not canceled after client disconnect")
	}
}

// when the client disconnects while the proxy is blocked reading the backend stream
// (the backend stays silent for a long time, e.g. the thinking phase):
// ctxBody must proactively close the backend connection, the proxy handler returns immediately,
// and the backend request is aborted
func TestProxyClientDisconnectAbortsBlockedBackend(t *testing.T) {
	backendDone := make(chan struct{})
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("partial")) // send only the first chunk, then stay silent for a long time (simulating thinking/slow generation)
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
		<-r.Context().Done() // wait for the proxy to disconnect the connection early
		close(backendDone)
	}))
	defer backend.Close()

	sup := newBackendProxy(Config{Backend: backend.URL}, context.Background())
	proxyDone := make(chan struct{})
	ctx, ctxCancel := context.WithCancel(context.Background())
	defer ctxCancel()
	req := httptest.NewRequest(http.MethodGet, "/stream", nil).WithContext(ctx)
	w := httptest.NewRecorder()
	go func() {
		sup.ServeHTTP(w, req)
		close(proxyDone)
	}()
	time.Sleep(100 * time.Millisecond) // wait for the first chunk to be forwarded and the proxy to block on the backend read
	ctxCancel()                        // simulate the client disconnect

	select {
	case <-proxyDone:
	case <-time.After(3 * time.Second):
		t.Fatal("client disconnect did not unblock the proxy handler")
	}
	select {
	case <-backendDone:
	case <-time.After(3 * time.Second):
		t.Fatal("backend request not aborted after client disconnect")
	}
}
