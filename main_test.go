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

// 客户端中途断开时，代理到后端的请求上下文必须被取消（提前结束后端生成，不空跑到完成）
func TestProxyClientDisconnectCancelsBackend(t *testing.T) {
	backendErr := make(chan error, 1)
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		go func() {
			<-r.Context().Done()
			backendErr <- r.Context().Err()
		}()
		fl := w.(http.Flusher)
		for i := 0; i < 200; i++ { // 长流式响应，模拟持续生成
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

	// 客户端：读到首批流式数据后立即断开连接
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

// 客户端断开时，代理正阻塞在后端流式读取（后端长时间不发包，如思考阶段）：
// ctxBody 必须主动关闭后端连接，代理 handler 立即返回，后端请求被终止
func TestProxyClientDisconnectAbortsBlockedBackend(t *testing.T) {
	backendDone := make(chan struct{})
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("partial")) // 只发首包，之后长时间不发包（模拟思考/慢生成）
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
		<-r.Context().Done() // 等待代理提前断开连接
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
	time.Sleep(100 * time.Millisecond) // 等首包转发、代理阻塞在后端读取上
	ctxCancel() // 模拟客户端断开

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
