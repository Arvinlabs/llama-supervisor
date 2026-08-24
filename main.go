package main

import (
	"context"
	"flag"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"os/exec"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"
)

var cfgPath = flag.String("config", "config.yaml", "配置文件路径")

// proxy 反向代理，附加空闲重启与探测逻辑
type proxy struct {
	proxy       *httputil.ReverseProxy
	backendURL  string
	backendAddr string // backend 的 host:port（启动时校验必须显式带端口）

	restart  *restartPolicy  // 重启策略（restart 未启用时为 nil）
	probe    *probePolicy    // 探测策略（probe 未启用时为 nil）
	watchdog *watchdogPolicy // 看门狗策略（watchdog 未启用时为 nil）
}

// newBackendProxy 创建到后端的反向代理，ctx 为服务器级 ctx（probe 探测不跟随用户请求）
func newBackendProxy(cfg Config, ctx context.Context) *proxy {
	backend, err := url.Parse(cfg.Backend)
	if err != nil {
		log.Fatalf("backend %q: %v", cfg.Backend, err)
	}
	if backend.Hostname() == "" || backend.Port() == "" {
		log.Fatalf("backend %q: host and explicit port are required", cfg.Backend)
	}
	rp := httputil.NewSingleHostReverseProxy(backend)
	// FlushInterval=0：每次 Write 后立即 flush，SSE/流式输出无缓冲直达客户端
	rp.FlushInterval = 0
	// 客户端断开（请求 ctx 取消）时主动关闭后端响应体：
	// Go 的 HTTP/1.1 响应体读不受请求 ctx 控制，否则代理会一直阻塞在后端流式读取上，后端持续生成
	rp.ModifyResponse = func(res *http.Response) error {
		cb := &ctxBody{ctx: res.Request.Context(), rc: res.Body, quit: make(chan struct{})}
		res.Body = cb
		cb.watch()
		return nil
	}
	p := &proxy{
		proxy:       rp,
		backendURL:  cfg.Backend,
		backendAddr: backend.Host,
	}
	p.proxy.ErrorHandler = func(w http.ResponseWriter, r *http.Request, err error) {
		if cerr := r.Context().Err(); cerr != nil {
			// 客户端已断开（ctx 已取消）：后端请求已被提前终止，不按后端故障处理
			log.Printf("[proxy] client disconnected, backend aborted: %s %s (%v)",
				r.Method, r.URL.Path, err)
		} else {
			log.Printf("proxy %s %s: %v", r.Method, r.URL.Path, err)
		}
		http.Error(w, "backend unavailable", http.StatusBadGateway)
	}

	if cfg.Restart.Enabled() {
		p.restart = newRestartPolicy(cfg.Restart, restartInterval(cfg))
	}
	if cfg.Probe.Enabled() {
		p.probe = newProbePolicy(ctx, cfg.Backend, cfg.Probe, probeInterval(cfg))
	}
	if cfg.Watchdog.Enabled() {
		p.watchdog = newWatchdogPolicy(cfg.Watchdog, cfg.Backend)
	}
	if p.restart == nil && p.probe == nil && p.watchdog == nil {
		log.Print("[config] restart, probe and watchdog all disabled, proxying only")
	}
	return p
}

func (p *proxy) onHTTPRequest() {
	if p.restart != nil {
		p.restart.onHTTPRequest()
	}
	if p.probe != nil {
		p.probe.onHTTPRequest()
	}
}

// statusRecorder 包装 ResponseWriter，记录响应状态码用于 access log
type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (s *statusRecorder) WriteHeader(code int) {
	s.status = code
	s.ResponseWriter.WriteHeader(code)
}

// Flush 透传 flush：ReverseProxy 需要 ResponseWriter 实现 http.Flusher 才会按 FlushInterval 流式转发
func (s *statusRecorder) Flush() {
	if f, ok := s.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

// ctxBody 包装后端响应体：ctx（客户端请求上下文）取消（客户端断开）时
// 主动关闭底层 body，中断可能正阻塞的读取，使后端连接被提前终止。
// Go 的 HTTP/1.1 客户端读响应体不受请求 ctx 控制（仅 http2 自动中断），
// 不处理的话客户端断开后代理仍会等后端流式读完，后端持续空跑
type ctxBody struct {
	ctx  context.Context
	rc   io.ReadCloser
	quit chan struct{}
	once sync.Once
}

func (b *ctxBody) Read(p []byte) (int, error) {
	if err := b.ctx.Err(); err != nil {
		return 0, err
	}
	return b.rc.Read(p)
}

func (b *ctxBody) Close() error {
	b.once.Do(func() {
		close(b.quit)
		// 未读完即 Close：http 客户端会关闭该 TCP 连接（而非归还连接池），
		// 后端因此感知到客户端断开，停止处理当前请求
		_ = b.rc.Close()
	})
	return nil
}

// watch 起一个伴随协程：ctx 取消（客户端断开）时打日志并 Close body；
// 响应正常结束（Close）时退出，无泄漏。
// 注意这里必须自己打日志：ReverseProxy 流式 copy 出错时不走 ErrorHandler，
// 而是 panic(http.ErrAbortHandler) 由 server 静默 recover（Issue 23643）
func (b *ctxBody) watch() {
	go func() {
		select {
		case <-b.ctx.Done():
			log.Printf("[proxy] client disconnected, aborting backend stream: %v", b.ctx.Err())
			b.Close()
		case <-b.quit:
		}
	}()
}

// ServeHTTP 请求到来时处理：probe 空闲超时则先探测（异常执行 command 并等后端就绪），然后转发，并记录 access log
func (p *proxy) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	start := time.Now()
	rec := &statusRecorder{ResponseWriter: w}
	// probe 空闲超时：先探测，实际执行了 command（后端已重启）则等待后端就绪
	// restart 计时无需手动重置：本次请求的 onHTTPRequest 已刷新
	if p.probe != nil && p.probe.consumeIdle(ctx) {
		p.waitBackendReady(ctx)
	}
	// 客户端断开时：请求 ctx 取消 -> ctxBody 关闭后端连接 -> 后端读取被中断，
	// copyResponse 出错走 ErrorHandler 打出 "[proxy] client disconnected, backend aborted"
	// 注意不能在此处用 r.Context().Err() 判断开：net/http 在 handler 正常返回后也会取消 ctx，会误报
	p.proxy.ServeHTTP(rec, r)
	logAccess(rec.status, r, start)
}

// startBackground 启动后台检查：
// restart 每秒检查一次，空闲到期则执行 restart.command，可周期性重复；
// watchdog 按配置间隔频繁采样 /slots，速度持续超过上限则执行 watchdog.command
func (p *proxy) startBackground(ctx context.Context) {
	if p.restart != nil {
		go func() {
			ticker := time.NewTicker(time.Second)
			defer ticker.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					p.restart.consumeIdle(ctx)
				}
			}
		}()
	}
	if p.watchdog != nil {
		go func() {
			ticker := time.NewTicker(p.watchdog.config.interval)
			defer ticker.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					p.watchdog.tick(ctx)
				}
			}
		}()
	}
}

// logAccess 记录一行访问日志：客户端、方法、路径、状态码、耗时
func logAccess(status int, r *http.Request, start time.Time) {
	if status == 0 {
		status = http.StatusOK
	}
	log.Printf("[access] %s %s %s %d %s", r.RemoteAddr, r.Method, r.URL.RequestURI(), status, time.Since(start).Round(time.Microsecond))
}

// waitBackendReady 执行 command 后轮询后端 /health，直到服务真正就绪（返回 2xx）才转发请求
func (p *proxy) waitBackendReady(ctx context.Context) {
	log.Printf("[proxy] waiting for backend %s to be ready", p.backendAddr)
	client := &http.Client{Timeout: 5 * time.Second}
	for {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, p.backendURL+"/health", nil)
		ok := false
		if err == nil {
			resp, err := client.Do(req)
			if err == nil {
				resp.Body.Close()
				ok = resp.StatusCode >= http.StatusOK && resp.StatusCode < http.StatusMultipleChoices
			}
		}
		if ok {
			log.Print("[proxy] backend ready")
			return
		}
		select {
		case <-time.After(500 * time.Millisecond):
		case <-ctx.Done():
			log.Printf("[proxy] wait canceled: %v", ctx.Err())
			return
		}
	}
}

// runCommand 执行命令并等待完成，ctx 取消时终止命令
func runCommand(ctx context.Context, label, cmdStr string) bool {
	if cmdStr == "" {
		return false
	}
	log.Printf("[%s] running: %s", label, cmdStr)
	c := exec.CommandContext(ctx, "sh", "-c", cmdStr)
	c.Stdout = os.Stdout
	c.Stderr = os.Stderr
	if err := c.Run(); err != nil {
		if ctx.Err() != nil {
			log.Printf("[%s] canceled: %v", label, ctx.Err())
		} else {
			log.Printf("[%s] failed: %v", label, err)
		}
		return false
	}
	log.Printf("[%s] completed", label)
	return true
}

func main() {
	flag.Parse()
	cfg, err := loadConfig(*cfgPath)
	if err != nil {
		log.Fatalf("load config: %v", err)
	}

	// 根 ctx：signal 驱动，全局继承（启动命令、探测、请求上下文均源自它）
	ctx, stopSignal := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stopSignal()

	if cfg.Restart.Enabled() {
		log.Printf("[config] restart enabled: interval=%ds",
			int(restartInterval(cfg).Seconds()))
		log.Print("[config] restart command: " + cfg.Restart.Command)
	} else {
		log.Print("[config] restart disabled")
	}
	if cfg.Probe.Enabled() {
		pc := buildProbeConfig(cfg.Probe)
		log.Printf("[config] probe enabled: interval=%ds model=%q maxTokens=%d repeatLimit=%d successLimit=%d timeout=%ds apiKey=%q",
			int(probeInterval(cfg).Seconds()), pc.model, pc.maxTokens, pc.repeatLimit, pc.successLimit, int(pc.timeout.Seconds()), secretMask(pc.apiKey))
		log.Print("[config] probe command: " + cfg.Probe.Command)
	} else {
		log.Print("[config] probe disabled")
	}
	if cfg.Watchdog.Enabled() {
		wc := buildWatchdogConfig(cfg.Watchdog)
		log.Printf("[config] watchdog enabled: interval=%ds maxRate=%gt/s times=%d apiKey=%q",
			int(wc.interval.Seconds()), wc.maxRate, wc.times, secretMask(cfg.Watchdog.ApiKey))
		log.Print("[config] watchdog command: " + cfg.Watchdog.Command)
	} else {
		log.Print("[config] watchdog disabled")
	}

	// 启动命令
	if cfg.StartupCommand != "" {
		runCommand(ctx, "startup", cfg.StartupCommand)
	}

	// 创建代理并启动 restart 后台空闲检查
	sup := newBackendProxy(cfg, ctx)
	sup.startBackground(ctx)

	ln, err := net.Listen("tcp", net.JoinHostPort(cfg.Host, strconv.Itoa(cfg.Port)))
	if err != nil {
		log.Fatalf("listen %s:%d failed: %v", cfg.Host, cfg.Port, err)
	}
	log.Printf("supervisor listening on http://%s:%d -> %s", cfg.Host, cfg.Port, cfg.Backend)

	srv := &http.Server{
		// 请求上下文继承根 ctx
		BaseContext: func(net.Listener) context.Context { return ctx },
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			sup.onHTTPRequest()
			sup.ServeHTTP(w, r)
		}),
	}

	// 收到退出信号则关闭服务
	go func() {
		<-ctx.Done()
		log.Print("received exit signal, shutting down")
		_ = srv.Close()
	}()

	_ = srv.Serve(ln)
}
