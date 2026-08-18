package main

import (
	"context"
	"flag"
	"log"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"os/exec"
	"os/signal"
	"strconv"
	"syscall"
	"time"
)

var cfgPath = flag.String("config", "config.yaml", "配置文件路径")

// proxy 反向代理，附加空闲重启与探测逻辑
type proxy struct {
	proxy       *httputil.ReverseProxy
	backendURL  string
	backendAddr string        // backend 的 host:port（启动时校验必须显式带端口）
	readyDelay  time.Duration // 执行 command 后端口就绪后再等这么久才转发

	restart *restartPolicy // 重启策略（restart 未启用时为 nil）
	probe   *probePolicy   // 探测策略（probe 未启用时为 nil）
}

// newBackendProxy 创建到后端的反向代理
func newBackendProxy(cfg Config) *proxy {
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
	p := &proxy{
		proxy:       rp,
		backendURL:  cfg.Backend,
		backendAddr: backend.Host,
		readyDelay:  time.Duration(cfg.WaitBackendReady) * time.Second,
	}
	p.proxy.ErrorHandler = func(w http.ResponseWriter, r *http.Request, err error) {
		log.Printf("proxy %s %s: %v", r.Method, r.URL.Path, err)
		http.Error(w, "backend unavailable", http.StatusBadGateway)
	}

	if cfg.Restart.Enabled() {
		p.restart = newRestartPolicy(cfg.Restart, restartInterval(cfg))
	}
	if cfg.Probe.Enabled() {
		p.probe = newProbePolicy(cfg.Backend, cfg.Probe, probeInterval(cfg))
	}
	if p.restart == nil && p.probe == nil {
		log.Print("[config] restart and probe both disabled, proxying only")
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
	p.proxy.ServeHTTP(rec, r)
	logAccess(rec.status, r, start)
}

// startBackground 启动 restart 的独立后台空闲检查：
// 每秒检查一次，空闲到期则执行 restart.command，可周期性重复
func (p *proxy) startBackground(ctx context.Context) {
	if p.restart == nil {
		return
	}
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

// logAccess 记录一行访问日志：客户端、方法、路径、状态码、耗时
func logAccess(status int, r *http.Request, start time.Time) {
	if status == 0 {
		status = http.StatusOK
	}
	log.Printf("[access] %s %s %s %d %s", r.RemoteAddr, r.Method, r.URL.RequestURI(), status, time.Since(start).Round(time.Microsecond))
}

// waitBackendReady 执行 command 后等待后端端口可连接，再转发请求
func (p *proxy) waitBackendReady(ctx context.Context) {
	log.Printf("[proxy] waiting for backend %s to be ready", p.backendAddr)
	for {
		conn, err := net.DialTimeout("tcp", p.backendAddr, 2*time.Second)
		if err == nil {
			conn.Close()
			if p.readyDelay > 0 {
				log.Printf("[proxy] backend ready, waiting %s before forwarding", p.readyDelay)
				select {
				case <-time.After(p.readyDelay):
				case <-ctx.Done():
					log.Printf("[proxy] wait canceled: %v", ctx.Err())
					return
				}
			}
			log.Printf("[proxy] backend ready")
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
		log.Printf("[config] probe enabled: interval=%ds model=%q prompt=%q maxTokens=%d repeatLimit=%d timeout=%ds apiKey=%q",
			int(probeInterval(cfg).Seconds()), pc.model, pc.prompt, pc.maxTokens, pc.repeatLimit, int(pc.timeout.Seconds()), secretMask(pc.apiKey))
		log.Print("[config] probe command: " + cfg.Probe.Command)
	} else {
		log.Print("[config] probe disabled")
	}

	// 启动命令
	if cfg.StartupCommand != "" {
		runCommand(ctx, "startup", cfg.StartupCommand)
	}

	// 创建代理并启动 restart 后台空闲检查
	sup := newBackendProxy(cfg)
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
