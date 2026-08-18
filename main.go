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
)

var cfgPath = flag.String("config", "config.yaml", "配置文件路径")

// proxy 反向代理，附加空闲重启与探测逻辑
type proxy struct {
	proxy      *httputil.ReverseProxy
	backendURL string

	restart *restartPolicy // 重启策略（restart 未启用时为 nil）
	probe   *probePolicy   // 探测策略（probe 未启用时为 nil）
}

// newBackendProxy 创建到后端的反向代理
func newBackendProxy(cfg Config) *proxy {
	backend, err := url.Parse(cfg.Backend)
	if err != nil {
		log.Fatalf("backend %q: %v", cfg.Backend, err)
	}
	p := &proxy{
		proxy:      httputil.NewSingleHostReverseProxy(backend),
		backendURL: cfg.Backend,
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

func (p *proxy) stop(ctx context.Context) {
	if p.restart != nil {
		p.restart.stop(ctx)
	}
	if p.probe != nil {
		p.probe.stop(ctx)
	}
}

// ServeHTTP 请求到来时处理，必要时执行命令，然后转发
func (p *proxy) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	// probe 与 restart 的 command 不在同一请求内都执行：
	// probe 本次实际执行了 command（后端已重启）则跳过 restart 并重置其计时；
	// probe 探测正常未执行 command 时，restart 照常触发
	if p.probe != nil && p.probe.consumeIdle(ctx) {
		if p.restart != nil {
			p.restart.reset()
		}
		p.proxy.ServeHTTP(w, r)
		return
	}
	if p.restart != nil {
		p.restart.consumeIdle(ctx)
	}
	p.proxy.ServeHTTP(w, r)
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
		log.Printf("[config] restart enabled: interval=%ds command=%q",
			int(restartInterval(cfg).Seconds()), cfg.Restart.Command)
	} else {
		log.Print("[config] restart disabled")
	}
	if cfg.Probe.Enabled() {
		pc := buildProbeConfig(cfg.Probe)
		log.Printf("[config] probe enabled: interval=%ds command=%q model=%q prompt=%q maxTokens=%d repeatLimit=%d timeout=%ds apiKey=%q",
			int(probeInterval(cfg).Seconds()), cfg.Probe.Command, pc.model, pc.prompt, pc.maxTokens, pc.repeatLimit, int(pc.timeout.Seconds()), secretMask(pc.apiKey))
	} else {
		log.Print("[config] probe disabled")
	}

	// 启动命令
	if cfg.StartupCommand != "" {
		runCommand(ctx, "startup", cfg.StartupCommand)
	}

	// 创建代理
	sup := newBackendProxy(cfg)

	// 启动后先探测一次
	if sup.probe != nil {
		log.Print("[startup] probing backend...")
		sup.probe.runProbe(ctx)
	}

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

	// 优雅退出
	go func() {
		<-ctx.Done()
		log.Print("received exit signal, waiting for idle timeout to trigger...")
		// 根 ctx 已取消，退出时触发的命令改用脱离取消的 ctx，避免被立即终止
		sup.stop(context.WithoutCancel(ctx))
		log.Print("idle logic triggered, exiting")
		_ = srv.Close()
	}()

	_ = srv.Serve(ln)
}
