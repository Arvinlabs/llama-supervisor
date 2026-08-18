package main

import (
	"context"
	"flag"
	"log"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"os/exec"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

// Config 对应 config.yaml 的配置项
type Config struct {
	Host     string `yaml:"host"`
	Port     int    `yaml:"port"`
	Backend  string `yaml:"backend"`
	Interval int    `yaml:"interval"` // 计时间隔，单位：秒
	Command  string `yaml:"command"`  // 计时到期后执行的命令

	StartupCommand bool `yaml:"startupCommand"` // 启动时是否同步执行一次 command（失败则启动失败）
}

// cmdTimer 命令计时器：
//   - 服务启动后，收到第一个请求时开始计时
//   - 每收到一个请求，计时器重置
//   - 计时到期执行一次 command
//   - 执行 command 后，若没有请求则不再计时；再次收到请求后重新开始计时
type cmdTimer struct {
	deadline time.Duration
	cmd      string

	mu    sync.Mutex
	timer *time.Timer
}

func newCmdTimer(cmd string, deadline time.Duration) *cmdTimer {
	return &cmdTimer{deadline: deadline, cmd: cmd}
}

// onHTTPRequest 每收到一个请求调用一次，重置计时器
func (t *cmdTimer) onHTTPRequest() {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.timer != nil {
		t.timer.Stop()
	}
	t.timer = time.AfterFunc(t.deadline, func() {
		t.mu.Lock()
		t.timer = nil // 计时结束，停止计时，直到下次请求
		t.mu.Unlock()

		log.Printf("[timer] interval(%s) elapsed, executing command: %s", t.deadline, t.cmd)
		out, err := exec.Command("sh", "-c", t.cmd).CombinedOutput()
		if len(out) > 0 {
			log.Printf("[timer] command output: %s", out)
		}
		if err != nil {
			log.Printf("[timer] command failed: %v", err)
		} else {
			log.Printf("[timer] command executed successfully")
		}
	})
}

// stop 停止计时器（服务退出时调用）
func (t *cmdTimer) stop() {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.timer != nil {
		t.timer.Stop()
		t.timer = nil
	}
}

// runCommandOnce 同步执行一次 command，失败返回错误
func runCommandOnce(ctx context.Context, command string) error {
	log.Printf("[startup] executing command: %s", command)
	cmd := exec.CommandContext(ctx, "sh", "-c", command)
	out, err := cmd.CombinedOutput()
	if len(out) > 0 {
		log.Printf("[startup] command output: %s", out)
	}
	if err != nil {
		log.Printf("[startup] command failed: %v", err)
		return err
	}
	log.Printf("[startup] command executed successfully")
	return nil
}

func loadConfig(path string) (Config, error) {
	var cfg Config
	if err := yamlUnmarshalFile(path, &cfg); err != nil {
		return cfg, err
	}
	return cfg, nil
}

func newBackendProxy(backend string) *httputil.ReverseProxy {
	target, err := url.Parse("http://" + backend)
	if err != nil {
		log.Fatalf("[config] invalid backend %q: %v", backend, err)
	}
	proxy := httputil.NewSingleHostReverseProxy(target)
	proxy.ErrorHandler = func(w http.ResponseWriter, r *http.Request, err error) {
		log.Printf("[proxy] error: %v", err)
		http.Error(w, "bad gateway", http.StatusBadGateway)
	}
	return proxy
}

func main() {
	configPath := flag.String("config", "config.yaml", "config file path")
	flag.Parse()

	cfg, err := loadConfig(*configPath)
	if err != nil {
		log.Fatalf("[config] load: %v", err)
	}
	if cfg.Interval <= 0 {
		log.Fatalf("[config] interval must be positive, got %d", cfg.Interval)
	}
	log.Printf("[config] host=%s port=%d backend=%s interval=%ds",
		cfg.Host, cfg.Port, cfg.Backend, cfg.Interval)
	log.Printf("[config] command=%q", cfg.Command)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// 配置开启时，启动时同步执行一次 command，失败则启动失败退出
	if cfg.StartupCommand {
		if err := runCommandOnce(ctx, cfg.Command); err != nil {
			log.Fatalf("[startup] failed: command execution failed, exiting")
		}
	} else {
		log.Println("[startup] startupCommand disabled, skip executing command")
	}

	proxy := newBackendProxy(cfg.Backend)
	timer := newCmdTimer(cfg.Command, time.Duration(cfg.Interval)*time.Second)

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		timer.onHTTPRequest() // 每收到请求重置计时
		log.Printf("[proxy] %s %s", r.Method, r.URL.RequestURI())
		proxy.ServeHTTP(w, r)
	})

	server := &http.Server{
		Addr:    netAddr(cfg.Host, cfg.Port),
		Handler: handler,
	}

	log.Printf("[server] listening on %s, proxying to %s", server.Addr, cfg.Backend)
	defer timer.stop()

	// 收到 Ctrl+C / SIGTERM 后优雅关闭,保持默认 shutdown 行为
	go func() {
		<-ctx.Done()
		log.Println("[shutdown] signal received, shutting down...")
		timer.stop()
		sctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := server.Shutdown(sctx); err != nil {
			log.Printf("[shutdown] error: %v", err)
		}
	}()

	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatalf("[server] error: %v", err)
	}
}
