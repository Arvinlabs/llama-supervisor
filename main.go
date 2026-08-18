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
	Host           string `yaml:"host"`
	Port           int    `yaml:"port"`
	Backend        string `yaml:"backend"`
	Interval       int    `yaml:"interval"`       // 空闲计时间隔，单位：秒，超过后下个请求直接执行 command（重启）
	Command        string `yaml:"command"`        // 后端异常时执行的命令
	StartupCommand bool   `yaml:"startupCommand"` // 启动时是否同步执行一次 command（失败则启动失败）

	// ===== 后端探测（probe）相关参数 =====
	ProbeInterval    int    `yaml:"probeInterval"`    // 探测空闲计时间隔，单位：秒，超过后下个请求先探测后端，异常才执行 command（默认等于 interval）
	ProbeApiKey      string `yaml:"probeApiKey"`      // 探测 api key（仅探测时携带 Bearer <key>，正常代理不使用）
	ProbeModel       string `yaml:"probeModel"`       // 探测 /v1/chat/completions 使用的 model，默认 default
	ProbePrompt      string `yaml:"probePrompt"`      // 探测提示词，默认 hi
	ProbeMaxTokens   int    `yaml:"probeMaxTokens"`   // 探测最大生成 token 数，默认 64
	ProbeRepeatLimit int    `yaml:"probeRepeatLimit"` // 末尾同一字符连续出现达到该长度判定异常，默认 20
	ProbeTimeout     int    `yaml:"probeTimeout"`     // 探测超时（秒），默认 5
}

// idleTracker 空闲计时器：
//   - 服务启动后，收到第一个请求时开始计时
//   - 每收到一个请求，计时器重置
//   - 空闲超过 deadline 时标记 exceeded
//   - 下一个请求到来时，由 handler 消费该标记
type idleTracker struct {
	deadline time.Duration

	mu       sync.Mutex
	timer    *time.Timer
	exceeded bool
}

func newIdleTracker(deadline time.Duration) *idleTracker {
	return &idleTracker{deadline: deadline}
}

// onHTTPRequest 每收到一个请求调用一次，重置计时器
func (t *idleTracker) onHTTPRequest() {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.timer != nil {
		t.timer.Stop()
	}
	t.timer = time.AfterFunc(t.deadline, func() {
		t.mu.Lock()
		t.timer = nil
		t.exceeded = true // 空闲超时，等待下一个请求到来时处理
		t.mu.Unlock()
		log.Printf("[idle] interval(%s) elapsed with no requests", t.deadline)
	})
}

// consumeIdle 返回并重置“空闲超时”标记
func (t *idleTracker) consumeIdle() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	e := t.exceeded
	t.exceeded = false
	return e
}

// reset 停止计时并清除“空闲超时”标记
func (t *idleTracker) reset() {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.timer != nil {
		t.timer.Stop()
		t.timer = nil
	}
	t.exceeded = false
}

// stop 停止计时器（服务退出时调用）
func (t *idleTracker) stop() {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.timer != nil {
		t.timer.Stop()
		t.timer = nil
	}
}

// runCommand 同步执行一次 command，失败返回错误
func runCommand(ctx context.Context, tag, command string) error {
	log.Printf("[%s] executing command: %s", tag, command)
	cmd := exec.CommandContext(ctx, "sh", "-c", command)
	out, err := cmd.CombinedOutput()
	if len(out) > 0 {
		log.Printf("[%s] command output: %s", tag, out)
	}
	if err != nil {
		log.Printf("[%s] command failed: %v", tag, err)
		return err
	}
	log.Printf("[%s] command executed successfully", tag)
	return nil
}

func loadConfig(path string) (Config, error) {
	var cfg Config
	if err := yamlUnmarshalFile(path, &cfg); err != nil {
		return cfg, err
	}
	return cfg, nil
}

func applyProbeDefaults(cfg *Config) probeConfig {
	pc := probeConfig{
		backend:     cfg.Backend,
		apiKey:      cfg.ProbeApiKey,
		model:       cfg.ProbeModel,
		prompt:      cfg.ProbePrompt,
		maxTokens:   cfg.ProbeMaxTokens,
		repeatLimit: cfg.ProbeRepeatLimit,
		timeout:     time.Duration(cfg.ProbeTimeout) * time.Second,
	}
	if pc.model == "" {
		pc.model = "default"
	}
	if pc.prompt == "" {
		pc.prompt = "hi"
	}
	if pc.maxTokens <= 0 {
		pc.maxTokens = 64
	}
	if pc.repeatLimit <= 0 {
		pc.repeatLimit = 20
	}
	if pc.timeout <= 0 {
		pc.timeout = 5 * time.Second
	}
	return pc
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
	probeCfg := applyProbeDefaults(&cfg)
	probeInterval := cfg.ProbeInterval
	if probeInterval <= 0 {
		probeInterval = cfg.Interval
	}
	log.Printf("[config] host=%s port=%d backend=%s interval=%ds probeInterval=%ds",
		cfg.Host, cfg.Port, cfg.Backend, cfg.Interval, probeInterval)
	log.Printf("[config] command=%q", cfg.Command)
	log.Printf("[config] probeApiKey=%q probeModel=%q probePrompt=%q probeMaxTokens=%d probeRepeatLimit=%d probeTimeout=%ds",
		secretMask(cfg.ProbeApiKey), probeCfg.model, probeCfg.prompt, probeCfg.maxTokens, probeCfg.repeatLimit, probeCfg.timeout/time.Second)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// 配置开启时，启动时同步执行一次 command，失败则启动失败退出
	if cfg.StartupCommand {
		if err := runCommand(ctx, "startup", cfg.Command); err != nil {
			log.Fatalf("[startup] failed: command execution failed, exiting")
		}
	} else {
		log.Println("[startup] startupCommand disabled, skip executing command")
	}

	proxy := newBackendProxy(cfg.Backend)
	// command 空闲计时器：空闲超过 interval 后，下个请求直接执行 command
	cmdIdle := newIdleTracker(time.Duration(cfg.Interval) * time.Second)
	// probe 空闲计时器：空闲超过 probeInterval 后，下个请求先探测后端，异常才执行 command
	probeIdle := newIdleTracker(time.Duration(probeInterval) * time.Second)

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case cmdIdle.consumeIdle():
			// 空闲超过 interval，直接执行 command（重启），执行完后再 proxy
			log.Println("[health] idle interval exceeded, executing command")
			if err := runCommand(ctx, "health", cfg.Command); err != nil {
				log.Printf("[health] command failed: %v, proxy anyway", err)
			}
		case probeIdle.consumeIdle():
			// 空闲超过 probeInterval，先探测后端，异常才执行 command
			log.Printf("[health] probe idle interval exceeded, probing backend %s /v1/chat/completions ...", cfg.Backend)
			if abnormal := probeBackend(ctx, probeCfg); abnormal {
				log.Println("[health] backend appears abnormal (repeated last character or probe failed), executing command")
				if err := runCommand(ctx, "health", cfg.Command); err != nil {
					log.Printf("[health] command failed: %v, proxy anyway", err)
				}
				cmdIdle.reset() // 刚执行过 command，重置 command 空闲计时
			} else {
				log.Println("[health] backend looks healthy, skip command")
			}
		}
		cmdIdle.onHTTPRequest()
		probeIdle.onHTTPRequest()
		log.Printf("[proxy] %s %s", r.Method, r.URL.RequestURI())
		proxy.ServeHTTP(w, r)
	})

	server := &http.Server{
		Addr:    netAddr(cfg.Host, cfg.Port),
		Handler: handler,
	}

	log.Printf("[server] listening on %s, proxying to %s", server.Addr, cfg.Backend)
	defer func() {
		cmdIdle.stop()
		probeIdle.stop()
	}()

	// 收到 Ctrl+C / SIGTERM 后优雅关闭,保持默认 shutdown 行为
	go func() {
		<-ctx.Done()
		log.Println("[shutdown] signal received, shutting down...")
		cmdIdle.stop()
		probeIdle.stop()
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

// secretMask 日志中脱敏显示 api key
func secretMask(key string) string {
	if key == "" {
		return ""
	}
	if len(key) <= 4 {
		return "****"
	}
	return key[:2] + "****" + key[len(key)-2:]
}
