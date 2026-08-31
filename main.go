package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"runtime"
	"strconv"
	"strings"
	"syscall"

	"github.com/Arvinlabs/llama-supervisor/internal/command"
	"github.com/Arvinlabs/llama-supervisor/internal/config"
	"github.com/Arvinlabs/llama-supervisor/internal/debug"
	"github.com/Arvinlabs/llama-supervisor/internal/probe"
	"github.com/Arvinlabs/llama-supervisor/internal/proxy"
	"github.com/Arvinlabs/llama-supervisor/internal/watchdog"
)

// Version information set during build via -ldflags (see Makefile)
var (
	Version   = "dev"
	BuildTime = "unknown"
)

var (
	cfgPath = flag.String("config", "config.yaml", "path to the config file")
	showVer = flag.Bool("version", false, "print version information and exit")
)

// printVersion prints the build version information
func printVersion() {
	fmt.Printf("llama-supervisor %s\n", Version)
	fmt.Printf("Build Time: %s\n", BuildTime)
	fmt.Printf("Go Version: %s\n", runtime.Version())
	fmt.Printf("Platform: %s/%s\n", runtime.GOOS, runtime.GOARCH)
}

func main() {
	flag.Parse()
	if *showVer {
		printVersion()
		return
	}
	cfg, err := config.LoadConfig(*cfgPath)
	if err != nil {
		log.Fatalf("load config: %v", err)
	}

	// Root ctx: signal-driven, inherited globally (startup command, probes and request contexts all derive from it)
	ctx, stopSignal := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stopSignal()

	log.Printf("llama-supervisor %s (built %s, %s)", Version, BuildTime, runtime.Version())
	log.Print("[config] apiKey=" + config.SecretMask(cfg.ApiKey))

	if cfg.Restart.Enabled() {
		log.Printf("[config] restart enabled: interval=%ds",
			int(config.RestartInterval(cfg).Seconds()))
		log.Print("[config] restart command: " + cfg.Restart.Command)
	} else {
		log.Print("[config] restart disabled")
	}
	if cfg.Probe.Enabled() {
		pc := probe.BuildProbeConfig(cfg.Probe, cfg.ApiKey)
		log.Printf("[config] probe enabled: interval=%ds model=%q maxTokens=%d repeatLimit=%d successLimit=%d timeout=%ds",
			int(config.ProbeInterval(cfg).Seconds()), pc.Model, pc.MaxTokens, pc.RepeatLimit, pc.SuccessLimit, int(pc.Timeout.Seconds()))
		log.Print("[config] probe command: " + cfg.Probe.Command)
	} else {
		log.Print("[config] probe disabled")
	}
	if cfg.Watchdog.Enabled() {
		wc := watchdog.BuildWatchdogConfig(cfg.Watchdog)
		log.Printf("[config] watchdog enabled: interval=%ds maxRate=%gt/s times=%d pause=%ds",
			int(wc.Interval.Seconds()), wc.MaxRate, wc.Times, int(wc.Pause.Seconds()))
		log.Print("[config] watchdog command: " + cfg.Watchdog.Command)
	} else {
		log.Print("[config] watchdog disabled")
	}
	if cfg.Request.Enabled() {
		var feats []string
		if len(cfg.Request.VirtualKeys) > 0 {
			feats = append(feats, "virtualKeys("+strconv.Itoa(len(cfg.Request.VirtualKeys))+")")
		}
		if cfg.Request.PrefixCache {
			feats = append(feats, "prefixCache")
		}
		log.Printf("[config] request enabled: %s", strings.Join(feats, ", "))
	} else {
		log.Print("[config] request disabled")
	}
	if cfg.Debug.Enabled() {
		d := cfg.Debug
		if d.Path == "" {
			d.Path = debug.DefaultDebugPath
		}
		log.Printf("[config] debug enabled: path=%s", d.Path)
		log.Print("[config] debug command: " + d.Command)
		if d.SavePath != "" {
			log.Print("[config] debug: saving inbound proxied requests to " + d.SavePath)
		}
		if d.OutSavePath != "" {
			log.Print("[config] debug: saving outbound proxied requests to " + d.OutSavePath)
		}
	} else {
		log.Print("[config] debug disabled")
	}

	// startup command
	if cfg.StartupCommand != "" {
		command.RunCommand(ctx, "startup", cfg.StartupCommand)
	}

	// create the supervisor and start the background checkers
	sup := proxy.New(cfg, ctx)
	sup.StartBackground(ctx)

	ln, err := net.Listen("tcp", net.JoinHostPort(cfg.Host, strconv.Itoa(cfg.Port)))
	if err != nil {
		log.Fatalf("listen %s:%d failed: %v", cfg.Host, cfg.Port, err)
	}
	log.Printf("supervisor listening on http://%s:%d -> %s", cfg.Host, cfg.Port, cfg.Backend)

	srv := &http.Server{
		// request contexts inherit the root ctx
		BaseContext: func(net.Listener) context.Context { return ctx },
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if sup.HandleDebug(w, r) {
				return
			}
			sup.OnHTTPRequest()
			sup.ServeHTTP(w, r)
		}),
	}

	// close the server on exit signal
	go func() {
		<-ctx.Done()
		log.Print("received exit signal, shutting down")
		_ = srv.Close()
	}()

	_ = srv.Serve(ln)
}
