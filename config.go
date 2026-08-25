package main

import (
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

const defaultInterval = 600 * time.Second

// RestartGroup restart config; Enable true means enabled
type RestartGroup struct {
	Enable   bool   `yaml:"enable"`   // whether enabled
	Interval int    `yaml:"interval"` // idle seconds before triggering, default 600
	Command  string `yaml:"command"`  // command run on timeout
}

func (g *RestartGroup) Enabled() bool {
	return g != nil && g.Enable
}

// ProbeGroup probe config; Enable true means enabled
type ProbeGroup struct {
	Enable       bool   `yaml:"enable"`       // whether enabled
	Interval     int    `yaml:"interval"`     // idle seconds before triggering, default 600
	Command      string `yaml:"command"`      // command run after the probe declares unhealthy
	Model        string `yaml:"model"`        // probe model name, default "default"
	Prompt       string `yaml:"prompt"`       // probe prompt, default "hi"
	MaxTokens    int    `yaml:"maxTokens"`    // max generated tokens for the probe, default 64
	RepeatLimit  int    `yaml:"repeatLimit"`  // repeated-tail threshold for declaring unhealthy, default 10
	SuccessLimit int    `yaml:"successLimit"` // normal content reaching this many cumulative characters is declared healthy early, default 20
	Timeout      int    `yaml:"timeout"`      // probe timeout in seconds, default 5
}

func (g *ProbeGroup) Enabled() bool {
	return g != nil && g.Enable
}

// WatchdogGroup watchdog config; Enable true means enabled
type WatchdogGroup struct {
	Enable   bool    `yaml:"enable"`   // whether enabled
	Interval int     `yaml:"interval"` // sampling interval in seconds, default 2
	MaxRate  float64 `yaml:"maxRate"`  // max generation speed (t/s); above it is declared unhealthy (output loop), default 300
	Times    int     `yaml:"times"`    // consecutive over-speed samples required to declare unhealthy, default 2
	Pause    int     `yaml:"pause"`    // seconds the watchdog fully pauses (no fetching) after a trigger or a /slots fetch failure, default 90
	Verbose  bool    `yaml:"verbose"`  // whether to log the measured speed on normal windows, default false
	Command  string  `yaml:"command"`  // shell command run after declaring unhealthy
}

func (g *WatchdogGroup) Enabled() bool {
	return g != nil && g.Enable
}

// RequestGroup request policy config; each sub-feature is independently switchable
type RequestGroup struct {
	// PrefixCache normalizes /v1/chat/completions request bodies so that semantically
	// identical requests produce identical bytes, maximizing the backend prefix cache hit rate
	PrefixCache bool `yaml:"prefixCache"`
}

// Enabled reports whether any request policy sub-feature is enabled
func (g *RequestGroup) Enabled() bool {
	return g != nil && g.PrefixCache
}

type Config struct {
	Host           string         `yaml:"host"`
	Port           int            `yaml:"port"`
	Backend        string         `yaml:"backend"`
	ApiKey         string         `yaml:"apiKey"` // global backend API key, sent as Bearer <key> on probe and /slots sampling requests; not used by normal proxying
	StartupCommand string         `yaml:"startupCommand"`
	Restart        *RestartGroup  `yaml:"restart"`
	Probe          *ProbeGroup    `yaml:"probe"`
	Watchdog       *WatchdogGroup `yaml:"watchdog"`
	Request        *RequestGroup  `yaml:"request"`
}

// restartInterval returns the restart group's idle threshold (configured in seconds)
func restartInterval(cfg Config) time.Duration {
	if cfg.Restart.Enabled() && cfg.Restart.Interval > 0 {
		return time.Duration(cfg.Restart.Interval) * time.Second
	}
	return defaultInterval
}

// probeInterval returns the probe group's idle threshold (configured in seconds)
func probeInterval(cfg Config) time.Duration {
	if cfg.Probe.Enabled() && cfg.Probe.Interval > 0 {
		return time.Duration(cfg.Probe.Interval) * time.Second
	}
	return defaultInterval
}

func loadConfig(path string) (Config, error) {
	var cfg Config
	data, err := os.ReadFile(path)
	if err != nil {
		return cfg, fmt.Errorf("read config: %w", err)
	}
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return cfg, fmt.Errorf("parse config: %w", err)
	}
	if cfg.Backend == "" {
		return cfg, fmt.Errorf("config missing backend")
	}
	return cfg, nil
}

func secretMask(s string) string {
	if s == "" {
		return "(empty)"
	}
	if len(s) <= 4 {
		return "****"
	}
	return "****" + s[len(s)-4:]
}
