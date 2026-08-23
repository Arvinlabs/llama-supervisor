package main

import (
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

const defaultInterval = 600 * time.Second

// RestartGroup 重启配置：enable 为 true 表示启用
type RestartGroup struct {
	Enable   bool   `yaml:"enable"`   // 是否启用
	Interval int    `yaml:"interval"` // 空闲多久(秒)触发，默认 600
	Command  string `yaml:"command"`  // 超时后执行的命令
}

func (g *RestartGroup) Enabled() bool {
	return g != nil && g.Enable
}

// ProbeGroup 探测配置：enable 为 true 表示启用
type ProbeGroup struct {
	Enable       bool   `yaml:"enable"`       // 是否启用
	Interval     int    `yaml:"interval"`     // 空闲多久(秒)触发，默认 600
	Command      string `yaml:"command"`      // 探测判定异常后执行的命令
	ApiKey       string `yaml:"apiKey"`       // 探测 api key（仅探测时携带 Bearer <key>，正常代理不使用）
	Model        string `yaml:"model"`        // 探测模型名，默认 "default"
	Prompt       string `yaml:"prompt"`       // 探测 prompt，默认 "hi"
	MaxTokens    int    `yaml:"maxTokens"`    // 探测最大生成 token 数，默认 64
	RepeatLimit  int    `yaml:"repeatLimit"`  // 连续重复字符判定异常的阈值，默认 10
	SuccessLimit int    `yaml:"successLimit"` // 正常内容累计达到该字符数即提前判定健康，默认 20
	Timeout      int    `yaml:"timeout"`      // 探测超时(秒)，默认 5
}

func (g *ProbeGroup) Enabled() bool {
	return g != nil && g.Enable
}

// WatchdogGroup 看门狗配置：enable 为 true 表示启用
type WatchdogGroup struct {
	Enable   bool    `yaml:"enable"`   // 是否启用
	Interval int     `yaml:"interval"` // 采样间隔(秒)，默认 3
	MaxRate  float64 `yaml:"maxRate"`  // 生成速度上限(t/s)，超过则判异常（死循环），默认 200
	Times    int     `yaml:"times"`    // 连续超速几次判异常，默认 1
	Command  string  `yaml:"command"`  // 判定异常后执行的命令(shell)
}

func (g *WatchdogGroup) Enabled() bool {
	return g != nil && g.Enable
}

type Config struct {
	Host           string         `yaml:"host"`
	Port           int            `yaml:"port"`
	Backend        string         `yaml:"backend"`
	StartupCommand string         `yaml:"startupCommand"`
	Restart        *RestartGroup  `yaml:"restart"`
	Probe          *ProbeGroup    `yaml:"probe"`
	Watchdog       *WatchdogGroup `yaml:"watchdog"`
}

// restartInterval 重启分组的空闲阈值（配置为秒）
func restartInterval(cfg Config) time.Duration {
	if cfg.Restart.Enabled() && cfg.Restart.Interval > 0 {
		return time.Duration(cfg.Restart.Interval) * time.Second
	}
	return defaultInterval
}

// probeInterval 探测分组的空闲阈值（配置为秒）
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
