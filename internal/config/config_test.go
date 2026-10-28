package config

import (
	"os"
	"path/filepath"
	"testing"
)

// writeConfig writes a config file into a temp dir and loads it
func loadTempConfig(t *testing.T, body string) (Config, error) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	return LoadConfig(path)
}

// the cloud numeric fields are validated: an out-of-range value must be an error
// instead of being silently replaced by the default
func TestLoadConfigValidatesCloudFields(t *testing.T) {
	cases := []struct {
		name string
		body string
	}{
		{"threshold above 1", "backend: http://127.0.0.1:8080\ncloud:\n  threshold: 1.5\n"},
		{"negative threshold", "backend: http://127.0.0.1:8080\ncloud:\n  threshold: -0.1\n"},
		{"negative outputReserve", "backend: http://127.0.0.1:8080\ncloud:\n  outputReserve: -1\n"},
		{"negative maxConcurrency", "backend: http://127.0.0.1:8080\ncloud:\n  maxConcurrency: -2\n"},
		{"negative estimateMargin", "backend: http://127.0.0.1:8080\ncloud:\n  estimateMargin: -1.5\n"},
		{"missing apiKey", "backend: http://127.0.0.1:8080\ncloud:\n  enable: true\n  baseUrl: https://api.deepseek.com\n"},
		{"bad baseUrl", "backend: http://127.0.0.1:8080\ncloud:\n  enable: true\n  baseUrl: api.deepseek.com\n  apiKey: sk-x\n"},
		{"unknown tokenizer", "backend: http://127.0.0.1:8080\ncloud:\n  tokenizer: heuristics\n"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if _, err := loadTempConfig(t, c.body); err == nil {
				t.Fatalf("config must be rejected:\n%s", c.body)
			}
		})
	}
}

// the documented cloud values load, and the effective helpers normalize them
func TestLoadConfigAcceptsCloudFields(t *testing.T) {
	cfg, err := loadTempConfig(t, `backend: http://127.0.0.1:8080
cloud:
  enable: true
  baseUrl: "https://api.deepseek.com"
  apiKey: "sk-x"
  model: "deepseek-chat"
  localMaxTokens: 8192
  threshold: 0.9
  outputReserve: 1024
  estimateMargin: 1.15
  maxConcurrency: 8
  tokenizer: "tiktoken"
  tiktokenModel: "gpt-4o"
`)
	if err != nil {
		t.Fatalf("valid config rejected: %v", err)
	}
	if !cfg.Cloud.Enabled() {
		t.Fatal("cloud must be enabled")
	}
	if got := cfg.Cloud.MaxConcurrency; got != 8 {
		t.Fatalf("maxConcurrency = %d", got)
	}
	if got := cfg.Cloud.Tokenizer; got != "tiktoken" {
		t.Fatalf("tokenizer = %q, want the configured tiktoken", got)
	}
	if got := CloudEstimateMargin(cfg); got != 1.15 {
		t.Fatalf("estimateMargin = %v, want the configured 1.15", got)
	}
}

// the defaults are used when the optional cloud fields are absent
func TestCloudDefaults(t *testing.T) {
	cfg, err := loadTempConfig(t, `backend: http://127.0.0.1:8080
cloud:
  enable: true
  baseUrl: "https://api.deepseek.com"
  apiKey: "sk-x"
`)
	if err != nil {
		t.Fatal(err)
	}
	if got := CloudThreshold(cfg); got != 0.9 {
		t.Fatalf("threshold = %v, want 0.9", got)
	}
	if got := CloudOutputReserve(cfg); got != 1024 {
		t.Fatalf("outputReserve = %d, want 1024", got)
	}
	if got := CloudEstimateMargin(cfg); got != 1 {
		t.Fatalf("estimateMargin = %v, want 1 (default)", got)
	}
	if got := cfg.Cloud.Tokenizer; got != "" {
		t.Fatalf("tokenizer = %q, want empty (tiktoken by default)", got)
	}
}
