package config

import (
	"fmt"
	"net/url"
	"os"
	"strings"
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
	RepeatChars  string `yaml:"repeatChars"`  // whitelist: only these characters' consecutive repetition counts as degenerate; empty (default) means any character
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
	// MinDraftRate minimum MTP draft acceptance ratio (timings.draft_n_accepted / timings.draft_n)
	// observed per chat completion; a completion below it for draftTimes in a row is declared
	// unhealthy and runs command. Sampled from every completion the proxy observes (not /slots).
	// 0 (default) disables this check
	MinDraftRate float64 `yaml:"minDraftRate"`
	// DraftTimes consecutive low draft-acceptance completions required to declare unhealthy, default 10
	DraftTimes int `yaml:"draftTimes"`
	// RepeatLimit consecutive identical tail runes in a streaming completion's generated
	// content that mark it a dead loop: when the over-speed streak is reached, a streaming
	// in-flight completion triggers only when its content is degenerate (a non-streaming
	// one, having no content to judge, triggers directly). Default 10
	RepeatLimit int `yaml:"repeatLimit"`
	// RepeatChars whitelist: only these characters' consecutive repetition counts as a dead
	// loop; empty (default) means any character
	RepeatChars string `yaml:"repeatChars"`
	// HardRepeat consecutive identical tail runes in a streaming completion's generated
	// content that trigger on their own, without any over-speed (a model can also loop at a
	// speed that never crosses maxRate). Default 100, a configured value <= 0 disables
	HardRepeat int    `yaml:"hardRepeat"`
	Command    string `yaml:"command"` // shell command run after declaring unhealthy
}

func (g *WatchdogGroup) Enabled() bool {
	return g != nil && g.Enable
}

// DebugGroup debug config; Enable true means enabled
type DebugGroup struct {
	Enable   bool   `yaml:"enable"`   // whether enabled
	Path     string `yaml:"path"`     // endpoint path, default "/debug/command"
	Command  string `yaml:"command"`  // shell command run on request
	SavePath string `yaml:"savePath"` // when debug is enabled, directory the inbound proxied requests are dumped to as plain text files named by the request time; empty disables saving
	// OutSavePath when debug is enabled, directory the outbound proxied requests (after the request
	// policy has rewritten it, when enabled) are dumped to as plain text files named by the request
	// time; empty disables saving
	OutSavePath string `yaml:"outSavePath"`
	// RespSavePath when debug is enabled, directory the backend responses are dumped to as JSON
	// files named by the response time: non-streaming bodies are stored as the parsed JSON (or
	// base64-encoded when not valid JSON), streaming (SSE) chat completions are assembled into
	// the non-streaming structure (deltas merged into one message, usage/timings from the final
	// chunk); empty disables saving
	RespSavePath string `yaml:"respSavePath"`
}

func (g *DebugGroup) Enabled() bool {
	return g != nil && g.Enable
}

// ErrGuardGroup backend error guard config; Enable true means enabled. The
// supervisor taps every proxied /v1/chat/completions response (streaming and
// non-streaming) and looks for an OpenAI-style error object in the backend
// payload: a stream `data: {"error":{...}}` event or a non-stream JSON body
// `{"error":{...}}`. When the error message matches any configured pattern the
// command runs (e.g. a supervisorctl restart), so a backend fault that surfaces
// as an error payload instead of a hang or an output loop is also self-healed
type ErrGuardGroup struct {
	Enable bool `yaml:"enable"` // whether enabled
	// Patterns is the list of regular expressions (Go regexp) matched against the
	// backend error message. An empty list matches nothing (the guard never triggers)
	Patterns []string `yaml:"patterns"`
	// Command shell command run when an error message matches
	Command string `yaml:"command"`
	// Cooldown seconds during which the command is not re-run after a trigger, so
	// a burst of matching errors (many concurrent requests hitting the same backend
	// fault) runs the command once. Default 30, a value <= 0 disables the cooldown
	Cooldown int `yaml:"cooldown"`
}

func (g *ErrGuardGroup) Enabled() bool {
	return g != nil && g.Enable
}

// EffectiveCooldown returns the error guard's effective cooldown: the configured
// seconds, the default 30s when unset (0), and 0 (disabled) when set negative
func (g *ErrGuardGroup) EffectiveCooldown() time.Duration {
	if g == nil {
		return 0
	}
	switch {
	case g.Cooldown < 0:
		return 0 // explicitly disabled
	case g.Cooldown > 0:
		return time.Duration(g.Cooldown) * time.Second
	}
	return 30 * time.Second
}

// RequestGroup request policy config; Enable true means enabled, each sub-feature
// is independently switchable within the group
type RequestGroup struct {
	Enable bool `yaml:"enable"` // whether enabled
	// PrefixCache normalizes /v1/chat/completions request bodies so that semantically
	// identical requests produce identical bytes, maximizing the backend prefix cache hit rate
	PrefixCache bool `yaml:"prefixCache"`
	// VirtualKeys is the list of virtual API keys; when non-empty the supervisor
	// requires clients to present one of them (OpenAI format, "Authorization: Bearer <key>")
	// and re-signs the outbound request with the global apiKey. The virtual keys never
	// reach the backend
	VirtualKeys []string `yaml:"virtualKeys"`
}

// Enabled reports whether the request policy is enabled (independent of the sub-features)
func (g *RequestGroup) Enabled() bool {
	return g != nil && g.Enable
}

// AnthropicGroup Anthropic protocol support config; Enable true means the
// supervisor accepts the Anthropic Messages API at POST /v1/messages: requests
// are translated to the OpenAI chat completion protocol at the outer edge and
// proxied to the supervisor itself, so the full OpenAI pipeline (auth, request
// policy, usage injection, observation, watchdog) runs unmodified internally
type AnthropicGroup struct {
	Enable bool `yaml:"enable"` // whether enabled
	// ModelMap optional client model name -> backend model name remapping;
	// unmapped names pass through to the backend unchanged
	ModelMap map[string]string `yaml:"modelMap"`
}

func (g *AnthropicGroup) Enabled() bool {
	return g != nil && g.Enable
}

// CloudGroup cloud routing config; Enable true (and BaseURL and APIKey set)
// means enabled. When the estimated size of a /v1/chat/completions request
// exceeds the local model's context window, that request is routed to the
// configured OpenAI-compatible cloud endpoint instead of the local backend
type CloudGroup struct {
	Enable bool `yaml:"enable"` // whether enabled
	// BaseURL the cloud endpoint base URL, e.g. https://api.deepseek.com (no trailing slash)
	BaseURL string `yaml:"baseUrl"`
	// APIKey the cloud API key, sent to the cloud as Bearer <key>; client keys
	// (virtual or real) never reach the cloud
	APIKey string `yaml:"apiKey"`
	// Model the model name rewritten into the request when it is routed to the
	// cloud; empty passes the client's model name through unchanged
	Model string `yaml:"model"`
	// LocalMaxTokens the local context window in tokens; 0 (default) means
	// auto-detect it from the backend (GET /props n_ctx, otherwise: the n_ctx=
	// suffix of the model ids from GET /v1/models)
	LocalMaxTokens int `yaml:"localMaxTokens"`
	// Threshold the request is routed to the cloud when its estimated token
	// size exceeds n_ctx * threshold; default 0.9 (a 10% headroom)
	Threshold float64 `yaml:"threshold"`
	// OutputReserve the output tokens reserved for the decision when the client
	// did not set max_tokens; the decision uses max(max_tokens, this). Default 1024
	OutputReserve int `yaml:"outputReserve"`
	// EstimateMargin the factor the prompt size estimate is multiplied by before
	// the routing decision (the estimate is a lower bound: image parts and the
	// chat template's rendering are only approximated, and some templates render
	// the tools definitions repeatedly). Default 1 (no allowance)
	EstimateMargin float64 `yaml:"estimateMargin"`
	// MaxConcurrency caps how many requests may be in flight to the cloud at
	// once; further requests wait for a slot (or for their client to disconnect).
	// 0 (default) means unlimited
	MaxConcurrency int `yaml:"maxConcurrency"`
	// Tokenizer the source of the prompt token count: "tiktoken" (default)
	// counts the text locally with the OpenAI-compatible BPE encoding of
	// TiktokenModel, without any backend round-trip (the decision works while
	// the backend is down; the count is approximate - the local model uses
	// its own tokenizer - which EstimateMargin should cover); "backend"
	// counts it with the local backend's own tokenizer (POST /tokenize,
	// exact: what the local model would actually encode)
	Tokenizer string `yaml:"tokenizer"`
	// TiktokenModel the model name whose tiktoken encoding is used when
	// Tokenizer is "tiktoken"; any name tiktoken knows (e.g. "gpt-4o") selects
	// its encoding, an empty or unknown name uses cl100k_base
	TiktokenModel string `yaml:"tiktokenModel"`
}

func (g *CloudGroup) Enabled() bool {
	return g != nil && g.Enable && g.BaseURL != "" && g.APIKey != ""
}

// CloudThreshold returns the cloud group's effective threshold (default 0.9)
func CloudThreshold(cfg Config) float64 {
	if cfg.Cloud != nil && cfg.Cloud.Threshold > 0 && cfg.Cloud.Threshold <= 1 {
		return cfg.Cloud.Threshold
	}
	return 0.9
}

// CloudOutputReserve returns the cloud group's effective output reserve in tokens (default 1024)
func CloudOutputReserve(cfg Config) int {
	if cfg.Cloud != nil && cfg.Cloud.OutputReserve > 0 {
		return cfg.Cloud.OutputReserve
	}
	return 1024
}

// CloudEstimateMargin returns the cloud group's effective estimate margin (default 1)
func CloudEstimateMargin(cfg Config) float64 {
	if cfg.Cloud != nil && cfg.Cloud.EstimateMargin > 0 {
		return cfg.Cloud.EstimateMargin
	}
	return 1
}

// StatsGroup stats policy config; Enable true (and SavePath non-empty) means enabled
type StatsGroup struct {
	Enable     bool   `yaml:"enable"`     // whether enabled
	SavePath   string `yaml:"savePath"`   // directory the per-day stats JSON files (YYYY-MM-DD.json) are saved to; empty disables the policy
	RetainDays int    `yaml:"retainDays"` // how many days of stats files are kept (older ones are deleted), default 7
}

func (g *StatsGroup) Enabled() bool {
	return g != nil && g.Enable && g.SavePath != ""
}

type Config struct {
	Host           string          `yaml:"host"`
	Port           int             `yaml:"port"`
	Backend        string          `yaml:"backend"`
	ApiKey         string          `yaml:"apiKey"` // global backend API key, sent as Bearer <key> on probe and /slots sampling requests; not used by normal proxying
	StartupCommand string          `yaml:"startupCommand"`
	Restart        *RestartGroup   `yaml:"restart"`
	Probe          *ProbeGroup     `yaml:"probe"`
	Watchdog       *WatchdogGroup  `yaml:"watchdog"`
	Request        *RequestGroup   `yaml:"request"`
	Cloud          *CloudGroup     `yaml:"cloud"`
	Anthropic      *AnthropicGroup `yaml:"anthropic"`
	Stats          *StatsGroup     `yaml:"stats"`
	Debug          *DebugGroup     `yaml:"debug"`
	ErrGuard       *ErrGuardGroup  `yaml:"errguard"`
}

// RestartInterval returns the restart group's idle threshold (configured in seconds)
func RestartInterval(cfg Config) time.Duration {
	if cfg.Restart.Enabled() && cfg.Restart.Interval > 0 {
		return time.Duration(cfg.Restart.Interval) * time.Second
	}
	return defaultInterval
}

// StatsRetainDays returns the stats group's retention in days (default 7)
func StatsRetainDays(cfg Config) int {
	if cfg.Stats.Enabled() && cfg.Stats.RetainDays > 0 {
		return cfg.Stats.RetainDays
	}
	return 7
}

// ProbeInterval returns the probe group's idle threshold (configured in seconds)
func ProbeInterval(cfg Config) time.Duration {
	if cfg.Probe.Enabled() && cfg.Probe.Interval > 0 {
		return time.Duration(cfg.Probe.Interval) * time.Second
	}
	return defaultInterval
}

func LoadConfig(path string) (Config, error) {
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
	if cfg.Cloud != nil {
		if cfg.Cloud.Enable {
			if cfg.Cloud.BaseURL == "" || cfg.Cloud.APIKey == "" {
				return cfg, fmt.Errorf("cloud enabled but baseUrl/apiKey missing")
			}
			u, err := url.Parse(cfg.Cloud.BaseURL)
			if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
				return cfg, fmt.Errorf("cloud baseUrl %q is not a valid http(s) URL", cfg.Cloud.BaseURL)
			}
		}
		// the numeric fields are validated even when the cloud is disabled: a
		// typo would otherwise be silently replaced by the default value
		if cfg.Cloud.Threshold < 0 || cfg.Cloud.Threshold > 1 {
			return cfg, fmt.Errorf("cloud threshold %v: must be in (0,1]", cfg.Cloud.Threshold)
		}
		if cfg.Cloud.OutputReserve < 0 {
			return cfg, fmt.Errorf("cloud outputReserve %d: must not be negative", cfg.Cloud.OutputReserve)
		}
		if cfg.Cloud.EstimateMargin < 0 {
			return cfg, fmt.Errorf("cloud estimateMargin %v: must not be negative", cfg.Cloud.EstimateMargin)
		}
		if cfg.Cloud.MaxConcurrency < 0 {
			return cfg, fmt.Errorf("cloud maxConcurrency %d: must not be negative", cfg.Cloud.MaxConcurrency)
		}
		switch strings.ToLower(cfg.Cloud.Tokenizer) {
		case "", "backend", "tiktoken":
		default:
			return cfg, fmt.Errorf("cloud tokenizer %q: must be \"backend\" or \"tiktoken\"", cfg.Cloud.Tokenizer)
		}
	}
	return cfg, nil
}

func SecretMask(s string) string {
	if s == "" {
		return "(empty)"
	}
	if len(s) <= 4 {
		return "****"
	}
	return "****" + s[len(s)-4:]
}
