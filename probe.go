package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"time"
)

type probeConfig struct {
	apiKey       string
	model        string
	prompt       string
	maxTokens    int
	repeatLimit  int
	successLimit int // normal content reaching this many cumulative characters is declared healthy early without waiting for generation to end; 0 disables it
	timeout      time.Duration
}

// buildProbeConfig builds the effective probe parameters from the probe config group (filling in
// defaults); apiKey is the global apiKey
func buildProbeConfig(g *ProbeGroup, apiKey string) probeConfig {
	pc := probeConfig{
		apiKey:       apiKey,
		model:        "default",
		prompt:       "hi",
		maxTokens:    64,
		repeatLimit:  10,
		successLimit: 20,
		timeout:      5 * time.Second,
	}
	if g.Model != "" {
		pc.model = g.Model
	}
	if g.Prompt != "" {
		pc.prompt = g.Prompt
	}
	if g.MaxTokens > 0 {
		pc.maxTokens = g.MaxTokens
	}
	if g.RepeatLimit > 0 {
		pc.repeatLimit = g.RepeatLimit
	}
	if g.SuccessLimit > 0 {
		pc.successLimit = g.SuccessLimit
	} else if g.SuccessLimit < 0 {
		pc.successLimit = 0 // a negative value disables early success
	}
	if g.Timeout > 0 {
		pc.timeout = time.Duration(g.Timeout) * time.Second
	}
	// keep the early-success threshold at least the repeat threshold, so a degenerate stream is flagged unhealthy before it can be declared healthy early
	if pc.successLimit > 0 && pc.successLimit < pc.repeatLimit {
		pc.successLimit = pc.repeatLimit
	}
	return pc
}

// probePolicy probe strategy: on idle timeout probe the backend, and run probe.command when unhealthy
type probePolicy struct {
	// ctx is the server-level ctx (driven by the exit signal); the probe and command execution do
	// not follow the user request ctx, so an early user disconnect cannot cancel the probe and
	// cause a false unhealthy verdict
	ctx     context.Context
	tracker *idleTracker
	cmd     string
	backend string
	probe   probeConfig
}

func newProbePolicy(ctx context.Context, backend string, g *ProbeGroup, interval time.Duration, apiKey string) *probePolicy {
	p := &probePolicy{
		ctx:     ctx,
		cmd:     g.Command,
		backend: backend,
		probe:   buildProbeConfig(g, apiKey),
	}
	p.tracker = newIdleTracker(interval, func(ctx context.Context) bool {
		return p.runProbe(ctx)
	})
	return p
}

func (p *probePolicy) onHTTPRequest() {
	p.tracker.onHTTPRequest()
}

func (p *probePolicy) consumeIdle(_ context.Context) bool {
	// the passed ctx is the user request ctx; the probe does not inherit it and uses the server-level ctx instead
	return p.tracker.consumeIdle(p.ctx)
}

// runProbe probes the backend; on unhealthy it runs probe.command and returns whether the command was actually executed
func (p *probePolicy) runProbe(ctx context.Context) bool {
	log.Printf("[probe] triggered: backend=%s model=%q prompt=%q maxTokens=%d repeatLimit=%d successLimit=%d timeout=%ds",
		p.backend, p.probe.model, p.probe.prompt, p.probe.maxTokens, p.probe.repeatLimit, p.probe.successLimit, int(p.probe.timeout.Seconds()))
	healthy, err := probeBackend(ctx, p.backend, p.probe)
	if healthy {
		log.Print("[probe] backend looks healthy")
		return false
	}
	log.Printf("[probe] backend looks unhealthy (%v)", err)
	if p.cmd == "" {
		log.Print("[probe] no command configured, skip")
		return false
	}
	return runCommand(ctx, "health", p.cmd)
}

// probeBackend probes whether the backend is usable (via a streaming chat completion)
func probeBackend(ctx context.Context, backend string, pc probeConfig) (healthy bool, err error) {
	ctx, cancel := context.WithTimeout(ctx, pc.timeout)
	defer cancel()

	body, err := json.Marshal(map[string]any{
		"model": pc.model,
		"messages": []map[string]string{
			{"role": "user", "content": pc.prompt},
		},
		"max_tokens": pc.maxTokens,
		"stream":     true,
	})
	if err != nil {
		return false, err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, backend+"/v1/chat/completions", bytes.NewReader(body))
	if err != nil {
		return false, err
	}
	req.Header.Set("Content-Type", "application/json")
	if pc.apiKey != "" {
		req.Header.Set("Authorization", "Bearer "+pc.apiKey)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return false, fmt.Errorf("connect: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 2048))
		return false, fmt.Errorf("backend %d: %s", resp.StatusCode, strings.TrimSpace(string(msg)))
	}

	return probeBackendStreaming(resp.Body, pc)
}

// sseEvent is one SSE event line (OpenAI-compatible chat completion chunk; some models additionally return reasoning_content)
type sseEvent struct {
	Choices []struct {
		Delta struct {
			Content          string `json:"content"`
			ReasoningContent string `json:"reasoning_content"`
		} `json:"delta"`
	} `json:"choices"`
}

// repeatChecker independently counts the tail-character repeat run of one field
type repeatChecker struct {
	limit    int
	lastChar rune
	run      int
}

func (c *repeatChecker) check(field string, s string) (bool, error) {
	for _, r := range s {
		if r == c.lastChar {
			c.run++
		} else {
			c.lastChar = r
			c.run = 1
		}
		if c.run >= c.limit {
			return false, fmt.Errorf("degenerate %s: %d consecutive repeated char %q", field, c.run, c.lastChar)
		}
	}
	return true, nil
}

// probeBackendStreaming reads the SSE stream and checks whether the content is degenerate (repeated tokens)
func probeBackendStreaming(body io.Reader, pc probeConfig) (bool, error) {
	scanner := bufio.NewScanner(body)
	scanner.Buffer(make([]byte, 1024*1024), 1024*1024)

	reasoningCheck := &repeatChecker{limit: pc.repeatLimit}
	contentCheck := &repeatChecker{limit: pc.repeatLimit}
	totalLen := 0 // cumulative normal generated characters (reasoning_content + content)

	for scanner.Scan() {
		line := scanner.Text()
		if !strings.HasPrefix(line, "data:") {
			continue
		}
		payload := strings.TrimSpace(strings.TrimPrefix(line, "data:"))
		if payload == "[DONE]" {
			break
		}
		var ev sseEvent
		if err := json.Unmarshal([]byte(payload), &ev); err != nil {
			continue
		}
		if len(ev.Choices) == 0 {
			continue
		}
		delta := ev.Choices[0].Delta
		if ok, err := reasoningCheck.check("reasoning_content", delta.ReasoningContent); !ok {
			return false, err
		}
		if ok, err := contentCheck.check("content", delta.Content); !ok {
			return false, err
		}
		totalLen += len(delta.ReasoningContent) + len(delta.Content)
		// early success: content is normal and the cumulative length reached successLimit, no need to wait for generation to end
		if pc.successLimit > 0 && totalLen >= pc.successLimit {
			return true, nil
		}
	}
	if err := scanner.Err(); err != nil {
		return false, err
	}
	return true, nil
}
