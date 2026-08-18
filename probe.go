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
	apiKey      string
	model       string
	prompt      string
	maxTokens   int
	repeatLimit int
	timeout     time.Duration
}

// buildProbeConfig 从探测分组构建实际探测参数（补默认值）
func buildProbeConfig(g *ProbeGroup) probeConfig {
	pc := probeConfig{
		apiKey:      g.ApiKey,
		model:       "default",
		prompt:      "hi",
		maxTokens:   64,
		repeatLimit: 20,
		timeout:     5 * time.Second,
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
	if g.Timeout > 0 {
		pc.timeout = time.Duration(g.Timeout) * time.Second
	}
	return pc
}

// probePolicy 探测策略：空闲超时则探测后端，判定异常则执行 probe.command
type probePolicy struct {
	tracker *idleTracker
	cmd     string
	backend string
	probe   probeConfig
}

func newProbePolicy(backend string, g *ProbeGroup, interval time.Duration) *probePolicy {
	p := &probePolicy{
		cmd:     g.Command,
		backend: backend,
		probe:   buildProbeConfig(g),
	}
	p.tracker = newIdleTracker(interval, func(ctx context.Context) bool {
		return p.runProbe(ctx)
	})
	return p
}

func (p *probePolicy) onHTTPRequest() {
	p.tracker.onHTTPRequest()
}

func (p *probePolicy) consumeIdle(ctx context.Context) bool {
	return p.tracker.consumeIdle(ctx)
}

// runProbe 探测后端，异常则执行 probe.command，返回是否实际执行了命令
func (p *probePolicy) runProbe(ctx context.Context) bool {
	log.Printf("[probe] triggered: backend=%s model=%q prompt=%q maxTokens=%d repeatLimit=%d timeout=%ds",
		p.backend, p.probe.model, p.probe.prompt, p.probe.maxTokens, p.probe.repeatLimit, int(p.probe.timeout.Seconds()))
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

// probeBackend 探测后端是否可用（通过流式 chat completion 判断）
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

// sseEvent 一行 SSE 事件（OpenAI 兼容 chat completion chunk）
type sseEvent struct {
	Choices []struct {
		Delta struct {
			Content string `json:"content"`
		} `json:"delta"`
	} `json:"choices"`
}

// probeBackendStreaming 读取 SSE 流，判断内容是否退化（连续重复 token）
func probeBackendStreaming(body io.Reader, pc probeConfig) (bool, error) {
	scanner := bufio.NewScanner(body)
	scanner.Buffer(make([]byte, 1024*1024), 1024*1024)

	var lastChar rune
	var run int

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
		for _, r := range ev.Choices[0].Delta.Content {
			if r == lastChar {
				run++
			} else {
				lastChar = r
				run = 1
			}
			if run >= pc.repeatLimit {
				return false, fmt.Errorf("degenerate content: %d consecutive repeated char %q", run, lastChar)
			}
		}
	}
	if err := scanner.Err(); err != nil {
		return false, err
	}
	return true, nil
}
