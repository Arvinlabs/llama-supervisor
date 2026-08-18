package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"log"
	"net/http"
	"strings"
	"time"
)

// probeConfig 后端健康探测配置
type probeConfig struct {
	backend     string // 后端地址 host:port
	apiKey      string // 后端 api key（Authorization: Bearer）
	model       string
	prompt      string
	maxTokens   int
	repeatLimit int // 末尾同一字符连续出现达到该长度即判定异常
	timeout     time.Duration
}

// probeBackend 流式调用后端 /v1/chat/completions 探测服务是否异常。
// 返回 true 表示后端异常（生成内容末尾字符持续重复，或探测失败）。
func probeBackend(ctx context.Context, p probeConfig) bool {
	pctx, cancel := context.WithTimeout(ctx, p.timeout)
	defer cancel()

	reqBody, err := json.Marshal(map[string]interface{}{
		"model":       p.model,
		"messages":    []map[string]string{{"role": "user", "content": p.prompt}},
		"stream":      true,
		"max_tokens":  p.maxTokens,
		"temperature": 0,
	})
	if err != nil {
		log.Printf("[health] build request failed: %v", err)
		return true
	}

	req, err := http.NewRequestWithContext(pctx, http.MethodPost,
		"http://"+p.backend+"/v1/chat/completions", bytes.NewReader(reqBody))
	if err != nil {
		log.Printf("[health] build request failed: %v", err)
		return true
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "text/event-stream")
	if p.apiKey != "" {
		req.Header.Set("Authorization", "Bearer "+p.apiKey)
	}

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		log.Printf("[health] probe request failed (treat as abnormal): %v", err)
		return true
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		log.Printf("[health] probe returned status %s (treat as abnormal)", resp.Status)
		return true
	}

	// 解析 SSE 流，流式过程中逐 token 检测末尾字符是否持续重复，达到阈值立即终止
	var content strings.Builder
	var lastChar rune
	var run int
	scanner := bufio.NewScanner(resp.Body)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if !strings.HasPrefix(line, "data:") {
			continue
		}
		data := strings.TrimSpace(strings.TrimPrefix(line, "data:"))
		if data == "[DONE]" {
			break
		}
		var chunk struct {
			Choices []struct {
				Delta struct {
					Content string `json:"content"`
				} `json:"delta"`
			} `json:"choices"`
		}
		if err := json.Unmarshal([]byte(data), &chunk); err != nil {
			continue
		}
		if len(chunk.Choices) == 0 {
			continue
		}
		for _, c := range chunk.Choices[0].Delta.Content {
			content.WriteRune(c)
			if c == lastChar {
				run++
			} else {
				lastChar, run = c, 1
			}
			// 末尾字符持续重复，说明 llama server 已异常（卡死/重复输出），提前终止
			if run >= p.repeatLimit {
				log.Printf("[health] abnormal: trailing char %q repeated %d times (limit %d), abort stream: %q",
					c, run, p.repeatLimit, content.String())
				return true
			}
		}
	}
	if err := scanner.Err(); err != nil && err != context.Canceled && err != context.DeadlineExceeded {
		log.Printf("[health] probe read stream failed: %v", err)
		return true
	}

	log.Printf("[health] probe content=%q trailing repeat=%d limit=%d", content.String(), run, p.repeatLimit)
	return false
}
