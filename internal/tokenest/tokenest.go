// Package tokenest estimates the prompt token size of a /v1/chat/completions
// request. The count is exact and comes from the local llama.cpp backend's own
// tokenizer (POST /tokenize), so it matches what the local model would actually
// encode. There is no local fallback: when the backend cannot count the text
// (no /tokenize, a failing call, a text too large to send) the estimate is an
// error, and the caller keeps the request local instead of guessing
package tokenest

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/tidwall/gjson"
)

const (
	// tokens attributed to each image part of a multimodal message (the local
	// model has no vision here; the value only affects the routing decision,
	// which the cloud endpoint - always multimodal-capable - would honor)
	defaultImageTokens = 256
	// per-message token budget covering the chat template's role markers
	// (the template itself is not rendered: it differs per model)
	perMessageOverhead = 4
	// bodies larger than this are not tokenized at all (the estimate fails, the
	// request stays local): the round-trip cost would be disproportionate, and
	// such a text already far exceeds any local context
	maxTokenizeBytes = 8 << 20
	tokenizeTimeout  = 10 * time.Second
)

// Estimator estimates the prompt token size of chat completion request bodies
type Estimator struct {
	backend string // the local backend base URL (for /tokenize)
	apiKey  string // the global backend key: key-protected deployments require it on /tokenize too
	client  *http.Client
}

// New builds an estimator for the backend; apiKey is the global backend key
// (sent on the /tokenize requests, empty when the backend has no key)
func New(backend, apiKey string) *Estimator {
	return &Estimator{backend: backend, apiKey: apiKey, client: &http.Client{Timeout: tokenizeTimeout}}
}

// Result is the prompt size estimate of one request body with its breakdown,
// so the caller can reconcile the estimate with the backend's real count
type Result struct {
	Tokens int // the estimated total prompt tokens (text + images + per-message/per-call overhead)
	Text   int // the exact token count of the prompt text from the backend's tokenizer
	Images int // the number of image parts (counted at a fixed allowance, not texted)
	NMsg   int // the number of messages
	Calls  int // the number of tool calls
	Tools  int // the raw byte size of the tools definitions (0 when none)
}

// Estimate returns the prompt token size of one chat completion request body:
// the exact text token count from the backend's tokenizer, plus the image parts
// and the chat template's per-message overhead. It fails - never guesses - when
// the backend cannot count the text. A body without any text yields the fixed
// parts only (0 for a body without messages, which the caller treats as "cannot
// judge, keep it local")
func (e *Estimator) Estimate(ctx context.Context, body []byte) (Result, error) {
	var res Result
	text, images, nMsg, nCalls, toolsBytes := extractPrompt(body)
	res.Images, res.NMsg, res.Calls, res.Tools = images, nMsg, nCalls, toolsBytes
	if len(text) > 0 {
		if len(text) > maxTokenizeBytes {
			return res, fmt.Errorf("prompt text too large for exact tokenization (%d bytes)", len(text))
		}
		n, err := e.tokenize(ctx, text)
		if err != nil {
			return res, err
		}
		res.Text = n
	}
	res.Tokens = res.Text + defaultImageTokens*images + perMessageOverhead*nMsg
	return res, nil
}

// tokenize asks the local llama.cpp backend's POST /tokenize for the exact
// token count of the prompt text; there is no other source for the count
func (e *Estimator) tokenize(ctx context.Context, text string) (int, error) {
	if e.backend == "" {
		return 0, fmt.Errorf("no backend configured for exact tokenization")
	}
	payload, err := json.Marshal(map[string]any{"content": text})
	if err != nil {
		return 0, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, e.backend+"/tokenize", bytes.NewReader(payload))
	if err != nil {
		return 0, err
	}
	req.Header.Set("Content-Type", "application/json")
	if e.apiKey != "" {
		req.Header.Set("Authorization", "Bearer "+e.apiKey)
	}
	resp, err := e.client.Do(req)
	if err != nil {
		return 0, fmt.Errorf("POST /tokenize: %w", err)
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(io.LimitReader(resp.Body, 64<<20))
	if err != nil {
		return 0, err
	}
	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("POST /tokenize: %d", resp.StatusCode)
	}
	tokens := gjson.GetBytes(data, "tokens").Array()
	if len(tokens) == 0 {
		return 0, fmt.Errorf("POST /tokenize: empty token list")
	}
	return len(tokens), nil
}

// extractPrompt pulls the prompt text (in request order), the number of image
// parts, the message count and the raw tools size out of a chat completion
// request body: the top-level system field, each message's reasoning_content
// (thinking models send it back on every later turn) and text content
// (string, or the text parts of an array content), each tool call as its raw
// JSON (not split into name/arguments), and the tools definitions. Image
// parts are counted, not texted
func extractPrompt(body []byte) (text string, images, nMsg, nCalls, toolsBytes int) {
	var sb strings.Builder
	if system := gjson.GetBytes(body, "system"); system.Type == gjson.String {
		sb.WriteString(system.String())
	}
	if tools := gjson.GetBytes(body, "tools"); tools.IsArray() {
		sb.WriteString(tools.Raw)
		toolsBytes = len(tools.Raw)
	}
	messages := gjson.GetBytes(body, "messages").Array()
	nMsg = len(messages)
	for _, m := range messages {
		if rc := m.Get("reasoning_content"); rc.Type == gjson.String {
			sb.WriteString(rc.String())
		}
		switch c := m.Get("content"); c.Type {
		case gjson.String:
			sb.WriteString(c.String())
		case gjson.JSON:
			for _, part := range c.Array() {
				switch part.Get("type").String() {
				case "text":
					sb.WriteString(part.Get("text").String())
				case "image_url", "image":
					images++
				}
			}
		}
		for _, tc := range m.Get("tool_calls").Array() {
			sb.WriteString(tc.Raw)
			nCalls++
		}
	}
	return sb.String(), images, nMsg, nCalls, toolsBytes
}
