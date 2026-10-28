// Package tokenest estimates the prompt token size of a /v1/chat/completions
// request. The text count comes from a configurable counter: by default a
// local tiktoken BPE encoding of the OpenAI family (no backend round-trip),
// or the local llama.cpp backend's own tokenizer (POST /tokenize), which
// matches what the local model would actually encode. There is no heuristic
// guess: when the counter cannot count the text (a failing call, a text
// too large to count) the estimate is an error, and the caller keeps the
// request local instead of guessing
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
	tiktoken "github.com/tiktoken-go/tokenizer"
)

const (
	// tokens attributed to each image part of a multimodal message (the local
	// model has no vision here; the value only affects the routing decision,
	// which the cloud endpoint - always multimodal-capable - would honor)
	defaultImageTokens = 256
	// per-message token budget covering the chat template's role markers
	// (the template itself is not rendered: it differs per model)
	perMessageOverhead = 4
	// bodies larger than this are not counted at all (the estimate fails, the
	// request stays local): the cost (the round trip in the backend counter,
	// the CPU in the tiktoken one) would be disproportionate, and such a text
	// already far exceeds any local context
	maxTokenizeBytes = 8 << 20
	tokenizeTimeout  = 10 * time.Second
)

// textCounter counts the tokens of the prompt text; the routing decision
// trusts the count it reports (it is never corrected or guessed)
type textCounter interface {
	Count(ctx context.Context, text string) (int, error)
}

// backendCounter counts the text with the local llama.cpp backend's own
// tokenizer (POST /tokenize); it matches what the local model would actually
// encode and is the default
type backendCounter struct {
	backend string // the local backend base URL
	apiKey  string // the global backend key: key-protected deployments require it on /tokenize too
	client  *http.Client
}

func (b backendCounter) Count(ctx context.Context, text string) (int, error) {
	if b.backend == "" {
		return 0, fmt.Errorf("no backend configured for exact tokenization")
	}
	payload, err := json.Marshal(map[string]any{"content": text})
	if err != nil {
		return 0, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, b.backend+"/tokenize", bytes.NewReader(payload))
	if err != nil {
		return 0, err
	}
	req.Header.Set("Content-Type", "application/json")
	if b.apiKey != "" {
		req.Header.Set("Authorization", "Bearer "+b.apiKey)
	}
	resp, err := b.client.Do(req)
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

// tiktokenCounter counts the text with a local tiktoken BPE encoding
// (github.com/tiktoken-go/tokenizer): no backend round-trip, so the count is
// available while the backend is down. The local model uses its own tokenizer,
// so the count is approximate - good enough for the routing decision, which
// the margin absorbs
type tiktokenCounter struct {
	codec tiktoken.Codec
	err   error // the codec's load error (reported on every Count call)
}

func newTiktokenCounter(model string) *tiktokenCounter {
	c := &tiktokenCounter{}
	enc, err := tiktoken.ForModel(tiktoken.Model(model))
	if err != nil {
		// an unknown or empty model name uses the OpenAI family's default
		// encoding
		enc, err = tiktoken.Get(tiktoken.Cl100kBase)
	}
	c.codec, c.err = enc, err
	return c
}

func (c *tiktokenCounter) Count(_ context.Context, text string) (int, error) {
	if c.err != nil {
		return 0, fmt.Errorf("tiktoken: %w", c.err)
	}
	n, err := c.codec.Count(text)
	if err != nil {
		return 0, fmt.Errorf("tiktoken: %w", err)
	}
	return n, nil
}

// Estimator estimates the prompt token size of chat completion request bodies
type Estimator struct {
	counter textCounter
}

// New builds an estimator that counts the prompt text with the local backend's
// own tokenizer (POST /tokenize); apiKey is the global backend key (sent on
// the /tokenize requests, empty when the backend has no key)
func New(backend, apiKey string) *Estimator {
	return &Estimator{counter: backendCounter{backend: backend, apiKey: apiKey, client: &http.Client{Timeout: tokenizeTimeout}}}
}

// NewTiktoken builds an estimator that counts the prompt text locally with the
// tiktoken BPE encoding of model (e.g. "gpt-4o") - no backend dependency; an
// empty or a name tiktoken does not know uses the cl100k_base encoding
func NewTiktoken(model string) *Estimator {
	return &Estimator{counter: newTiktokenCounter(model)}
}

// CounterName reports the counter that feeds the estimate, for the logs:
// "backend" for the local backend's tokenizer, otherwise the tiktoken encoding
// actually in use (e.g. "tiktoken/cl100k_base" - also when the model name was
// empty or unknown and the default encoding took over)
func (e *Estimator) CounterName() string {
	if c, ok := e.counter.(*tiktokenCounter); ok {
		if c.err != nil {
			return "tiktoken/unavailable"
		}
		return "tiktoken/" + c.codec.GetName()
	}
	return "backend"
}

// Result is the prompt size estimate of one request body with its breakdown,
// so the caller can reconcile the estimate with the backend's real count
type Result struct {
	Tokens int // the estimated total prompt tokens (text + images + per-message/per-call overhead)
	Text   int // the token count of the prompt text from the configured counter
	Images int // the number of image parts (counted at a fixed allowance, not texted)
	NMsg   int // the number of messages
	Calls  int // the number of tool calls
	Tools  int // the raw byte size of the tools definitions (0 when none)
}

// Estimate returns the prompt token size of one chat completion request body:
// the prompt text token count from the configured counter, plus the image
// parts and the chat template's per-message overhead. It fails - never guesses
// - when the counter cannot count the text. A body without any text yields the
// fixed parts only (0 for a body without messages, which the caller treats as
// "cannot judge, keep it local")
func (e *Estimator) Estimate(ctx context.Context, body []byte) (Result, error) {
	var res Result
	text, images, nMsg, nCalls, toolsBytes := extractPrompt(body)
	res.Images, res.NMsg, res.Calls, res.Tools = images, nMsg, nCalls, toolsBytes
	if len(text) > 0 {
		if len(text) > maxTokenizeBytes {
			return res, fmt.Errorf("prompt text too large to count (%d bytes)", len(text))
		}
		n, err := e.counter.Count(ctx, text)
		if err != nil {
			return res, err
		}
		res.Text = n
	}
	res.Tokens = res.Text + defaultImageTokens*images + perMessageOverhead*nMsg
	return res, nil
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
