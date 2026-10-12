// Package anthropic implements the Anthropic Messages API (POST /v1/messages)
// on top of the supervisor's OpenAI pipeline: the incoming Anthropic request is
// converted to the OpenAI chat completion protocol at the outer edge (via
// relaykit) and proxied to the supervisor itself, so the loopback request runs
// the full, unmodified OpenAI pipeline (auth, request policy, usage injection,
// completion observation, watchdog, debug dumps); the OpenAI response (plain
// JSON or SSE) is converted back to the Anthropic protocol before it reaches
// the client.
package anthropic

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net/http"
	"time"

	"github.com/QuantumNous/new-api/relaykit/dto"
	"github.com/QuantumNous/new-api/relaykit/relayconvert"
	"github.com/QuantumNous/new-api/relaykit/relayconvert/convmeta"
	"github.com/QuantumNous/new-api/relaykit/types"
	"github.com/tidwall/gjson"
)

// messagesPath is the Anthropic Messages endpoint
const messagesPath = "/v1/messages"

// Handler serves POST /v1/messages by converting to the OpenAI chat completion
// protocol and proxying the request to the supervisor itself
type Handler struct {
	selfURL  string            // the supervisor's own loopback base URL
	modelMap map[string]string // optional client model name -> backend model name
	client   *http.Client
}

// New builds the handler; selfURL must be the supervisor's own listen address on
// the loopback interface
func New(selfURL string, modelMap map[string]string) *Handler {
	registerMediaResolver()
	// no client-level timeout: a generation can run arbitrarily long; the request
	// context (canceled on client disconnect) bounds the call
	return &Handler{
		selfURL:  selfURL,
		modelMap: modelMap,
		client:   &http.Client{},
	}
}

// SetSelfURL replaces the self request base URL (tests listen on ephemeral ports)
func (h *Handler) SetSelfURL(u string) {
	h.selfURL = u
}

// Serve reports whether the request is the Anthropic Messages endpoint and, if so,
// serves it; it never falls through to the reverse proxy
func (h *Handler) Serve(w http.ResponseWriter, r *http.Request) bool {
	if r.Method != http.MethodPost || r.URL.Path != messagesPath {
		return false
	}
	start := time.Now()
	rec := &statusRecorder{ResponseWriter: w}
	h.Handle(rec, r)
	if rec.status == 0 {
		rec.status = http.StatusOK
	}
	log.Printf("[access] %s %s %s %d %s", r.RemoteAddr, r.Method, r.URL.RequestURI(), rec.status, time.Since(start).Round(time.Microsecond))
	return true
}

// Handle converts the Anthropic request to the OpenAI chat completion protocol,
// proxies it to the supervisor itself and converts the response back
func (h *Handler) Handle(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	_ = r.Body.Close()
	if err != nil {
		writeError(w, http.StatusBadGateway, "failed to read the request body")
		return
	}
	var claudeReq dto.ClaudeRequest
	if err := json.Unmarshal(body, &claudeReq); err != nil || claudeReq.Model == "" {
		writeError(w, http.StatusBadRequest, "invalid request: a JSON body with a model field is required")
		return
	}
	// optional model name remapping (client names -> backend names)
	originModel := claudeReq.Model
	if m, ok := h.modelMap[originModel]; ok {
		claudeReq.Model = m
	}
	stream := claudeReq.Stream != nil && *claudeReq.Stream
	meta := &convmeta.Values{
		OriginModelName:     originModel,
		UpstreamModelName:   claudeReq.Model,
		ChannelMetaAttached: true,
		IsStream:            stream,
		// the stream state (block indices, usage, terminal flag) is carried here for
		// the lifetime of the stream; the response registry increments the send
		// count itself, so it starts at 0 and the first frame emits message_start
		Options: &convmeta.Options{},
	}
	result, err := relayconvert.ConvertRequest(r.Context(), meta, types.RelayFormatOpenAI, &claudeReq)
	if err != nil {
		log.Printf("[anthropic] request conversion failed: %v", err)
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	for _, d := range result.Diagnostics {
		log.Printf("[anthropic] conversion %s: %s (path: %s)", d.Severity, d.Message, d.Path)
	}
	oaiReq, ok := result.Value.(*dto.GeneralOpenAIRequest)
	if !ok {
		log.Printf("[anthropic] unexpected conversion result type %T", result.Value)
		writeError(w, http.StatusInternalServerError, "internal conversion error")
		return
	}
	out, err := json.Marshal(oaiReq)
	if err != nil {
		log.Printf("[anthropic] marshal OpenAI request: %v", err)
		writeError(w, http.StatusInternalServerError, "internal conversion error")
		return
	}

	// the self request: same host, OpenAI path, the full pipeline runs internally.
	// The client's API key is passed through verbatim, so the request policy of
	// the internal pipeline is the single authentication point
	self, err := http.NewRequestWithContext(r.Context(), http.MethodPost, h.selfURL+"/v1/chat/completions", bytes.NewReader(out))
	if err != nil {
		log.Printf("[anthropic] build self request: %v", err)
		writeError(w, http.StatusInternalServerError, "internal conversion error")
		return
	}
	self.Header.Set("Content-Type", "application/json")
	if k := clientKey(r); k != "" {
		self.Header.Set("Authorization", k)
	}
	resp, err := h.client.Do(self)
	if err != nil {
		if errors.Is(r.Context().Err(), context.Canceled) {
			log.Print("[anthropic] client disconnected before the self request completed")
			return
		}
		log.Printf("[anthropic] self request failed: %v", err)
		writeError(w, http.StatusBadGateway, "self request failed")
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		log.Printf("[anthropic] self request returned %d", resp.StatusCode)
		writeError(w, mapUpstreamStatus(resp.StatusCode), openAIErrorMessage(resp.Body))
		return
	}
	if stream {
		h.serveStream(r.Context(), w, resp.Body, meta)
	} else {
		h.serveSingle(w, resp.Body, meta)
	}
}

// statusRecorder records the response status code for the access log
type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (s *statusRecorder) WriteHeader(code int) {
	s.status = code
	s.ResponseWriter.WriteHeader(code)
}

// clientKey extracts the API key the client presented, in the Authorization
// header form the internal pipeline expects: the Anthropic x-api-key header
// first, else the Authorization header verbatim (empty when the client sent
// neither; then the inner request policy decides)
func clientKey(r *http.Request) string {
	if k := r.Header.Get("x-api-key"); k != "" {
		return "Bearer " + k
	}
	return r.Header.Get("Authorization")
}

// openAIErrorMessage extracts the message field from an OpenAI error body
// ({"error":{"message":...}}); empty when the body carries none
func openAIErrorMessage(body io.Reader) string {
	buf, err := io.ReadAll(io.LimitReader(body, 64*1024))
	if err != nil {
		return ""
	}
	if m := gjson.GetBytes(buf, "error.message").String(); m != "" {
		return m
	}
	return ""
}

// mapUpstreamStatus maps a self request status code to the Anthropic wire status
// (the Anthropic API uses 500 for upstream failures, not 502/503)
func mapUpstreamStatus(status int) int {
	switch status {
	case http.StatusBadRequest,
		http.StatusUnauthorized,
		http.StatusForbidden,
		http.StatusNotFound,
		http.StatusTooManyRequests:
		return status
	default:
		return http.StatusInternalServerError
	}
}
