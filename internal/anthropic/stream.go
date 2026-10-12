package anthropic

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"

	"github.com/QuantumNous/new-api/relaykit/dto"
	"github.com/QuantumNous/new-api/relaykit/relayconvert"
	"github.com/QuantumNous/new-api/relaykit/relayconvert/convmeta"
	"github.com/QuantumNous/new-api/relaykit/types"
	"github.com/tidwall/gjson"
)

// injectedErrorField detects the OpenAI-format stream error event the pipeline
// injects when a stream ends abnormally ({"error":{...}} with no choices); a
// genuine chat completion chunk never carries a top-level error field
const injectedErrorField = "error"

// serveStream converts the OpenAI SSE stream from the self request into the
// Anthropic SSE event stream and writes it to the client as it arrives
func (h *Handler) serveStream(ctx context.Context, w http.ResponseWriter, body io.Reader, meta *convmeta.Values) {
	state, err := relayconvert.NewResponseStreamState(
		types.RelayFormatOpenAI, types.RelayFormatClaude, relayconvert.ResponseStreamOptions{})
	if err != nil {
		log.Printf("[anthropic] build stream state: %v", err)
		writeError(w, http.StatusInternalServerError, "internal conversion error")
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.WriteHeader(http.StatusOK)
	sc := bufio.NewScanner(body)
	sc.Buffer(make([]byte, 64*1024), 8*1024*1024) // tool arguments and long lines
	for sc.Scan() {
		data, ok := sseDataLine(sc.Bytes())
		if !ok {
			continue
		}
		if bytes.Equal(data, []byte("[DONE]")) {
			break
		}
		// the pipeline's injected stream error event: relay it as an Anthropic
		// error event and end the stream
		if gjson.GetBytes(data, injectedErrorField).Exists() {
			log.Print("[anthropic] relaying injected stream error event to the client")
			writeAnthropicStreamError(w, "stream interrupted")
			return
		}
		var chunk dto.ChatCompletionsStreamResponse
		if err := json.Unmarshal(data, &chunk); err != nil {
			log.Printf("[anthropic] skip unparseable SSE payload: %v", err)
			continue
		}
		results, err := relayconvert.ConvertStreamResponseChunk(ctx, meta, state, &chunk)
		if err != nil {
			log.Printf("[anthropic] convert stream chunk: %v", err)
			continue
		}
		for _, res := range results {
			evt, ok := res.Value.(*dto.ClaudeResponse)
			if !ok {
				continue
			}
			if err := writeClaudeEvent(w, evt); err != nil {
				return // the client left; the request ctx aborts the self request
			}
		}
	}
	if err := sc.Err(); err != nil {
		// the self stream broke without the injected error event (client
		// disconnect or a connection reset before the guard could inject)
		log.Printf("[anthropic] self stream read failed: %v", err)
		return
	}
	// the upstream ended without a terminal message_stop (an early [DONE] with
	// no final usage chunk, ...): close the stream so the client never hangs
	results, err := relayconvert.FinalizeStreamResponse(ctx, meta, state)
	if err != nil {
		log.Printf("[anthropic] finalize stream: %v", err)
		return
	}
	for _, res := range results {
		evt, ok := res.Value.(*dto.ClaudeResponse)
		if !ok {
			continue
		}
		if err := writeClaudeEvent(w, evt); err != nil {
			return
		}
	}
}

// serveSingle converts a non-streaming OpenAI JSON response to the Anthropic
// message response
func (h *Handler) serveSingle(w http.ResponseWriter, body io.Reader, meta *convmeta.Values) {
	buf, err := io.ReadAll(body)
	if err != nil {
		log.Printf("[anthropic] read self response: %v", err)
		writeError(w, http.StatusBadGateway, "failed to read the response")
		return
	}
	var oaiResp dto.OpenAITextResponse
	if err := json.Unmarshal(buf, &oaiResp); err != nil {
		log.Printf("[anthropic] the self response is not a chat completion JSON: %v", err)
		writeError(w, http.StatusBadGateway, "the backend returned a non-JSON response")
		return
	}
	resp := relayconvert.ResponseOpenAI2Claude(&oaiResp, meta)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	enc := json.NewEncoder(w)
	enc.SetEscapeHTML(false)
	_ = enc.Encode(resp)
}

// sseDataLine extracts the payload of one SSE "data:" line; it reports ok false
// for comments, event names and blank lines
func sseDataLine(line []byte) ([]byte, bool) {
	line = bytes.TrimLeft(line, " \t\r\n")
	if !bytes.HasPrefix(line, []byte("data:")) {
		return nil, false
	}
	return bytes.TrimSpace(line[len("data:"):]), true
}

// writeClaudeEvent writes one Anthropic SSE event ("event: <type>" + the JSON
// payload) and flushes it to the client
func writeClaudeEvent(w http.ResponseWriter, evt *dto.ClaudeResponse) error {
	payload, err := json.Marshal(evt)
	if err != nil {
		return err
	}
	if _, err := fmt.Fprintf(w, "event: %s\ndata: %s\n\n", evt.Type, payload); err != nil {
		return err
	}
	if f, ok := w.(http.Flusher); ok {
		f.Flush()
	}
	return nil
}

// writeAnthropicStreamError writes the Anthropic SSE error event used to relay
// an aborted stream
func writeAnthropicStreamError(w http.ResponseWriter, message string) {
	payload, _ := json.Marshal(map[string]any{
		"type": "error",
		"error": map[string]string{
			"type":    "api_error",
			"message": message,
		},
	})
	_, _ = fmt.Fprintf(w, "event: error\ndata: %s\n\n", payload)
	if f, ok := w.(http.Flusher); ok {
		f.Flush()
	}
}
