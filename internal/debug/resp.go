package debug

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// TapResponse dumps the backend response to a JSON file named by the response time
// (YYYYMMDD_HHMMSS.mmm.json) under the configured RespSavePath, for later inspection and
// replay. It is a no-op when RespSavePath is not set or the response has no body. The dump
// carries the status and the response headers; the body is buffered and written when the
// body is closed (the proxy closes it after forwarding to the client):
//   - a streaming (text/event-stream) chat completion is assembled into the non-streaming
//     structure (the per-chunk deltas merged into one message, usage/timings taken from the
//     final chunk), so streaming and non-streaming responses dump in the same shape; a
//     stream that does not look like a chat completion falls back to the JSON array of the
//     SSE data events (each data line: parsed as JSON when valid, kept as the raw string
//     otherwise, e.g. [DONE]),
//   - a non-streaming body is stored as the parsed JSON when valid, base64-encoded otherwise.
//
// In either case the dump stays a single JSON document.
// The wrapped body passes every byte through untouched, so the client sees the response
// exactly as the backend sent it. An early close on a canceled request leaves a partial
// (but still valid) JSON dump, which is expected. Failures are logged but never affect the
// caller.
func (p *Policy) TapResponse(res *http.Response) {
	if p.respSavePath == "" || res.Body == nil {
		return
	}
	name := time.Now().Format("20060102_150405.000") + ".json"
	if err := os.MkdirAll(p.respSavePath, 0o755); err != nil {
		log.Printf("[debug] save response: mkdir %s: %v", p.respSavePath, err)
		return
	}
	f, err := os.Create(filepath.Join(p.respSavePath, name))
	if err != nil {
		log.Printf("[debug] save response: create %s: %v", name, err)
		return
	}
	res.Body = &teeResponse{
		body:   res.Body,
		f:      f,
		path:   filepath.Join(p.respSavePath, name),
		status: res.Status,
		header: res.Header.Clone(),
		stream: strings.HasPrefix(res.Header.Get("Content-Type"), "text/event-stream"),
	}
}

// teeResponse passes the wrapped body through and, when it is closed, writes the dump:
// the status, the headers and the buffered body - the body pretty-printed if it is valid
// JSON, the SSE events as a JSON array when streaming, base64-encoded otherwise
type teeResponse struct {
	body   io.ReadCloser
	f      *os.File
	path   string // full path of the dump file, used in the log line
	status string // the response status line (e.g. "200 OK")
	header http.Header
	stream bool // whether the body is an SSE stream (judged from the Content-Type)
	buf    bytes.Buffer
	closed bool // guard: the proxy may close the body more than once
}

func (b *teeResponse) Read(p []byte) (int, error) {
	n, err := b.body.Read(p)
	if n > 0 {
		_, _ = b.buf.Write(p[:n])
	}
	return n, err
}

func (b *teeResponse) Close() error {
	if b.closed {
		return nil
	}
	b.closed = true
	// one log line per saved response: success or the failure that stopped it
	if err := b.dump(); err != nil {
		log.Printf("[debug] save response: write %s: %v", b.path, err)
	} else {
		log.Printf("[debug] saved response to %s", b.path)
	}
	err := b.body.Close()
	if cerr := b.f.Close(); err == nil {
		err = cerr
	}
	return err
}

// responseDump is the on-disk dump: the status line, the headers, and the body - the
// non-streaming structure assembled from the stream chunks when streaming (or the raw SSE
// events when the stream is not a chat completion), the parsed JSON body otherwise
type responseDump struct {
	Status  string      `json:"status"`
	Header  http.Header `json:"headers"`
	Stream  bool        `json:"stream"`
	Body    any         `json:"body,omitempty"`       // the parsed JSON body (stream: assembled)
	Events  []any       `json:"events,omitempty"`     // streaming, non-completion: one entry per SSE data line
	BodyB64 string      `json:"bodyBase64,omitempty"` // non-streaming body that is not valid JSON
}

// dump writes the buffered body to the file as a single pretty-printed JSON document
func (b *teeResponse) dump() error {
	dump := responseDump{Status: b.status, Header: b.header, Stream: b.stream}
	data := b.buf.Bytes()
	switch {
	case b.stream:
		events := parseSSEEvents(data)
		if assembled, ok := assembleCompletion(events); ok {
			dump.Body = assembled
		} else {
			dump.Events = events // not a chat completion stream (or no chunks at all): keep the raw events
		}
	case len(data) > 0 && json.Valid(data):
		if err := json.Unmarshal(data, &dump.Body); err != nil {
			dump.BodyB64 = base64.StdEncoding.EncodeToString(data)
		}
	default:
		if len(data) > 0 {
			dump.BodyB64 = base64.StdEncoding.EncodeToString(data)
		}
	}
	enc := json.NewEncoder(b.f)
	enc.SetIndent("", "  ")
	return enc.Encode(dump)
}

// assembleCompletion merges the SSE chunks of a streaming chat completion into the
// non-streaming structure: the envelope (id/created/model/system_fingerprint) comes from
// the first chunk, each choice's delta fields (role/content/reasoning_content) are
// concatenated into one message and its finish_reason taken from the terminating chunk,
// and usage/timings come from the final usage chunk. The stream must carry at least one
// chunk with at least one choice, otherwise (false) is returned and the caller keeps the
// raw events
func assembleCompletion(events []any) (any, bool) {
	type mergedChoice struct {
		message   map[string]any
		finish    any
		hasFinish bool
	}
	var (
		envelope    = map[string]any{}
		envelopeSet bool
		choices     = map[int]*mergedChoice{}
		order       []int
		usage       any
		timings     any
		chunkCount  int
	)
	for _, e := range events {
		chunk, ok := e.(map[string]any)
		if !ok {
			continue
		}
		chunkCount++
		if !envelopeSet {
			for _, k := range []string{"id", "created", "model", "system_fingerprint"} {
				if v, ok := chunk[k]; ok {
					envelope[k] = v
				}
			}
			envelopeSet = true
		}
		if v, ok := chunk["usage"]; ok {
			usage = v
		}
		if v, ok := chunk["timings"]; ok {
			timings = v
		}
		list, _ := chunk["choices"].([]any)
		for _, c := range list {
			cm, ok := c.(map[string]any)
			if !ok {
				continue
			}
			idx, _ := cm["index"].(float64)
			m, exists := choices[int(idx)]
			if !exists {
				m = &mergedChoice{message: map[string]any{}}
				choices[int(idx)] = m
				order = append(order, int(idx))
			}
			delta, _ := cm["delta"].(map[string]any)
			for _, k := range []string{"role", "content", "reasoning_content"} {
				if s, ok := delta[k].(string); ok && s != "" {
					if cur, ok := m.message[k]; ok {
						m.message[k] = fmt.Sprint(cur) + s
					} else {
						m.message[k] = s
					}
				}
			}
			if fr, ok := cm["finish_reason"]; ok && fr != nil {
				m.finish, m.hasFinish = fr, true
			}
		}
	}
	if chunkCount == 0 || len(order) == 0 {
		return nil, false
	}
	out := map[string]any{"object": "chat.completion"}
	for k, v := range envelope {
		out[k] = v
	}
	merged := make([]any, 0, len(order))
	for _, i := range order {
		m := choices[i]
		c := map[string]any{"index": float64(i), "message": m.message}
		if m.hasFinish {
			c["finish_reason"] = m.finish
		}
		merged = append(merged, c)
	}
	out["choices"] = merged
	if usage != nil {
		out["usage"] = usage
	}
	if timings != nil {
		out["timings"] = timings
	}
	return out, true
}

// parseSSEEvents parses the buffered SSE body into the list of data events: each "data:"
// line yields one entry - parsed as JSON when the payload is valid JSON, kept as the raw
// string otherwise (e.g. the terminal [DONE]); the event's non-data fields (id:, event:,
// comment lines) are not part of a chat completion stream and are skipped
func parseSSEEvents(data []byte) []any {
	var events []any
	for _, line := range strings.Split(string(data), "\n") {
		line = strings.TrimSuffix(line, "\r")
		if !strings.HasPrefix(line, "data:") {
			continue
		}
		payload := strings.TrimSpace(strings.TrimPrefix(line, "data:"))
		if payload == "" {
			continue
		}
		if json.Valid([]byte(payload)) {
			var v any
			if err := json.Unmarshal([]byte(payload), &v); err == nil {
				events = append(events, v)
				continue
			}
		}
		events = append(events, payload)
	}
	return events
}
