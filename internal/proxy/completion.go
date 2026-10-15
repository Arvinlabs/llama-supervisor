package proxy

import (
	"bytes"
	"io"
	"net/http"
	"strconv"
	"strings"

	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"

	"github.com/Arvinlabs/llama-supervisor/internal/errguard"
	"github.com/Arvinlabs/llama-supervisor/internal/observe"
)

// completionsPath is the OpenAI-compatible chat completion endpoint: the only
// proxied path the completion tap observes, and the only one eligible for the
// injected SSE error event.
const completionsPath = "/v1/chat/completions"

// injectUsage force-injects stream_options.include_usage=true into streaming
// POST chat completion JSON bodies so the backend always reports usage (and the
// timings that carry the MTP draft stats); everything else passes through
// unchanged.
func injectUsage(r *http.Request) {
	if r.Method != http.MethodPost || r.URL.Path != completionsPath {
		return
	}
	if mt := r.Header.Get("Content-Type"); !strings.HasPrefix(mt, "application/json") {
		return
	}
	body, err := io.ReadAll(r.Body)
	_ = r.Body.Close()
	if err != nil {
		return
	}
	out := body
	if len(bytes.TrimSpace(body)) > 0 && gjson.GetBytes(body, "stream").Bool() &&
		!gjson.GetBytes(body, "stream_options.include_usage").Bool() { // the client already asked for usage
		if injected, ierr := sjson.SetBytes(body, "stream_options.include_usage", true); ierr == nil {
			out = injected
		}
	}
	r.Body = io.NopCloser(bytes.NewReader(out))
	r.ContentLength = int64(len(out))
	r.Header.Set("Content-Length", strconv.Itoa(len(out)))
}

// completionTap passes the chat completion response bytes through untouched
// while parsing the completion's Observation out of them. In stream mode it
// runs an SSE line scanner (a line may be split across reads); in non-stream
// mode it accumulates the body and parses it at EOF.
type completionTap struct {
	inner   io.ReadCloser
	hub     *observe.Hub
	guard   *errguard.Policy // error guard (nil when disabled): a backend error payload found in the response is reported to it
	stream  bool
	id      int    // the hub stream id of this in-flight completion (stream consumers, e.g. the watchdog over-speed judgment)
	partial []byte // stream: the unfinished trailing SSE line
	buf     []byte // non-stream: accumulated body, parsed once at EOF
	done    bool   // observation already published
	eofDone bool   // non-stream: the EOF body already parsed (observation and/or error)
}

func (b *completionTap) Read(p []byte) (int, error) {
	n, err := b.inner.Read(p)
	if n > 0 {
		if b.stream {
			b.scan(p[:n])
		} else {
			b.buf = append(b.buf, p[:n]...)
		}
	}
	if err == io.EOF && !b.stream && !b.eofDone {
		b.eofDone = true
		b.publish(parseObservation(b.buf))
		if e, ok := parseError(b.buf); ok {
			b.reportError(e)
		}
		b.buf = nil
	}
	return n, err
}

// scan feeds an SSE chunk through the line scanner and publishes the
// observation as soon as the chunk carrying it arrives
func (b *completionTap) scan(chunk []byte) {
	// with an error guard attached the scan keeps running past the usage chunk so a
	// trailing error event is still seen; without one it stops once the observation is out
	if b.done && b.guard == nil {
		return
	}
	b.partial = append(b.partial, chunk...)
	for {
		i := bytes.IndexByte(b.partial, '\n')
		if i < 0 {
			return
		}
		line := b.partial[:i+1]
		b.partial = b.partial[i+1:]
		// a backend error event (data: {"error":{...}}): report it to the guard
		if e, ok := errorFromSSELine(line); ok {
			b.reportError(e)
		}
		// feed the generated content to the stream consumers (the watchdog over-speed judgment)
		if s := contentFromSSELine(line); s != "" {
			b.hub.StreamContent(b.id, s)
		}
		if !b.done {
			if o, ok := observationFromSSELine(line); ok {
				b.publish(o)
			}
		}
	}
}

// reportError hands a detected error object to the guard, but ignores the supervisor's
// own injected stream-interruption event (tagged injectedErrorCode): that event is the
// supervisor synthesizing an abnormal stream end, not a backend error payload
func (b *completionTap) reportError(e observe.ErrorInfo) {
	if b.guard != nil && e.Code != injectedErrorCode {
		b.guard.OnBackendError(e)
	}
}

// publish delivers the observation to the consumers when it carries data
func (b *completionTap) publish(o observe.Observation) {
	if o == (observe.Observation{}) {
		return
	}
	b.hub.Notify(o)
	b.done = true
	// the stream buffer is only released once the scan is guaranteed to stop (no guard
	// still waiting for a trailing error event); the non-stream buf is released at EOF
	if b.guard == nil {
		b.partial = nil
	}
}

// Close unregisters this completion from the stream consumers and forwards to the inner
// body; the http client closes the backend TCP connection when the body is closed before
// full read, so the backend stops processing early
func (b *completionTap) Close() error {
	b.hub.StreamEnd(b.id)
	return b.inner.Close()
}

// contentFromSSELine extracts the generated content (content and reasoning_content) from
// one complete SSE line; empty for non-data lines and the [DONE] terminator
func contentFromSSELine(line []byte) string {
	line = bytes.TrimLeft(line, " \t\r\n")
	if !bytes.HasPrefix(line, []byte("data:")) {
		return ""
	}
	data := bytes.TrimSpace(line[len("data:"):])
	if len(data) == 0 || bytes.Equal(data, []byte("[DONE]")) {
		return ""
	}
	return gjson.GetBytes(data, "choices.0.delta.reasoning_content").String() +
		gjson.GetBytes(data, "choices.0.delta.content").String()
}

// observationFromSSELine extracts an Observation from one complete SSE line; it
// reports ok only when the line is a `data:` event carrying completion data
func observationFromSSELine(line []byte) (observe.Observation, bool) {
	line = bytes.TrimLeft(line, " \t\r\n")
	if !bytes.HasPrefix(line, []byte("data:")) {
		return observe.Observation{}, false
	}
	data := bytes.TrimSpace(line[len("data:"):])
	if len(data) == 0 || bytes.Equal(data, []byte("[DONE]")) {
		return observe.Observation{}, false
	}
	o := parseObservation(data)
	return o, o != (observe.Observation{})
}

// parseObservation extracts the token and MTP draft counters from a chat
// completion response body (or SSE data payload). timings is a sibling of usage
// at the top level of the payload (present for both stream and non-stream).
func parseObservation(data []byte) observe.Observation {
	o := observe.Observation{
		Prompt:        int(gjson.GetBytes(data, "usage.prompt_tokens").Int()),
		Cached:        int(gjson.GetBytes(data, "usage.prompt_tokens_details.cached_tokens").Int()),
		Completion:    int(gjson.GetBytes(data, "usage.completion_tokens").Int()),
		Total:         int(gjson.GetBytes(data, "usage.total_tokens").Int()),
		DraftN:        int(gjson.GetBytes(data, "timings.draft_n").Int()),
		DraftAccepted: int(gjson.GetBytes(data, "timings.draft_n_accepted").Int()),
	}
	if o.Total == 0 {
		o.Total = o.Prompt + o.Completion
	}
	return o
}

// errorFromSSELine extracts an ErrorInfo from one complete SSE data line that carries
// an OpenAI-style error object; it reports ok only when the line is a `data:` event
// (not [DONE]) whose payload has a top-level "error" object
func errorFromSSELine(line []byte) (observe.ErrorInfo, bool) {
	line = bytes.TrimLeft(line, " \t\r\n")
	if !bytes.HasPrefix(line, []byte("data:")) {
		return observe.ErrorInfo{}, false
	}
	data := bytes.TrimSpace(line[len("data:"):])
	if len(data) == 0 || bytes.Equal(data, []byte("[DONE]")) {
		return observe.ErrorInfo{}, false
	}
	return parseError(data)
}

// parseError extracts the type/message/code of an OpenAI-style top-level "error"
// object from a JSON payload (a stream data payload or a non-stream response body).
// It reports ok only when an error object with a message or a type is present, so a
// normal completion body (no "error" key) yields nothing
func parseError(data []byte) (observe.ErrorInfo, bool) {
	// fast path: skip the gjson scan for payloads with no "error" key (the common case)
	if !bytes.Contains(data, []byte(`"error"`)) {
		return observe.ErrorInfo{}, false
	}
	errObj := gjson.GetBytes(data, "error")
	if !errObj.Exists() || !errObj.IsObject() {
		return observe.ErrorInfo{}, false
	}
	e := observe.ErrorInfo{
		Type:    errObj.Get("type").String(),
		Message: errObj.Get("message").String(),
		Code:    errObj.Get("code").String(),
	}
	if e.Message == "" && e.Type == "" {
		return observe.ErrorInfo{}, false
	}
	return e, true
}
