package proxy

import (
	"bytes"
	"io"
	"net/http"
	"strconv"
	"strings"

	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"

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
	stream  bool
	partial []byte // stream: the unfinished trailing SSE line
	buf     []byte // non-stream: accumulated body, parsed once at EOF
	done    bool   // observation already published
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
	if err == io.EOF && !b.stream && !b.done {
		b.publish(parseObservation(b.buf))
	}
	return n, err
}

// scan feeds an SSE chunk through the line scanner and publishes the
// observation as soon as the chunk carrying it arrives
func (b *completionTap) scan(chunk []byte) {
	if b.done {
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
		if o, ok := observationFromSSELine(line); ok {
			b.publish(o)
			return
		}
	}
}

// publish delivers the observation to the consumers when it carries data
func (b *completionTap) publish(o observe.Observation) {
	if o == (observe.Observation{}) {
		return
	}
	b.hub.Notify(o)
	b.done = true
	b.partial = nil
	b.buf = nil
}

// Close forwards to the inner body; the http client closes the backend TCP
// connection when the body is closed before full read, so the backend stops
// processing early
func (b *completionTap) Close() error { return b.inner.Close() }

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
