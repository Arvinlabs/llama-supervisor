package stats

import (
	"bytes"
	_ "embed"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"

	"github.com/Arvinlabs/llama-supervisor/internal/config"
)

//go:embed web/stats.html
var pageHTML []byte

// CompletionsPath is the only proxied endpoint the stats policy accounts for:
// the OpenAI-compatible chat completion endpoint
const CompletionsPath = "/v1/chat/completions"

const (
	defaultRetainDays = 7
	dateLayout        = "2006-01-02"
)

// Usage is one completion's token usage as reported by the backend
type Usage struct {
	Prompt     int // prompt tokens (input)
	Cached     int // prompt tokens served from the backend prefix cache (input cache)
	Completion int // generated tokens (output)
	Total      int // prompt + completion
}

// dayStats is the per-day stats document: one JSON file per day in the save
// path, named YYYY-MM-DD.json, holding the day's cumulative token counters
type dayStats struct {
	Date       string `json:"date"`
	Requests   int    `json:"requests"`
	Input      int    `json:"input"`
	InputCache int    `json:"input_cache"`
	Output     int    `json:"output"`
	Total      int    `json:"total"`
}

// Policy accounts the token usage of every /v1/chat/completions request into
// one per-day JSON file. It works on both response shapes:
//   - non-stream: the backend sends the usage object in the JSON response body
//     (always present for non-stream chat completions);
//   - stream: the usage arrives only in the final SSE chunk when
//     stream_options.include_usage is requested — since clients do not
//     necessarily set it, ModifyRequest force-injects it into the outbound
//     request, so the usage chunk is always present (the client simply
//     receives an extra standard usage chunk at the end of the stream).
//
// Every byte of the response passes through to the client unmodified and
// unbuffered; the policy only taps it.
type Policy struct {
	savePath   string
	retainDays int
	mu         sync.Mutex // serializes the read-merge-write of the day file
	lastPurge  string     // local date of the last purge; the dir is re-scanned at most once a day
}

// New builds the policy, creates the save directory and purges expired day
// files. It exits on failure: a configured stats policy must work.
func New(g *config.StatsGroup) *Policy {
	retainDays := g.RetainDays
	if retainDays <= 0 {
		retainDays = defaultRetainDays
	}
	p := &Policy{savePath: g.SavePath, retainDays: retainDays}
	if err := os.MkdirAll(g.SavePath, 0o755); err != nil {
		log.Fatalf("[stats] create save dir %s: %v", g.SavePath, err)
	}
	p.purge()
	return p
}

// ModifyRequest force-injects stream_options.include_usage=true into streaming
// POST /v1/chat/completions JSON bodies so the backend always reports usage;
// everything else passes through unchanged
func (p *Policy) ModifyRequest(r *http.Request) {
	if r.Method != http.MethodPost || r.URL.Path != CompletionsPath {
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
	p.restoreBody(r, out)
}

// restoreBody (re)installs the body on the request after it was drained
func (p *Policy) restoreBody(r *http.Request, body []byte) {
	r.Body = io.NopCloser(bytes.NewReader(body))
	r.ContentLength = int64(len(body))
	r.Header.Set("Content-Length", strconv.Itoa(len(body)))
}

// Wrap wraps a chat completion response body (only called for the chat
// completion endpoint) to tap the usage out of it; the wrapped body is what
// the proxy copies to the client
func (p *Policy) Wrap(res *http.Response) io.ReadCloser {
	stream := strings.HasPrefix(res.Header.Get("Content-Type"), "text/event-stream")
	return &body{inner: res.Body, policy: p, stream: stream}
}

// body passes the response bytes through untouched while scanning them for the
// usage object. In stream mode it runs an SSE line scanner (a line may be split
// across reads); in non-stream mode it accumulates the body and parses it at EOF
type body struct {
	inner  io.ReadCloser
	policy *Policy
	stream bool

	partial []byte // stream: the unfinished trailing SSE line
	buf     []byte // non-stream: accumulated body, parsed once at EOF
	done    bool   // usage already captured
}

func (b *body) Read(p []byte) (int, error) {
	n, err := b.inner.Read(p)
	if n > 0 {
		if b.stream {
			b.scan(p[:n])
		} else {
			b.buf = append(b.buf, p[:n]...)
		}
	}
	if err == io.EOF && !b.stream && !b.done {
		b.capture(parseUsage(b.buf))
	}
	return n, err
}

// scan feeds an SSE chunk through the line scanner and captures the usage as
// soon as the chunk carrying it arrives
func (b *body) scan(chunk []byte) {
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
		if u, ok := usageFromSSELine(line); ok {
			b.capture(u)
			return
		}
	}
}

// capture records the usage when it is present
func (b *body) capture(u Usage) {
	if u == (Usage{}) {
		return
	}
	b.policy.record(u)
	b.done = true
	b.partial = nil // release the scanner state, the body is done with stats
	b.buf = nil
}

// Close forwards to the inner body; the http client closes the backend TCP
// connection when the body is closed before full read, so the backend stops
// processing early
func (b *body) Close() error { return b.inner.Close() }

// usageFromSSELine extracts the usage from one complete SSE line; it reports
// ok only when the line is a `data:` event carrying a non-empty usage object
func usageFromSSELine(line []byte) (Usage, bool) {
	line = bytes.TrimLeft(line, " \t\r\n")
	if !bytes.HasPrefix(line, []byte("data:")) {
		return Usage{}, false
	}
	data := bytes.TrimSpace(line[len("data:"):])
	if len(data) == 0 || bytes.Equal(data, []byte("[DONE]")) {
		return Usage{}, false
	}
	u := parseUsage(data)
	return u, u != (Usage{})
}

// parseUsage extracts the token counters from a chat completion response body
// (or SSE data payload)
func parseUsage(data []byte) Usage {
	u := Usage{
		Prompt:     int(gjson.GetBytes(data, "usage.prompt_tokens").Int()),
		Cached:     int(gjson.GetBytes(data, "usage.prompt_tokens_details.cached_tokens").Int()),
		Completion: int(gjson.GetBytes(data, "usage.completion_tokens").Int()),
		Total:      int(gjson.GetBytes(data, "usage.total_tokens").Int()),
	}
	if u.Total == 0 {
		u.Total = u.Prompt + u.Completion
	}
	return u
}

// record adds one usage to today's day file (read-merge-atomic write); the
// first record after a date change also purges expired day files (at most once a day)
func (p *Policy) record(u Usage) {
	date := time.Now().Format(dateLayout)
	p.mu.Lock()
	defer p.mu.Unlock()

	path := filepath.Join(p.savePath, date+".json")
	d := dayStats{Date: date}
	if data, err := os.ReadFile(path); err == nil {
		if err := json.Unmarshal(data, &d); err != nil {
			log.Printf("[stats] read %s: %v", path, err)
			d = dayStats{Date: date} // a corrupt file is rebuilt from this record on
		}
	} else if !os.IsNotExist(err) {
		log.Printf("[stats] read %s: %v", path, err)
	}
	d.Requests++
	d.Input += u.Prompt
	d.InputCache += u.Cached
	d.Output += u.Completion
	d.Total += u.Total

	data, err := json.MarshalIndent(&d, "", "  ")
	if err == nil {
		data = append(data, '\n')
		tmp := path + ".tmp"
		if werr := os.WriteFile(tmp, data, 0o644); werr != nil {
			log.Printf("[stats] write %s: %v", path, werr)
		} else if rerr := os.Rename(tmp, path); rerr != nil {
			log.Printf("[stats] rename %s: %v", path, rerr)
			_ = os.Remove(tmp)
		}
	}
	if p.lastPurge != date { // at most one directory scan per day
		p.purge()
	}
}

// Days returns all parsed per-day stats, newest day first
func (p *Policy) Days() ([]dayStats, error) {
	entries, err := os.ReadDir(p.savePath)
	if err != nil {
		return nil, err
	}
	var days []dayStats
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".json") {
			continue
		}
		name := strings.TrimSuffix(e.Name(), ".json")
		if _, err := time.Parse(dateLayout, name); err != nil {
			continue // not a day file
		}
		data, err := os.ReadFile(filepath.Join(p.savePath, e.Name()))
		if err != nil {
			return nil, err
		}
		var d dayStats
		if err := json.Unmarshal(data, &d); err != nil {
			return nil, fmt.Errorf("%s: %v", e.Name(), err)
		}
		days = append(days, d)
	}
	sort.Slice(days, func(i, j int) bool { return days[i].Date > days[j].Date })
	return days, nil
}

// Handle serves the embedded stats page on /stats and its JSON data on
// /stats/data; it reports whether the request was one of them (otherwise the
// request is left untouched to be proxied)
func (p *Policy) Handle(w http.ResponseWriter, r *http.Request) bool {
	switch r.URL.Path {
	case "/stats", "/stats/":
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		_, _ = w.Write(pageHTML)
		return true
	case "/stats/data":
		days, err := p.Days()
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return true
		}
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		_ = json.NewEncoder(w).Encode(map[string][]dayStats{"days": days})
		return true
	default:
		return false
	}
}

// purge deletes day files whose date is older than the retention window
func (p *Policy) purge() {
	now := time.Now()
	p.lastPurge = now.Format(dateLayout)
	cutoff := now.AddDate(0, 0, -p.retainDays)
	entries, err := os.ReadDir(p.savePath)
	if err != nil {
		return
	}
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".json") {
			continue
		}
		name := strings.TrimSuffix(e.Name(), ".json")
		day, err := time.Parse(dateLayout, name)
		if err != nil {
			continue // not a day file
		}
		if day.Before(cutoff) {
			path := filepath.Join(p.savePath, e.Name())
			if err := os.Remove(path); err != nil {
				log.Printf("[stats] remove %s: %v", path, err)
			} else {
				log.Printf("[stats] purged %s (older than %d days)", e.Name(), p.retainDays)
			}
		}
	}
}
