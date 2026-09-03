package stats

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Arvinlabs/llama-supervisor/internal/config"
)

// newTestPolicy builds a policy over a fresh temp dir with the given retention
func newTestPolicy(t *testing.T, retainDays int) *Policy {
	t.Helper()
	return New(&config.StatsGroup{Enable: true, SavePath: t.TempDir(), RetainDays: retainDays})
}

// todayFile returns today's day file path in the policy's save dir
func todayFile(t *testing.T, p *Policy) string {
	t.Helper()
	return filepath.Join(p.savePath, time.Now().Format(dateLayout)+".json")
}

// readDay reads and unmarshals a day file
func readDay(t *testing.T, path string) dayStats {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read day file: %v", err)
	}
	var d dayStats
	if err := json.Unmarshal(data, &d); err != nil {
		t.Fatalf("parse day file %s: %v", path, err)
	}
	return d
}

// newCompletionsRequest builds a POST /v1/chat/completions request with the given body
func newCompletionsRequest(t *testing.T, body string) *http.Request {
	t.Helper()
	r := httptest.NewRequest(http.MethodPost, "http://backend/v1/chat/completions", strings.NewReader(body))
	r.Header.Set("Content-Type", "application/json")
	return r
}

// a streaming request without usage requested gets stream_options.include_usage injected
func TestModifyRequestInjectsIncludeUsage(t *testing.T) {
	p := newTestPolicy(t, 30)
	r := newCompletionsRequest(t, `{"model":"m","stream":true,"messages":[{"role":"user","content":"hi"}]}`)
	p.ModifyRequest(r)
	data, err := io.ReadAll(r.Body)
	if err != nil {
		t.Fatal(err)
	}
	out := string(data)
	if !strings.Contains(out, `"stream_options":{"include_usage":true}`) {
		t.Fatalf("include_usage not injected: %s", out)
	}
	// the rest of the body is preserved
	if !strings.Contains(out, `"model":"m"`) || !strings.Contains(out, `"stream":true`) {
		t.Fatalf("original body fields lost: %s", out)
	}
	if r.ContentLength != int64(len(data)) || r.Header.Get("Content-Length") != fmt.Sprint(len(data)) {
		t.Fatalf("content length not updated: len=%d contentLength=%d header=%s", len(data), r.ContentLength, r.Header.Get("Content-Length"))
	}
}

// a request that already asks for usage is passed through byte-identical
func TestModifyRequestKeepsExistingIncludeUsage(t *testing.T) {
	p := newTestPolicy(t, 30)
	in := `{"model":"m","stream":true,"stream_options":{"include_usage":true},"messages":[]}`
	r := newCompletionsRequest(t, in)
	p.ModifyRequest(r)
	data, _ := io.ReadAll(r.Body)
	if string(data) != in {
		t.Fatalf("body must be byte-identical:\n%s\n%s", in, data)
	}
}

// non-stream, other paths, GETs and non-JSON bodies pass through unchanged
func TestModifyRequestOnlyStreamsOfCompletions(t *testing.T) {
	p := newTestPolicy(t, 30)
	stream := `{"model":"m","stream":true,"messages":[]}`
	cases := []struct {
		name string
		req  *http.Request
	}{
		{"non-stream", newCompletionsRequest(t, `{"model":"m","stream":false,"messages":[]}`)},
		{"other path", func() *http.Request {
			r := httptest.NewRequest(http.MethodPost, "http://backend/v1/completion", strings.NewReader(stream))
			r.Header.Set("Content-Type", "application/json")
			return r
		}()},
		{"get method", func() *http.Request {
			r := httptest.NewRequest(http.MethodGet, "http://backend/v1/chat/completions", nil)
			r.Header.Set("Content-Type", "application/json")
			return r
		}()},
		{"non-json content type", func() *http.Request {
			r := httptest.NewRequest(http.MethodPost, "http://backend/v1/chat/completions", strings.NewReader(stream))
			r.Header.Set("Content-Type", "text/plain")
			return r
		}()},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			p.ModifyRequest(c.req)
			if c.req.Body == nil {
				return
			}
			data, _ := io.ReadAll(c.req.Body)
			if strings.Contains(string(data), "include_usage") {
				t.Fatalf("include_usage injected into a non-eligible request: %s", data)
			}
		})
	}
}

// the SSE tap: chunks split the usage line arbitrarily across reads, the client
// still receives every byte, and the usage of the final chunk is recorded
func TestStreamUsageTap(t *testing.T) {
	p := newTestPolicy(t, 30)
	// the exact stream shape of a real backend (with the usage chunk and [DONE])
	sse := strings.Join([]string{
		`data: {"choices":[{"delta":{"content":"你好"},"index":0,"finish_reason":null}]}` + `,"id":"x","model":"m","object":"chat.completion.chunk"}` + "\n\n",
		`data: {"choices":[],"id":"x","model":"m","object":"chat.completion.chunk","usage":{"completion_tokens":240,"prompt_tokens":27,"total_tokens":267,"prompt_tokens_details":{"cached_tokens":23}}}` + "\n\n",
		`data: [DONE]` + "\n\n",
	}, "")
	// cut the whole stream into awkward 7-byte reads so lines are split across Reads
	src := strings.NewReader(sse)
	inner := &splitReader{r: src, n: 7}
	res := &http.Response{Header: http.Header{"Content-Type": []string{"text/event-stream"}}, Body: inner}
	b := p.Wrap(res).(*body)

	var got bytes.Buffer
	for {
		buf := make([]byte, 13)
		n, err := b.Read(buf)
		if n > 0 {
			got.Write(buf[:n])
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
	}
	// every byte reached the client, unmodified and in order
	if got.String() != sse {
		t.Fatalf("stream bytes altered:\n%s\n%s", sse, got.String())
	}
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}
	if !inner.closed {
		t.Fatal("Close not forwarded to the inner body")
	}

	d := readDay(t, todayFile(t, p))
	if d.Date != time.Now().Format(dateLayout) || d.Requests != 1 || d.Input != 27 || d.InputCache != 23 || d.Output != 240 || d.Total != 267 {
		t.Fatalf("unexpected day stats: %+v", d)
	}
}

// the non-stream tap: the JSON body is accumulated and parsed at EOF
func TestJSONUsageTap(t *testing.T) {
	p := newTestPolicy(t, 30)
	jsonBody := `{"id":"x","object":"chat.completion","choices":[{"message":{"content":"hi"},"finish_reason":"stop"}],"usage":{"prompt_tokens":10,"completion_tokens":5,"total_tokens":15}}`
	inner := &splitReader{r: strings.NewReader(jsonBody), n: 4}
	res := &http.Response{Header: http.Header{"Content-Type": []string{"application/json"}}, Body: inner}
	b := p.Wrap(res).(*body)

	var got bytes.Buffer
	for {
		buf := make([]byte, 9)
		n, err := b.Read(buf)
		if n > 0 {
			got.Write(buf[:n])
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
	}
	if got.String() != jsonBody {
		t.Fatalf("json bytes altered:\n%s\n%s", jsonBody, got.String())
	}
	d := readDay(t, todayFile(t, p))
	if d.Requests != 1 || d.Input != 10 || d.InputCache != 0 || d.Output != 5 || d.Total != 15 {
		t.Fatalf("unexpected day stats: %+v", d)
	}
}

// the real non-stream response shape as returned by the backend (extra keys like
// timings are ignored, the body passes through byte-identical)
func TestRealNonStreamResponse(t *testing.T) {
	p := newTestPolicy(t, 30)
	payload := `{"choices":[{"finish_reason":"stop","index":0,"message":{"role":"assistant","content":"Hello! How can I help you today?"}}],"created":1788351285,"model":"qwen-local","system_fingerprint":"b10621-c1d0e7a00","object":"chat.completion","usage":{"completion_tokens":10,"prompt_tokens":24,"total_tokens":34,"prompt_tokens_details":{"cached_tokens":0}},"id":"chatcmpl-fUOlfys3hxmQoQB2CDOvLSKN41mnoyqQ","timings":{"cache_n":0,"prompt_n":24,"prompt_ms":719.016,"prompt_per_token_ms":29.959,"prompt_per_second":33.378951233352254,"predicted_n":10,"predicted_ms":154.186,"predicted_per_token_ms":17.131777777777778,"predicted_per_second":58.371058332144294,"draft_n":6,"draft_n_accepted":6}}`
	// small reads to force multi-read accumulation
	res := &http.Response{Header: http.Header{"Content-Type": []string{"application/json; charset=utf-8"}}, Body: io.NopCloser(&splitReader{r: strings.NewReader(payload), n: 64})}
	b := p.Wrap(res).(*body)

	var got bytes.Buffer
	if _, err := io.Copy(&got, b); err != nil {
		t.Fatal(err)
	}
	if got.String() != payload {
		t.Fatalf("body altered for the client")
	}
	d := readDay(t, todayFile(t, p))
	if d.Requests != 1 || d.Input != 24 || d.InputCache != 0 || d.Output != 10 || d.Total != 34 {
		t.Fatalf("unexpected day stats: %+v (want input=24 input_cache=0 output=10 total=34)", d)
	}
}

// records on the same day are merged into one file
func TestRecordMergesSameDay(t *testing.T) {
	p := newTestPolicy(t, 30)
	p.record(Usage{Prompt: 1, Cached: 1, Completion: 2, Total: 3})
	p.record(Usage{Prompt: 10, Cached: 4, Completion: 20, Total: 30})
	d := readDay(t, todayFile(t, p))
	if d.Requests != 2 || d.Input != 11 || d.InputCache != 5 || d.Output != 22 || d.Total != 33 {
		t.Fatalf("day stats not merged: %+v", d)
	}
}

// a stream without any usage chunk (e.g. interrupted before the final chunk) records nothing
func TestStreamWithoutUsageRecordsNothing(t *testing.T) {
	p := newTestPolicy(t, 30)
	sse := "data: {\"choices\":[{\"delta\":{\"content\":\"hi\"}}]}\n\n"
	res := &http.Response{Header: http.Header{"Content-Type": []string{"text/event-stream"}}, Body: io.NopCloser(strings.NewReader(sse))}
	b := p.Wrap(res).(*body)
	io.Copy(io.Discard, b)
	if _, err := os.Stat(todayFile(t, p)); !os.IsNotExist(err) {
		t.Fatalf("a usage-less stream must not create a day file: %v", err)
	}
}

// day files older than the retention window are purged at startup, others kept
func TestPurgeAtStartup(t *testing.T) {
	dir := t.TempDir()
	old := time.Now().AddDate(0, 0, -40).Format(dateLayout)
	keep := time.Now().AddDate(0, 0, -20).Format(dateLayout)
	fresh := time.Now().Format(dateLayout)
	for _, name := range []string{old + ".json", keep + ".json", fresh + ".json", "not-a-date.json"} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte("{}"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	New(&config.StatsGroup{Enable: true, SavePath: dir, RetainDays: 30})
	if _, err := os.Stat(filepath.Join(dir, old+".json")); !os.IsNotExist(err) {
		t.Fatalf("expired day file not purged: %v", err)
	}
	for _, name := range []string{keep + ".json", fresh + ".json", "not-a-date.json"} {
		if _, err := os.Stat(filepath.Join(dir, name)); err != nil {
			t.Fatalf("day file %s must be kept: %v", name, err)
		}
	}
}

// an unset (or non-positive) retention falls back to the 7-day default
func TestDefaultRetainDays(t *testing.T) {
	dir := t.TempDir()
	old := time.Now().AddDate(0, 0, -8).Format(dateLayout)
	keep := time.Now().AddDate(0, 0, -6).Format(dateLayout)
	for _, name := range []string{old + ".json", keep + ".json"} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte("{}"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	p := New(&config.StatsGroup{Enable: true, SavePath: dir})
	if p.retainDays != 7 {
		t.Fatalf("default retention = %d, want 7", p.retainDays)
	}
	if _, err := os.Stat(filepath.Join(dir, old+".json")); !os.IsNotExist(err) {
		t.Fatalf("day file 8 days old must be purged with the default retention: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, keep+".json")); err != nil {
		t.Fatalf("day file 6 days old must be kept: %v", err)
	}
}

// the purge runs at most once a day: an expired file that appears after startup
// is NOT deleted by same-day records (it waits for the next day's first record)
func TestPurgeAtMostOncePerDay(t *testing.T) {
	p := newTestPolicy(t, 30)
	old := time.Now().AddDate(0, 0, -40).Format(dateLayout)
	oldPath := filepath.Join(p.savePath, old+".json")
	if err := os.WriteFile(oldPath, []byte("{}"), 0o644); err != nil {
		t.Fatal(err)
	}
	p.record(Usage{Prompt: 1, Completion: 1, Total: 2})
	if _, err := os.Stat(oldPath); err != nil {
		t.Fatalf("same-day record must not purge: %v", err)
	}
}

// the day file is written as indented JSON with the documented field names
func TestDayFileFormat(t *testing.T) {
	p := newTestPolicy(t, 30)
	p.record(Usage{Prompt: 27, Cached: 23, Completion: 240, Total: 267})
	data, err := os.ReadFile(todayFile(t, p))
	if err != nil {
		t.Fatal(err)
	}
	var m map[string]any
	if err := json.Unmarshal(data, &m); err != nil {
		t.Fatal(err)
	}
	for k := range map[string]int{"date": 1, "requests": 1, "input": 1, "input_cache": 1, "output": 1, "total": 1} {
		if _, ok := m[k]; !ok {
			t.Fatalf("field %q missing: %s", k, data)
		}
	}
	if m["input"].(float64) != 27 || m["input_cache"].(float64) != 23 || m["output"].(float64) != 240 || m["total"].(float64) != 267 {
		t.Fatalf("unexpected counters: %s", data)
	}
	if !strings.Contains(string(data), "\n  ") {
		t.Fatalf("day file must be indented JSON: %s", data)
	}
}

// usageFromSSELine edge cases
func TestUsageFromSSELine(t *testing.T) {
	usageLine := "data: {\"usage\":{\"prompt_tokens\":2,\"completion_tokens\":3,\"total_tokens\":5}}"
	if u, ok := usageFromSSELine([]byte(usageLine)); !ok || u.Prompt != 2 || u.Completion != 3 || u.Total != 5 {
		t.Fatalf("usage line: ok=%v u=%+v", ok, u)
	}
	for _, line := range []string{
		"data: [DONE]",
		"data:",
		"data:   ",
		": keep-alive",
		"data: {\"choices\":[{\"delta\":{\"content\":\"x\"}}]}",
		"",
	} {
		if _, ok := usageFromSSELine([]byte(line)); ok {
			t.Fatalf("line must not yield usage: %q", line)
		}
	}
}

// a missing total_tokens falls back to prompt + completion
func TestParseUsageTotalFallback(t *testing.T) {
	u := parseUsage([]byte(`{"usage":{"prompt_tokens":4,"completion_tokens":6}}`))
	if u.Total != 10 {
		t.Fatalf("total must fall back to prompt+completion: %+v", u)
	}
}

// Handle serves the embedded page on /stats and the JSON data on /stats/data
func TestHandleServesPageAndData(t *testing.T) {
	p := newTestPolicy(t, 30)
	p.record(Usage{Prompt: 27, Cached: 23, Completion: 240, Total: 267})

	// the data endpoint
	w := httptest.NewRecorder()
	if !p.Handle(w, httptest.NewRequest(http.MethodGet, "http://s/stats/data", nil)) {
		t.Fatal("/stats/data must be handled")
	}
	if ct := w.Header().Get("Content-Type"); !strings.HasPrefix(ct, "application/json") {
		t.Fatalf("data content type = %q", ct)
	}
	var resp struct {
		Days []dayStats `json:"days"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}
	if len(resp.Days) != 1 {
		t.Fatalf("want 1 day, got %d: %s", len(resp.Days), w.Body.String())
	}
	d := resp.Days[0]
	if d.Date != time.Now().Format(dateLayout) || d.Requests != 1 || d.Input != 27 || d.InputCache != 23 || d.Output != 240 || d.Total != 267 {
		t.Fatalf("unexpected day: %+v", d)
	}

	// the page endpoint
	w = httptest.NewRecorder()
	if !p.Handle(w, httptest.NewRequest(http.MethodGet, "http://s/stats", nil)) {
		t.Fatal("/stats must be handled")
	}
	if ct := w.Header().Get("Content-Type"); !strings.HasPrefix(ct, "text/html") {
		t.Fatalf("page content type = %q", ct)
	}
	if !strings.Contains(w.Body.String(), "llama-supervisor") || !strings.Contains(w.Body.String(), "/stats/data") {
		t.Fatalf("page content unexpected: %q", w.Body.String())
	}

	// the page must not trigger a favicon request from the browser
	if !strings.Contains(w.Body.String(), `<link rel="icon" href="data:,">`) {
		t.Fatalf("page must carry an inline icon: %q", w.Body.String())
	}

	// any other path is left untouched (to be proxied)
	if p.Handle(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "http://s/other", nil)) {
		t.Fatal("other paths must not be handled")
	}
}

// Days returns the parsed day files newest first and skips non-day files
func TestDaysNewestFirst(t *testing.T) {
	p := newTestPolicy(t, 30)
	for _, off := range []int{-3, -1, 0} {
		date := time.Now().AddDate(0, 0, off).Format(dateLayout)
		d := dayStats{Date: date, Requests: 1, Input: 100, InputCache: 40, Output: 200, Total: 300}
		data, _ := json.Marshal(&d)
		if err := os.WriteFile(filepath.Join(p.savePath, date+".json"), data, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(p.savePath, "junk.json"), []byte(`{"date":"not-a-date"}`), 0o644); err != nil {
		t.Fatal(err)
	}
	days, err := p.Days()
	if err != nil {
		t.Fatal(err)
	}
	if len(days) != 3 {
		t.Fatalf("want 3 days, got %d", len(days))
	}
	for i := 1; i < len(days); i++ {
		if days[i-1].Date <= days[i].Date {
			t.Fatalf("days not sorted newest first: %v", days)
		}
	}
}

// splitReader reads at most n bytes per Read (forces line-splitting) and records Close
type splitReader struct {
	r      *strings.Reader
	n      int
	closed bool
}

func (s *splitReader) Read(p []byte) (int, error) {
	if len(p) > s.n {
		p = p[:s.n]
	}
	return s.r.Read(p)
}

func (s *splitReader) Close() error {
	s.closed = true
	return nil
}
