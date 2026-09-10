package stats

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Arvinlabs/llama-supervisor/internal/config"
	"github.com/Arvinlabs/llama-supervisor/internal/observe"
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

// OnCompletion (the consumer entry point) accounts a completion with usage
func TestOnCompletionRecords(t *testing.T) {
	p := newTestPolicy(t, 30)
	p.OnCompletion(observe.Observation{Prompt: 27, Cached: 23, Completion: 240, Total: 267, DraftN: 10, DraftAccepted: 5})
	d := readDay(t, todayFile(t, p))
	if d.Requests != 1 || d.Input != 27 || d.InputCache != 23 || d.Output != 240 || d.Total != 267 {
		t.Fatalf("unexpected day stats: %+v", d)
	}
}

// an observation with no token usage (e.g. draft-only) is skipped, so the
// request counter is not inflated
func TestOnCompletionSkipsNoUsage(t *testing.T) {
	p := newTestPolicy(t, 30)
	p.OnCompletion(observe.Observation{DraftN: 10, DraftAccepted: 5})
	if _, err := os.Stat(todayFile(t, p)); !os.IsNotExist(err) {
		t.Fatalf("a no-usage observation must not create a day file: %v", err)
	}
}

// records on the same day are merged into one file
func TestRecordMergesSameDay(t *testing.T) {
	p := newTestPolicy(t, 30)
	p.record(observe.Observation{Prompt: 1, Cached: 1, Completion: 2, Total: 3})
	p.record(observe.Observation{Prompt: 10, Cached: 4, Completion: 20, Total: 30})
	d := readDay(t, todayFile(t, p))
	if d.Requests != 2 || d.Input != 11 || d.InputCache != 5 || d.Output != 22 || d.Total != 33 {
		t.Fatalf("day stats not merged: %+v", d)
	}
}

// records are also accumulated into the current hour-of-day bucket
func TestRecordTracksHour(t *testing.T) {
	p := newTestPolicy(t, 30)
	p.record(observe.Observation{Prompt: 27, Cached: 23, Completion: 240, Total: 267})
	p.record(observe.Observation{Prompt: 10, Cached: 4, Completion: 20, Total: 30})
	d := readDay(t, todayFile(t, p))
	wantHour := time.Now().Hour()
	if len(d.Hours) != 1 {
		t.Fatalf("want 1 hour bucket, got %d: %+v", len(d.Hours), d.Hours)
	}
	h, ok := d.Hours[wantHour]
	if !ok {
		t.Fatalf("missing bucket for hour %d: %+v", wantHour, d.Hours)
	}
	if h.Requests != 2 || h.Input != 37 || h.InputCache != 27 || h.Output != 260 || h.Total != 297 {
		t.Fatalf("hour stats not merged: %+v", h)
	}
	// the hour buckets must agree with the day totals
	var sum hourStats
	for _, v := range d.Hours {
		sum.Requests += v.Requests
		sum.Input += v.Input
		sum.InputCache += v.InputCache
		sum.Output += v.Output
		sum.Total += v.Total
	}
	wantSum := hourStats{Requests: d.Requests, Input: d.Input, InputCache: d.InputCache, Output: d.Output, Total: d.Total}
	if sum != wantSum {
		t.Fatalf("hour buckets %v do not sum to day totals %+v", sum, d)
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
	p.record(observe.Observation{Prompt: 1, Completion: 1, Total: 2})
	if _, err := os.Stat(oldPath); err != nil {
		t.Fatalf("same-day record must not purge: %v", err)
	}
}

// the day file is written as indented JSON with the documented field names
func TestDayFileFormat(t *testing.T) {
	p := newTestPolicy(t, 30)
	p.record(observe.Observation{Prompt: 27, Cached: 23, Completion: 240, Total: 267})
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

// Handle serves the embedded page on /stats and the JSON data on /stats/data
func TestHandleServesPageAndData(t *testing.T) {
	p := newTestPolicy(t, 30)
	p.OnCompletion(observe.Observation{Prompt: 27, Cached: 23, Completion: 240, Total: 267})

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
	if !strings.Contains(w.Body.String(), "Tokens") || !strings.Contains(w.Body.String(), "/stats/data") {
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
