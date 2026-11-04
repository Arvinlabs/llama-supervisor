package stats

import (
	_ "embed"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/Arvinlabs/llama-supervisor/internal/command"
	"github.com/Arvinlabs/llama-supervisor/internal/config"
	"github.com/Arvinlabs/llama-supervisor/internal/observe"
)

//go:embed web/stats.html
var pageHTML []byte

const (
	defaultRetainDays = 7
	dateLayout        = "2006-01-02"
)

// counters holds one bucket's cumulative token counters (the counters of every
// completion observed in that bucket, local and cloud together)
type counters struct {
	Requests   int `json:"requests"`
	Input      int `json:"input"`
	InputCache int `json:"input_cache"`
	Output     int `json:"output"`
	Total      int `json:"total"`
}

// add accumulates one observation into the counters
func (c *counters) add(o observe.Observation) {
	c.Requests++
	c.Input += o.Prompt
	c.InputCache += o.Cached
	c.Output += o.Completion
	c.Total += o.Total
}

// hourStats holds one hour-of-day's cumulative token counters within a day; the
// promoted counters are the hour's totals, Cloud is the cloud-routed subset of
// them (its own counters, already included in the totals)
type hourStats struct {
	counters
	Cloud counters `json:"cloud"`
}

// dayStats is the per-day stats document: one JSON file per day in the save
// path, named YYYY-MM-DD.json, holding the day's cumulative token counters and
// a per-hour breakdown (keyed by hour-of-day 0-23) for the same day; the
// promoted counters are the day's totals, Cloud is the cloud-routed subset of
// them (its own counters, already included in the totals)
type dayStats struct {
	Date string `json:"date"`
	counters
	Cloud counters          `json:"cloud"`
	Hours map[int]hourStats `json:"hours,omitempty"`
}

// Policy is the stats consumer: it accounts every observed /v1/chat/completions
// completion into one per-day JSON file. It implements observe.Consumer and
// observe.CloudConsumer: cloud-routed completions are accounted too, into the
// same day file's separate cloud counters (so the local/cloud split is visible
// while the totals stay comparable). The proxy parses each completion and hands
// it over via OnCompletion (the response tap lives in the proxy, not here).
type Policy struct {
	savePath   string
	retainDays int
	mu         sync.Mutex // serializes the read-merge-write of the day file
	lastPurge  string     // local date of the last purge; the dir is re-scanned at most once a day

	ctx        context.Context // server-level ctx the control commands run under
	startCmd   string          // shell command that starts the backend (empty: button hidden)
	stopCmd    string          // shell command that stops the backend (empty: button hidden)
	ctrlMu     sync.Mutex      // at most one control command runs at a time
	health     func() bool     // backend liveness provider (nil: the state is not reported)
	setHealth  func(bool)      // backend liveness setter (nil: the state cannot be overridden); a successful control command applies its expected state immediately
}

// SetHealthSetter installs the backend liveness setter (owned by the
// supervisor's health probe): when a control command finishes successfully the
// probe state is switched at once (up for start, down for stop) instead of
// waiting for the next health poll
func (p *Policy) SetHealthSetter(f func(bool)) { p.setHealth = f }

// SetHealth installs the backend liveness provider (owned by the supervisor's
// health probe): its current state is served with /stats/data so the page can
// enable the start/stop buttons only for the missing state
func (p *Policy) SetHealth(f func() bool) { p.health = f }

// compile-time proofs: stats consumes local and cloud completions alike
var (
	_ observe.Consumer      = (*Policy)(nil)
	_ observe.CloudConsumer = (*Policy)(nil)
)

// ConsumeCloud reports that the stats policy also accounts cloud-routed
// completions (in the day's cloud counters); it implements observe.CloudConsumer
func (p *Policy) ConsumeCloud() bool { return true }

// New builds the policy, creates the save directory and purges expired day
// files. It exits on failure: a configured stats policy must work.
func New(g *config.StatsGroup, ctx context.Context) *Policy {
	retainDays := g.RetainDays
	if retainDays <= 0 {
		retainDays = defaultRetainDays
	}
	p := &Policy{savePath: g.SavePath, retainDays: retainDays,
		ctx: ctx, startCmd: g.StartCommand, stopCmd: g.StopCommand}
	if err := os.MkdirAll(g.SavePath, 0o755); err != nil {
		log.Fatalf("[stats] create save dir %s: %v", g.SavePath, err)
	}
	p.purge()
	return p
}

// OnCompletion accounts one observed chat completion into today's day file;
// it implements observe.Consumer. A cloud-routed completion (o.Cloud) is
// accounted into the same day totals and additionally into the cloud counters.
// Observations carrying no token usage (e.g. a draft-only chunk) are skipped so
// the request counter stays accurate.
func (p *Policy) OnCompletion(o observe.Observation) {
	if o.Prompt == 0 && o.Cached == 0 && o.Completion == 0 && o.Total == 0 {
		return
	}
	p.record(o)
}

// record adds one observation to today's day file (read-merge-atomic write);
// the first record after a date change also purges expired day files (at most once a day)
func (p *Policy) record(o observe.Observation) {
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
	d.counters.add(o)
	if o.Cloud {
		d.Cloud.add(o)
	}

	// accumulate the same counters into this hour-of-day bucket
	hour := time.Now().Hour()
	if d.Hours == nil {
		d.Hours = make(map[int]hourStats)
	}
	h := d.Hours[hour]
	h.counters.add(o)
	if o.Cloud {
		h.Cloud.add(o)
	}
	d.Hours[hour] = h

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

// Handle serves the embedded stats page on /stats, its JSON data on /stats/data
// and the backend control endpoint on /stats/control; it reports whether the
// request was one of them (otherwise the request is left untouched to be proxied)
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
		data := map[string]any{
			"days": days,
			// whether the page's start/stop buttons are available
			"start": p.startCmd != "",
			"stop":  p.stopCmd != "",
		}
		if p.health != nil {
			// whether the backend is up right now
			data["health"] = p.health()
		}
		_ = json.NewEncoder(w).Encode(data)
		return true
	case "/stats/control":
		p.handleControl(w, r)
		return true
	default:
		return false
	}
}

// handleControl runs the backend start/stop command selected by the ?action=
// query parameter (POST only): it runs the command synchronously (it must finish
// before the page shows the result) and reports the outcome as JSON
func (p *Policy) handleControl(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var cmd, action string
	switch r.URL.Query().Get("action") {
	case "start":
		action, cmd = "start", p.startCmd
	case "stop":
		action, cmd = "stop", p.stopCmd
	default:
		http.Error(w, "unknown action", http.StatusBadRequest)
		return
	}
	if cmd == "" {
		http.Error(w, action+" command not configured", http.StatusBadRequest)
		return
	}
	if !p.ctrlMu.TryLock() {
		http.Error(w, "another control command is already running", http.StatusConflict)
		return
	}
	defer p.ctrlMu.Unlock()

	ok := command.RunCommand(p.ctx, "stats-control "+action, cmd)
	if ok && p.setHealth != nil {
		// the command succeeded: apply the expected backend state (up for
		// start, down for stop) to the liveness tracker immediately
		p.setHealth(action == "start")
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"ok":      ok,
		"message": fmt.Sprintf("%s command %s", action, map[bool]string{true: "completed", false: "failed"}[ok]),
	})
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
