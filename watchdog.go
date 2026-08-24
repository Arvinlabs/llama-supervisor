package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net/http"
	"sync"
	"time"
)

// watchdogConfig effective watchdog parameters (defaults filled in)
type watchdogConfig struct {
	interval time.Duration // sampling interval in seconds, default 2 (frequent sampling to catch fast output loops early)
	maxRate  float64       // max generation speed (t/s); above it is declared unhealthy
	times    int           // consecutive over-speed samples required to declare unhealthy, default 2
	command  string        // shell command run after declaring unhealthy
	verbose  bool          // whether to log the measured speed on normal windows, default false
}

// buildWatchdogConfig builds the effective parameters from the watchdog config group (filling in defaults)
func buildWatchdogConfig(g *WatchdogGroup) watchdogConfig {
	wc := watchdogConfig{
		interval: 2 * time.Second,
		maxRate:  200,
		times:    2,
		command:  g.Command,
		verbose:  g.Verbose,
	}
	if g.Interval > 0 {
		wc.interval = time.Duration(g.Interval) * time.Second
	}
	if g.MaxRate > 0 {
		wc.maxRate = g.MaxRate
	}
	if g.Times > 0 {
		wc.times = g.Times
	}
	return wc
}

// watchdogState is one /slots sample
type watchdogState struct {
	processing bool // whether any slot is generating
	nDecoded   int  // sum of n_decoded over all slots
}

// decideFast pure function: flag unhealthy when the average generation speed between two samples exceeds maxRate
// (when llama is stuck in a "////"-style output loop, the decode speed spikes abnormally)
func decideFast(prev, cur watchdogState, elapsed time.Duration, maxRate float64) bool {
	if !prev.processing {
		return false
	}
	return float64(cur.nDecoded-prev.nDecoded)/elapsed.Seconds() > maxRate
}

// watchdogPolicy watchdog strategy: sample the backend /slots frequently, and when the generation
// speed keeps exceeding the threshold (very likely an output loop) run watchdog.command (similar to
// restart); after a trigger one sample is skipped before resuming
type watchdogPolicy struct {
	mu       sync.Mutex
	config   watchdogConfig
	backend  string
	apiKey   string // Bearer API key sent when sampling /slots (none when empty)
	prev     watchdogState
	wedges   int
	paused   bool
	lastFail string // last fetch error message; only logged when it changes
}

// newWatchdogPolicy creates the watchdog policy; apiKey is the global apiKey
func newWatchdogPolicy(g *WatchdogGroup, backend string, apiKey string) *watchdogPolicy {
	return &watchdogPolicy{
		config:  buildWatchdogConfig(g),
		backend: backend,
		apiKey:  apiKey,
	}
}

// tick performs one sample: fetch /slots, compare with the previous sample, and run the command
// only after `times` consecutive over-speed samples to avoid a single burst triggering it;
// after running, one sample is skipped (during restart) and the check restarts from the next sample
func (w *watchdogPolicy) tick(ctx context.Context) {
	state, err := fetchSlots(ctx, w.backend, w.apiKey)
	if err != nil {
		if msg := err.Error(); msg != w.lastFail {
			log.Printf("[watchdog] fetch /slots failed: %v", err)
			w.lastFail = msg
		}
		// a failed sample breaks the streak: the next over-speed sample cannot be
		// consecutive with an earlier one, so reset the consecutive counter
		w.mu.Lock()
		w.wedges = 0
		w.mu.Unlock()
		return
	}
	w.lastFail = ""

	w.mu.Lock()
	if w.paused { // skip one sample after the last trigger and reset the baseline
		w.paused = false
		w.wedges = 0
		w.prev = state
		w.mu.Unlock()
		return
	}
	prev := w.prev
	w.prev = state
	w.mu.Unlock()

	if !decideFast(prev, state, w.config.interval, w.config.maxRate) {
		w.mu.Lock()
		w.wedges = 0
		w.mu.Unlock()
		// when verbose is on and a generation request is active in both windows, info-log the measured speed (the first window has no valid rate, no log)
		if w.config.verbose && state.processing && prev.processing {
			rate := float64(state.nDecoded-prev.nDecoded) / w.config.interval.Seconds()
			log.Printf("[watchdog] ok: n_decoded %d -> %d (%.0f t/s <= maxRate %gt/s)",
				prev.nDecoded, state.nDecoded, rate, w.config.maxRate)
		}
		return
	}

	w.mu.Lock()
	w.wedges++
	n := w.wedges
	need := w.config.times
	w.mu.Unlock()
	if n < need {
		return
	}

	rate := float64(state.nDecoded-prev.nDecoded) / w.config.interval.Seconds()
	log.Printf("[watchdog] abnormally fast: n_decoded %d -> %d (%.0f t/s > maxRate %gt/s), restarting backend",
		prev.nDecoded, state.nDecoded, rate, w.config.maxRate)
	w.mu.Lock()
	w.paused = true
	w.wedges = 0
	w.mu.Unlock()
	if w.config.command == "" {
		log.Print("[watchdog] no command configured, skip")
		return
	}
	runCommand(ctx, "watchdog", w.config.command)
}

// slotNextToken is a field of slot next_token[] elements in the /slots response (llama.cpp)
type slotNextToken struct {
	NDecoded int `json:"n_decoded"` // tokens generated so far in the slot
}

// slotInfo is a field of the /slots response (llama.cpp)
type slotInfo struct {
	IsProcessing bool            `json:"is_processing"`
	NextToken    []slotNextToken `json:"next_token"`
}

// fetchSlots fetches the backend /slots and aggregates a watchdogState; when apiKey is non-empty, sends Bearer <key>
func fetchSlots(ctx context.Context, backend, apiKey string) (watchdogState, error) {
	cctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(cctx, http.MethodGet, backend+"/slots", nil)
	if err != nil {
		return watchdogState{}, err
	}
	if apiKey != "" {
		req.Header.Set("Authorization", "Bearer "+apiKey)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return watchdogState{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 256))
		return watchdogState{}, errors.New("backend /slots " + resp.Status + ": " + string(msg))
	}
	var slots []slotInfo
	if err := json.NewDecoder(resp.Body).Decode(&slots); err != nil {
		return watchdogState{}, err
	}
	st := watchdogState{}
	for _, s := range slots {
		for _, t := range s.NextToken {
			st.nDecoded += t.NDecoded
		}
		if s.IsProcessing {
			st.processing = true
		}
	}
	return st, nil
}
