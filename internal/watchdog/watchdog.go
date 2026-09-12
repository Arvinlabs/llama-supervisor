package watchdog

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/Arvinlabs/llama-supervisor/internal/command"
	"github.com/Arvinlabs/llama-supervisor/internal/config"
	"github.com/Arvinlabs/llama-supervisor/internal/observe"
)

// watchdogConfig effective watchdog parameters (defaults filled in)
type Config struct {
	Interval time.Duration // sampling interval in seconds, default 2 (frequent sampling to catch fast output loops early)
	MaxRate  float64       // max generation speed (t/s); above it is declared unhealthy, default 300
	Times    int           // consecutive over-speed samples required to declare unhealthy, default 2
	Pause    time.Duration // how long to fully pause (no fetching) after a trigger or a /slots fetch failure, default 30s
	Command  string        // shell command run after declaring unhealthy
	Verbose  bool          // whether to log the measured speed on normal windows, default false

	// MinDraftRate is the minimum MTP draft acceptance ratio (timings.draft_n_accepted /
	// timings.draft_n) observed per chat completion; a completion below it for DraftTimes in
	// a row declares the backend unhealthy. It is a push signal (sampled from the observed
	// completions, not /slots) and is disabled when <= 0 (the default)
	MinDraftRate float64
	// DraftTimes is the number of consecutive low draft-acceptance completions required to
	// declare unhealthy, default 10
	DraftTimes int

	// RepeatLimit is the run of identical tail runes in a streaming completion's generated
	// content that marks it a dead loop: when the over-speed streak is reached, a streaming
	// in-flight completion triggers only when its content is degenerate, while a non-streaming
	// one (no content to judge yet) triggers directly, default 10
	RepeatLimit int
	// RepeatChars is the whitelist of runes whose consecutive repetition counts as a dead
	// loop; empty means any rune counts
	RepeatChars string
	repeatSet   map[rune]bool // built from RepeatChars; nil means any rune counts
}

// countsRepeat reports whether a rune's consecutive repetition counts as a dead loop:
// every rune when no whitelist is configured, otherwise only the listed ones
func (w *Policy) countsRepeat(r rune) bool {
	return w.config.repeatSet == nil || w.config.repeatSet[r]
}

// BuildWatchdogConfig builds the effective parameters from the watchdog config group (filling in defaults)
func BuildWatchdogConfig(g *config.WatchdogGroup) Config {
	wc := Config{
		Interval:     2 * time.Second,
		MaxRate:      300,
		Times:        2,
		Pause:        30 * time.Second,
		Command:      g.Command,
		Verbose:      g.Verbose,
		MinDraftRate: g.MinDraftRate, // 0 (unset) disables the draft-acceptance check
		DraftTimes:   10,
		RepeatLimit:  10,
	}
	if g.Interval > 0 {
		wc.Interval = time.Duration(g.Interval) * time.Second
	}
	if g.MaxRate > 0 {
		wc.MaxRate = g.MaxRate
	}
	if g.Times > 0 {
		wc.Times = g.Times
	}
	if g.Pause > 0 {
		wc.Pause = time.Duration(g.Pause) * time.Second
	}
	if g.DraftTimes > 0 {
		wc.DraftTimes = g.DraftTimes
	}
	if g.RepeatLimit > 0 {
		wc.RepeatLimit = g.RepeatLimit
	}
	wc.RepeatChars = g.RepeatChars
	if g.RepeatChars != "" {
		wc.repeatSet = make(map[rune]bool)
		for _, r := range g.RepeatChars {
			wc.repeatSet[r] = true
		}
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

// streamState is the live view the watchdog keeps of one in-flight /v1/chat/completions
// response, so the over-speed monitor can judge the response's kind and generated content
type streamState struct {
	stream     bool // whether the response is a stream
	lastRune   rune // last generated rune
	run        int  // length of the trailing run of that rune (across content pieces)
	degenerate bool // sticky: the generated content fell into a repeated-rune dead loop
}

// Policy watchdog strategy: sample the backend /slots frequently, and when the generation
// speed keeps exceeding the threshold (very likely an output loop) run watchdog.command (similar to
// restart); after a trigger or a /slots fetch failure the watchdog fully pauses (no fetching at
// all) for pause seconds
type Policy struct {
	mu          sync.Mutex
	config      Config
	backend     string
	apiKey      string // Bearer API key sent when sampling /slots (none when empty)
	prev        watchdogState
	wedges      int                  // consecutive over-speed /slots samples
	draftWedges int                  // consecutive low draft-acceptance completions
	streams     map[int]*streamState // in-flight chat completion responses, by hub stream id
	pauseUntil  time.Time            // fully paused (no fetching) before this moment
	skipNext    bool                 // after a pause the first sample only rebuilds the baseline (no rate check)
	lastFail    string               // last fetch error message; only logged when it changes
}

// newWatchdogPolicy creates the watchdog policy; apiKey is the global apiKey
func New(g *config.WatchdogGroup, backend string, apiKey string) *Policy {
	return &Policy{
		config:  BuildWatchdogConfig(g),
		backend: backend,
		apiKey:  apiKey,
		streams: make(map[int]*streamState),
	}
}

// Interval returns the effective sampling interval
func (w *Policy) Interval() time.Duration {
	return w.config.Interval
}

// tick performs one sample: fetch /slots, compare with the previous sample, and once the
// over-speed streak reaches `times` judge the in-flight completions before running the
// command (streaming ones by their generated content, non-streaming ones directly), so a
// single burst never triggers it; after a trigger (or a /slots fetch failure) the watchdog
// fully pauses for pause seconds (no fetching at all), and the first sample after the pause
// only rebuilds the baseline
func (w *Policy) Tick(ctx context.Context) {
	w.mu.Lock()
	if time.Now().Before(w.pauseUntil) { // fully paused: no fetch at all
		w.mu.Unlock()
		return
	}
	w.mu.Unlock()

	state, err := fetchSlots(ctx, w.backend, w.apiKey)
	if err != nil {
		if msg := err.Error(); msg != w.lastFail {
			log.Printf("[watchdog] fetch /slots failed: %v", err)
			w.lastFail = msg
		}
		w.mu.Lock()
		// a failed sample breaks the streak: the next over-speed sample cannot be
		// consecutive with an earlier one, so reset the consecutive counter
		w.wedges = 0
		// start a full pause on a fresh failure (an active pause is not extended tick by tick)
		if !time.Now().Before(w.pauseUntil) {
			w.pauseUntil = time.Now().Add(w.config.Pause)
			w.skipNext = true
			log.Printf("[watchdog] fully paused for %s after /slots fetch failure", w.config.Pause)
		}
		w.mu.Unlock()
		return
	}
	w.lastFail = ""

	w.mu.Lock()
	if w.skipNext { // first sample after a pause: the gap makes a rate check meaningless, baseline only
		w.skipNext = false
		w.wedges = 0
		w.prev = state
		w.mu.Unlock()
		return
	}
	prev := w.prev
	w.prev = state
	w.mu.Unlock()

	if !decideFast(prev, state, w.config.Interval, w.config.MaxRate) {
		w.mu.Lock()
		w.wedges = 0
		w.mu.Unlock()
		// when verbose is on and a generation request is active in both windows, info-log the measured speed (the first window has no valid rate, no log)
		if w.config.Verbose && state.processing && prev.processing {
			rate := float64(state.nDecoded-prev.nDecoded) / w.config.Interval.Seconds()
			log.Printf("[watchdog] ok: n_decoded %d -> %d (%.0f t/s <= maxRate %gt/s)",
				prev.nDecoded, state.nDecoded, rate, w.config.MaxRate)
		}
		return
	}

	// over-speed: only after `times` consecutive over-speed samples the in-flight
	// completions are judged (a held judgment is re-made on the next over-speed sample
	// while the streak is still running)
	w.mu.Lock()
	w.wedges++
	n := w.wedges
	need := w.config.Times
	w.mu.Unlock()
	if n < need {
		return
	}
	w.evaluateLoop(ctx, prev, state)
}

// trigger declares the backend unhealthy for reason: it fully pauses the watchdog (no
// fetching) for pause seconds, resets the streaks, and runs the command (log-only when
// no command is configured)
func (w *Policy) trigger(ctx context.Context, reason string) {
	w.mu.Lock()
	w.pauseUntil = time.Now().Add(w.config.Pause)
	w.wedges = 0
	w.skipNext = true
	log.Printf("[watchdog] fully paused for %s after trigger", w.config.Pause)
	w.mu.Unlock()
	log.Printf("[watchdog] %s, restarting backend", reason)
	if w.config.Command == "" {
		log.Print("[watchdog] no command configured, skip")
		return
	}
	command.RunCommand(ctx, "watchdog", w.config.Command)
}

// evaluateLoop is called when the over-speed streak is reached (and re-made on each further
// over-speed sample while it is held): it judges the in-flight completions before
// triggering. A non-streaming completion has no content to judge yet, so it triggers
// directly; a streaming completion triggers only when its generated content has degenerated
// into a dead loop (otherwise the judgment is held); an over-speed with no in-flight
// completion visible to the tap (e.g. the request did not go through this proxy) triggers
// directly
func (w *Policy) evaluateLoop(ctx context.Context, prev, cur watchdogState) {
	w.mu.Lock()
	nTotal, nNonStream, nLoop := 0, 0, 0
	for _, s := range w.streams {
		nTotal++
		if !s.stream {
			nNonStream++
		} else if s.degenerate {
			nLoop++
		}
	}
	w.mu.Unlock()

	if nTotal > 0 && nNonStream == 0 && nLoop == 0 {
		log.Printf("[watchdog] over-speed but none of the %d streaming completion(s) is in a content loop, holding", nTotal)
		return
	}
	rate := float64(cur.nDecoded-prev.nDecoded) / w.config.Interval.Seconds()
	w.trigger(ctx, fmt.Sprintf("abnormally fast: n_decoded %d -> %d (%.0f t/s > maxRate %gt/s); %d in-flight completion(s), %d non-streaming, %d in a content loop",
		prev.nDecoded, cur.nDecoded, rate, w.config.MaxRate, nTotal, nNonStream, nLoop))
}

// OnCompletion feeds one observed chat completion into the MTP draft-acceptance
// monitor; it implements observe.Consumer. No-op when the check is disabled
// (MinDraftRate <= 0) or the observation carries no draft data. It shares the
// watchdog's pause with the over-speed monitor: a trigger of either fully pauses
// the watchdog (no /slots fetching).
func (w *Policy) OnCompletion(o observe.Observation) {
	if w.config.MinDraftRate <= 0 || !o.HasDraft() {
		return
	}
	w.observeDraft(o.DraftRate())
}

// observeDraft tracks consecutive low draft-acceptance samples. When the streak
// reaches the threshold it declares the backend unhealthy and runs the command
// (like the over-speed trigger), then fully pauses. The command runs in a
// goroutine: OnCompletion runs on the proxy's per-request stream-copy path and
// must not block the client.
func (w *Policy) observeDraft(rate float64) {
	w.mu.Lock()
	if time.Now().Before(w.pauseUntil) { // fully paused: no bookkeeping
		w.mu.Unlock()
		return
	}
	if rate < w.config.MinDraftRate {
		w.draftWedges++
	} else {
		w.draftWedges = 0
	}
	triggered := w.draftWedges >= w.config.DraftTimes
	below := w.draftWedges
	if triggered {
		w.draftWedges = 0
		w.pauseUntil = time.Now().Add(w.config.Pause)
		w.skipNext = true
	}
	w.mu.Unlock()
	if !triggered {
		if w.config.Verbose {
			log.Printf("[watchdog] ok: draft acceptance %.3f >= min %.3f (%d/%d below)", rate, w.config.MinDraftRate, below, w.config.DraftTimes)
		}
		return
	}
	go w.trigger(context.Background(), fmt.Sprintf("low draft acceptance: %.3f < min %.3f for %d completions in a row",
		rate, w.config.MinDraftRate, w.config.DraftTimes))
}

// OnStreamStart registers one in-flight /v1/chat/completions response; it implements
// observe.StreamConsumer
func (w *Policy) OnStreamStart(id int, stream bool) {
	w.mu.Lock()
	w.streams[id] = &streamState{stream: stream}
	w.mu.Unlock()
}

// OnStreamContent feeds one piece of generated content (content and reasoning_content) of
// one streaming completion; the over-speed monitor marks the stream degenerate once its
// tail keeps the same whitelisted rune (repeatChars; any rune when empty) for RepeatLimit in
// a row (a typical dead loop, e.g. "////"); a rune that is not counted also breaks the run
func (w *Policy) OnStreamContent(id int, content string) {
	w.mu.Lock()
	if s, ok := w.streams[id]; ok {
		for _, r := range content {
			counted := w.countsRepeat(r)
			if counted && r == s.lastRune {
				s.run++
			} else {
				s.lastRune = r
				s.run = 1
			}
			if !s.degenerate && counted && s.run >= w.config.RepeatLimit {
				s.degenerate = true
			}
		}
	}
	w.mu.Unlock()
}

// OnStreamEnd unregisters the in-flight /v1/chat/completions response
func (w *Policy) OnStreamEnd(id int) {
	w.mu.Lock()
	delete(w.streams, id)
	w.mu.Unlock()
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
