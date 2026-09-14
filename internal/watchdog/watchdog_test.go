package watchdog

import (
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Arvinlabs/llama-supervisor/internal/config"
	"github.com/Arvinlabs/llama-supervisor/internal/observe"
)

func TestDecideFast(t *testing.T) {
	// no generation, no trigger
	if decideFast(watchdogState{}, watchdogState{processing: true, nDecoded: 10000}, time.Second, 200) {
		t.Fatal("no prev processing should not trigger")
	}
	// normal speed, no trigger (44 t/s < 200)
	if decideFast(watchdogState{processing: true, nDecoded: 100}, watchdogState{processing: true, nDecoded: 144}, time.Second, 200) {
		t.Fatal("44 t/s should not trigger")
	}
	// exactly at the limit, no trigger (> rather than >=)
	if decideFast(watchdogState{processing: true, nDecoded: 100}, watchdogState{processing: true, nDecoded: 300}, time.Second, 200) {
		t.Fatal("200 t/s at threshold should not trigger")
	}
	// over speed triggers (output loop scenario)
	if !decideFast(watchdogState{processing: true, nDecoded: 100}, watchdogState{processing: true, nDecoded: 301}, time.Second, 200) {
		t.Fatal("201 t/s above 200 t/s should trigger")
	}
	// it still counts if the slot stops by the end of the window (the loop just ended, but the window was indeed over speed)
	if !decideFast(watchdogState{processing: true, nDecoded: 100}, watchdogState{nDecoded: 3000}, time.Second, 200) {
		t.Fatal("burst then stop within window should still trigger")
	}
}

// slotsHandler serves a /slots response whose n_decoded can be changed dynamically
type slotsHandler struct {
	nDecoded   atomic.Int64
	processing atomic.Bool
	reqs       atomic.Int64 // number of /slots requests served
}

func (s *slotsHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.reqs.Add(1)
	w.Header().Set("Content-Type", "application/json")
	proc := "false"
	if s.processing.Load() {
		proc = "true"
	}
	// matches the real /slots response: n_decoded lives inside next_token[]
	w.Write([]byte(`[{"id":1,"is_processing":` + proc + `,"next_token":[{"n_decoded":` + strconv.FormatInt(s.nDecoded.Load(), 10) + `}]}]`))
}

// default times=2: a single over-speed sample does not trigger, two consecutive ones do (entering paused)
func TestWatchdogTickDefaultTimes(t *testing.T) {
	h := &slotsHandler{}
	h.nDecoded.Store(100)
	h.processing.Store(true)
	srv := httptest.NewServer(h)
	defer srv.Close()

	p := New(&config.WatchdogGroup{Enable: true, Interval: 1, MaxRate: 10, Command: ""}, srv.URL, "")

	p.Tick(t.Context()) // sample 1: baseline n=100
	h.nDecoded.Store(105)
	p.Tick(t.Context()) // sample 2: 5 t/s < 10, normal
	if p.paused() {
		t.Fatal("normal speed should not trigger")
	}
	h.nDecoded.Store(300)
	p.Tick(t.Context()) // sample 3: 195 t/s > 10, fast 1/2, no trigger
	if p.paused() {
		t.Fatal("single fast sample should not trigger with default times=2")
	}
	h.nDecoded.Store(500)
	p.Tick(t.Context()) // sample 4: fast 2/2, trigger
	if !p.paused() {
		t.Fatal("expected pause window after two fast samples with default times=2")
	}
}

// times=2: two consecutive over-speed samples are required to trigger
func TestWatchdogTickTriggersAfterTwoFast(t *testing.T) {
	h := &slotsHandler{}
	h.nDecoded.Store(100)
	h.processing.Store(true)
	srv := httptest.NewServer(h)
	defer srv.Close()

	p := New(&config.WatchdogGroup{Enable: true, Interval: 1, MaxRate: 10, Times: 2, Command: ""}, srv.URL, "")

	p.Tick(t.Context()) // sample 1: baseline n=100
	h.nDecoded.Store(200)
	p.Tick(t.Context()) // sample 2: fast 1/2
	if p.paused() {
		t.Fatal("should not trigger on first fast sample")
	}
	h.nDecoded.Store(300)
	p.Tick(t.Context()) // sample 3: fast 2/2, trigger
	if !p.paused() {
		t.Fatal("expected pause window after two consecutive fast samples")
	}
}

// after a trigger the watchdog fully pauses (no fetching at all); after the pause the first
// sample only rebuilds the baseline, and the check then resumes and can re-trigger
func TestWatchdogTickPauseWindow(t *testing.T) {
	h := &slotsHandler{}
	h.nDecoded.Store(100)
	h.processing.Store(true)
	srv := httptest.NewServer(h)
	defer srv.Close()

	p := New(&config.WatchdogGroup{Enable: true, Interval: 1, MaxRate: 10, Times: 2, Pause: 90, Command: ""}, srv.URL, "")

	p.Tick(t.Context()) // baseline, 1 fetch
	h.nDecoded.Store(200)
	p.Tick(t.Context()) // fast 1/2, 2 fetches
	h.nDecoded.Store(300)
	p.Tick(t.Context()) // fast 2/2 -> fully paused, 3 fetches
	if !p.paused() {
		t.Fatal("expected full pause after trigger")
	}
	h.nDecoded.Store(900)
	p.Tick(t.Context()) // inside the pause: no fetch at all
	if got := h.reqs.Load(); got != 3 {
		t.Fatalf("expected no fetch during the pause, got %d requests", got)
	}
	if !p.paused() {
		t.Fatal("expected still paused inside the pause window")
	}
	// simulate the pause expiring: the first sample only rebuilds the baseline
	p.pauseUntil = time.Now().Add(-time.Second)
	p.Tick(t.Context()) // 4th fetch, baseline only (a 100 -> 900 jump must not count)
	h.nDecoded.Store(2000)
	p.Tick(t.Context()) // 1100 t/s but only fast 1/2, no trigger
	if p.paused() {
		t.Fatal("single fast sample after resume should not trigger")
	}
	h.nDecoded.Store(3000)
	p.Tick(t.Context()) // fast 2/2 after resume -> trigger again
	if !p.paused() {
		t.Fatal("expected re-trigger after two consecutive fast samples post-pause")
	}
}

// the counter is reset when the speed drops back: an isolated over-speed window never triggers
func TestWatchdogTickRecovers(t *testing.T) {
	h := &slotsHandler{}
	h.nDecoded.Store(100)
	h.processing.Store(true)
	srv := httptest.NewServer(h)
	defer srv.Close()

	p := New(&config.WatchdogGroup{Enable: true, Interval: 1, MaxRate: 10, Times: 2, Command: ""}, srv.URL, "")

	p.Tick(t.Context())
	h.nDecoded.Store(200)
	p.Tick(t.Context()) // fast 1/2
	h.nDecoded.Store(205)
	p.Tick(t.Context()) // back to normal speed, counter reset
	h.nDecoded.Store(300)
	p.Tick(t.Context()) // another over-speed, only 1/2, no trigger
	if p.paused() {
		t.Fatal("single fast window after recovery should not trigger")
	}
}

// sampling is request-driven: no fetch before any request, the first fetch a fixed
// firstProbeDelay after the first request arrives, sampling continues across concurrent
// requests, and it stops once the last request ends
func TestWatchdogRequestDrivenProbing(t *testing.T) {
	h := &slotsHandler{}
	h.processing.Store(true)
	srv := httptest.NewServer(h)
	defer srv.Close()

	p := New(&config.WatchdogGroup{Enable: true, Interval: 1, MaxRate: 10, Command: ""}, srv.URL, "")

	time.Sleep(300 * time.Millisecond)
	if got := h.reqs.Load(); got != 0 {
		t.Fatalf("no fetch should happen before any request, got %d", got)
	}

	p.OnRequestStart()
	time.Sleep(300 * time.Millisecond)
	if got := h.reqs.Load(); got != 0 {
		t.Fatalf("no fetch before the first 1s delay, got %d", got)
	}
	time.Sleep(1500 * time.Millisecond) // ~1.8s in: one fetch at the 1s mark
	if got := h.reqs.Load(); got != 1 {
		t.Fatalf("expected 1 fetch after the first delay, got %d", got)
	}

	// a concurrent request arrives and ends while the first is still in flight:
	// probing must not stop
	p.OnRequestStart()
	p.OnRequestEnd()
	time.Sleep(1500 * time.Millisecond)
	if got := h.reqs.Load(); got < 2 {
		t.Fatalf("expected continued sampling while a request is in flight, got %d", got)
	}

	// the last request ends: sampling stops
	p.OnRequestEnd()
	got := h.reqs.Load()
	time.Sleep(1600 * time.Millisecond)
	if now := h.reqs.Load(); now != got {
		t.Fatalf("expected no fetch after the last request ended, got %d then %d", got, now)
	}
}

// no generation (no processing in either window), no trigger
func TestWatchdogTickIdle(t *testing.T) {
	h := &slotsHandler{}
	srv := httptest.NewServer(h)
	defer srv.Close()

	p := New(&config.WatchdogGroup{Enable: true, Interval: 1, MaxRate: 10, Command: ""}, srv.URL, "")

	p.Tick(t.Context())
	p.Tick(t.Context())
	if p.paused() {
		t.Fatal("idle backend should not trigger")
	}
}

// when apiKey is non-empty the sample request carries Bearer <key>; when empty, none
func TestFetchSlotsApiKey(t *testing.T) {
	var got atomic.Value
	h := &slotsHandler{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got.Store(r.Header.Get("Authorization"))
		h.ServeHTTP(w, r)
	}))
	defer srv.Close()

	if _, err := fetchSlots(t.Context(), srv.URL, "secret-key"); err != nil {
		t.Fatal(err)
	}
	if v := got.Load(); v != "Bearer secret-key" {
		t.Fatalf("unexpected Authorization header: %v", v)
	}
	if _, err := fetchSlots(t.Context(), srv.URL, ""); err != nil {
		t.Fatal(err)
	}
	if v := got.Load(); v != "" {
		t.Fatalf("expected no Authorization header, got: %v", v)
	}
}

// real /slots response shape: some slots have no next_token field; n_decoded must be aggregated from next_token[]
func TestFetchSlotsRealShape(t *testing.T) {
	body := `[
	  {"id": 0, "n_ctx": 163840, "speculative": true, "is_processing": false},
	  {"id": 1, "n_ctx": 163840, "speculative": true, "is_processing": true,
	   "id_task": 3849, "n_prompt_tokens": 4181,
	   "params": {"temperature": 1, "max_tokens": -1},
	   "next_token": [{"has_next_token": true, "has_new_line": false, "n_remain": -1, "n_decoded": 123}]}
	]`
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(body))
	}))
	defer srv.Close()

	st, err := fetchSlots(t.Context(), srv.URL, "")
	if err != nil {
		t.Fatal(err)
	}
	if !st.processing || st.nDecoded != 123 {
		t.Fatalf("unexpected state: %+v", st)
	}
}

// a /slots fetch failure must not affect the baseline
func TestWatchdogTickFetchFail(t *testing.T) {
	p := New(&config.WatchdogGroup{Enable: true, Interval: 1, MaxRate: 10, Command: ""}, "http://127.0.0.1:1", "")
	p.prev = watchdogState{processing: true, nDecoded: 100}
	p.Tick(t.Context())
	if p.prev.nDecoded != 100 {
		t.Fatal("fetch failure must not reset baseline")
	}
}

// a /slots fetch failure breaks the over-speed streak (later non-consecutive fast samples must
// not trigger) and opens a pause window for the configured pause duration
func TestWatchdogTickFetchFailResetsStreak(t *testing.T) {
	p := New(&config.WatchdogGroup{Enable: true, Interval: 1, MaxRate: 10, Times: 2, Pause: 90, Command: ""}, "http://127.0.0.1:1", "")
	p.prev = watchdogState{processing: true, nDecoded: 100}
	p.wedges = 1        // previous sample was over speed, 1/2
	p.Tick(t.Context()) // fetch fails: streak broken, pause window opened
	if p.wedges != 0 {
		t.Fatalf("expected streak reset on fetch failure, got wedges=%d", p.wedges)
	}
	if !p.paused() {
		t.Fatal("expected pause window after fetch failure")
	}
	// a second consecutive failure must not extend the active pause window
	until := p.pauseUntilEnd()
	p.Tick(t.Context())
	if !p.pauseUntilEnd().Equal(until) {
		t.Fatal("active pause window must not be extended by consecutive failures")
	}
}

// a non-streaming completion in flight has no content to judge: the over-speed streak triggers directly
func TestWatchdogLoopNonStreamTriggers(t *testing.T) {
	h := &slotsHandler{}
	h.nDecoded.Store(100)
	h.processing.Store(true)
	srv := httptest.NewServer(h)
	defer srv.Close()

	p := New(&config.WatchdogGroup{Enable: true, Interval: 1, MaxRate: 10, Times: 2, Command: ""}, srv.URL, "")

	p.Tick(t.Context())       // sample 1: baseline n=100
	p.OnStreamStart(1, false) // a non-streaming completion is in flight
	h.nDecoded.Store(300)
	p.Tick(t.Context()) // fast 1/2
	if p.paused() {
		t.Fatal("a single over-speed sample should not trigger")
	}
	h.nDecoded.Store(600)
	p.Tick(t.Context()) // fast 2/2 -> non-streaming in flight, trigger directly
	if !p.paused() {
		t.Fatal("expected a direct trigger when a non-streaming completion is in flight")
	}
	p.OnStreamEnd(1)
}

// an over-speed whose in-flight stream is not in a content loop is held, and re-judged on
// each further over-speed sample while the streak is running; when the content degenerates
// it triggers
func TestWatchdogLoopStreamHoldsUntilLoop(t *testing.T) {
	h := &slotsHandler{}
	h.nDecoded.Store(100)
	h.processing.Store(true)
	srv := httptest.NewServer(h)
	defer srv.Close()

	p := New(&config.WatchdogGroup{Enable: true, Interval: 1, MaxRate: 10, Times: 2, RepeatLimit: 3, Command: ""}, srv.URL, "")

	p.Tick(t.Context()) // baseline
	id := 0
	p.OnStreamStart(id, true)
	p.OnStreamContent(id, "hello world, generating fast and fine, ")
	h.nDecoded.Store(300)
	p.Tick(t.Context()) // fast 1/2
	p.OnStreamContent(id, "still producing varied content, ")
	h.nDecoded.Store(600)
	p.Tick(t.Context()) // fast 2/2 -> judged: healthy content, hold
	if p.paused() {
		t.Fatal("an over-speed stream with healthy content must not trigger")
	}
	p.OnStreamContent(id, "###") // the dead loop appears while still over-speed
	h.nDecoded.Store(900)
	p.Tick(t.Context()) // fast 3/2 -> judged again: content loop, trigger
	if !p.paused() {
		t.Fatal("a held over-speed should trigger once the content degenerates")
	}
	p.OnStreamEnd(id)
}

// an over-speed with no in-flight completion visible to the tap (the request did not go
// through this proxy) triggers directly
func TestWatchdogLoopUnknownTriggers(t *testing.T) {
	h := &slotsHandler{}
	h.nDecoded.Store(100)
	h.processing.Store(true)
	srv := httptest.NewServer(h)
	defer srv.Close()

	p := New(&config.WatchdogGroup{Enable: true, Interval: 1, MaxRate: 10, Times: 2, Command: ""}, srv.URL, "")

	p.Tick(t.Context()) // baseline
	h.nDecoded.Store(300)
	p.Tick(t.Context()) // fast 1/2
	h.nDecoded.Store(600)
	p.Tick(t.Context()) // fast 2/2 -> nothing in flight, trigger directly
	if !p.paused() {
		t.Fatal("an over-speed with no in-flight completion should trigger directly")
	}
}

func TestBuildWatchdogConfigDefaults(t *testing.T) {
	wc := BuildWatchdogConfig(&config.WatchdogGroup{Enable: true})
	if wc.Interval != 2*time.Second || wc.MaxRate != 300 || wc.Times != 2 || wc.Pause != 30*time.Second || wc.Command != "" || wc.Verbose {
		t.Fatalf("unexpected defaults: %+v", wc)
	}
}

func TestBuildWatchdogConfigOverrides(t *testing.T) {
	wc := BuildWatchdogConfig(&config.WatchdogGroup{Enable: true, Interval: 5, MaxRate: 500, Times: 3, Pause: 30, Command: "cmd", Verbose: true})
	if wc.Interval != 5*time.Second || wc.MaxRate != 500 || wc.Times != 3 || wc.Pause != 30*time.Second || wc.Command != "cmd" || !wc.Verbose {
		t.Fatalf("unexpected overrides: %+v", wc)
	}
}

// expectPaused waits (with a timeout) until the policy is in a full pause window;
// needed for the draft check whose trigger runs in a background goroutine
func expectPaused(t *testing.T, p *Policy) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for !p.paused() {
		if time.Now().After(deadline) {
			t.Fatal("expected a pause window after the trigger")
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
}

// draftObs builds an observation with the given draft acceptance counters
func draftObs(accepted, n int) observe.Observation {
	return observe.Observation{DraftN: n, DraftAccepted: accepted}
}

// N consecutive completions below minDraftRate trigger (entering a full pause)
func TestObserveDraftTriggers(t *testing.T) {
	p := New(&config.WatchdogGroup{Enable: true, MinDraftRate: 0.5, DraftTimes: 2, Command: ""}, "http://127.0.0.1:1", "")
	p.OnCompletion(draftObs(40, 100)) // 0.40 < 0.5, low 1/2
	if p.paused() {
		t.Fatal("a single low sample should not trigger")
	}
	p.OnCompletion(draftObs(30, 100)) // 0.30 < 0.5, low 2/2 -> trigger (runs in a goroutine)
	expectPaused(t, p)
}

// an accepted sample at/above the threshold resets the streak
func TestObserveDraftResets(t *testing.T) {
	p := New(&config.WatchdogGroup{Enable: true, MinDraftRate: 0.5, DraftTimes: 2, Command: ""}, "http://127.0.0.1:1", "")
	p.OnCompletion(draftObs(40, 100)) // low 1/2
	p.OnCompletion(draftObs(90, 100)) // 0.90 >= 0.5, streak reset
	p.OnCompletion(draftObs(40, 100)) // low 1/2 again
	if p.paused() {
		t.Fatal("a recovered streak must not trigger")
	}
}

// acceptance exactly at the threshold is not "below" (>= is ok), so it never counts
func TestObserveDraftAtThreshold(t *testing.T) {
	p := New(&config.WatchdogGroup{Enable: true, MinDraftRate: 0.5, DraftTimes: 2, Command: ""}, "http://127.0.0.1:1", "")
	p.OnCompletion(draftObs(50, 100)) // 0.50 == 0.5, not below
	p.OnCompletion(draftObs(50, 100))
	if p.paused() {
		t.Fatal("at-threshold acceptance must not count as low")
	}
}

// a disabled check (minDraftRate <= 0) and a no-draft observation are both ignored
func TestObserveDraftIgnored(t *testing.T) {
	disabled := New(&config.WatchdogGroup{Enable: true, MinDraftRate: 0, DraftTimes: 2, Command: ""}, "http://127.0.0.1:1", "")
	disabled.OnCompletion(draftObs(1, 100)) // very low, but the check is disabled
	if disabled.paused() {
		t.Fatal("a disabled check must not trigger")
	}
	p := New(&config.WatchdogGroup{Enable: true, MinDraftRate: 0.5, DraftTimes: 2, Command: ""}, "http://127.0.0.1:1", "")
	p.OnCompletion(observe.Observation{Prompt: 10, Completion: 20, Total: 30}) // no draft data
	if p.paused() {
		t.Fatal("a no-draft observation must not trigger the draft check")
	}
}

// while paused the draft check does no bookkeeping and cannot extend the pause window
func TestObserveDraftWhilePaused(t *testing.T) {
	p := New(&config.WatchdogGroup{Enable: true, MinDraftRate: 0.5, DraftTimes: 1, Command: ""}, "http://127.0.0.1:1", "")
	p.OnCompletion(draftObs(10, 100)) // low 1/1 -> trigger (runs in a goroutine) + pause
	expectPaused(t, p)
	until := p.pauseUntilEnd()
	p.OnCompletion(draftObs(10, 100)) // still paused: ignored
	if !p.pauseUntilEnd().Equal(until) {
		t.Fatal("an in-pause observation must not extend the pause window")
	}
}

// BuildWatchdogConfig defaults and overrides for the draft check
func TestBuildWatchdogConfigDraft(t *testing.T) {
	wc := BuildWatchdogConfig(&config.WatchdogGroup{Enable: true})
	if wc.MinDraftRate != 0 || wc.DraftTimes != 10 {
		t.Fatalf("unexpected draft defaults: minDraftRate=%v draftTimes=%d", wc.MinDraftRate, wc.DraftTimes)
	}
	wc = BuildWatchdogConfig(&config.WatchdogGroup{Enable: true, MinDraftRate: 0.1, DraftTimes: 5})
	if wc.MinDraftRate != 0.1 || wc.DraftTimes != 5 {
		t.Fatalf("unexpected draft overrides: minDraftRate=%v draftTimes=%d", wc.MinDraftRate, wc.DraftTimes)
	}
}

// the repeatChars whitelist: only the listed runes count toward the dead loop, a non-listed
// rune breaks the run, and an empty whitelist keeps counting any rune
func TestWatchdogRepeatWhitelist(t *testing.T) {
	p := New(&config.WatchdogGroup{Enable: true, RepeatLimit: 3, RepeatChars: "/", Command: ""}, "http://127.0.0.1:1", "")
	id := 0
	p.OnStreamStart(id, true)
	p.OnStreamContent(id, "aaaa") // 'a' is not whitelisted: no loop
	if p.streams[id].degenerate {
		t.Fatal("a run of a non-whitelisted rune must not count as a dead loop")
	}
	p.OnStreamStart(id+1, true)
	p.OnStreamContent(id+1, "//a//") // broken by 'a': runs of 2 and 2
	if p.streams[id+1].degenerate {
		t.Fatal("a whitelisted run broken by another rune must not count")
	}
	p.OnStreamContent(id+1, "/") // the second run reaches 3: dead loop
	if !p.streams[id+1].degenerate {
		t.Fatal("a whitelisted run reaching the limit must mark the stream degenerate")
	}

	q := New(&config.WatchdogGroup{Enable: true, RepeatLimit: 3, Command: ""}, "http://127.0.0.1:1", "")
	q.OnStreamStart(id, true)
	q.OnStreamContent(id, "aaa") // empty whitelist: any rune counts
	if !q.streams[id].degenerate {
		t.Fatal("an empty whitelist must keep counting any rune")
	}
}

// BuildWatchdogConfig defaults and overrides for the content-loop judgment
func TestBuildWatchdogConfigRepeatLimit(t *testing.T) {
	wc := BuildWatchdogConfig(&config.WatchdogGroup{Enable: true})
	if wc.RepeatLimit != 10 {
		t.Fatalf("unexpected repeatLimit default: %d", wc.RepeatLimit)
	}
	wc = BuildWatchdogConfig(&config.WatchdogGroup{Enable: true, RepeatLimit: 8})
	if wc.RepeatLimit != 8 {
		t.Fatalf("unexpected repeatLimit override: %d", wc.RepeatLimit)
	}
}
