package main

import (
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync/atomic"
	"testing"
	"time"
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
}

func (s *slotsHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
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

	p := newWatchdogPolicy(&WatchdogGroup{Enable: true, Interval: 1, MaxRate: 10, Command: ""}, srv.URL, "")

	p.tick(t.Context()) // sample 1: baseline n=100
	h.nDecoded.Store(105)
	p.tick(t.Context()) // sample 2: 5 t/s < 10, normal
	if p.paused {
		t.Fatal("normal speed should not trigger")
	}
	h.nDecoded.Store(300)
	p.tick(t.Context()) // sample 3: 195 t/s > 10, fast 1/2, no trigger
	if p.paused {
		t.Fatal("single fast sample should not trigger with default times=2")
	}
	h.nDecoded.Store(500)
	p.tick(t.Context()) // sample 4: fast 2/2, trigger
	if !p.paused {
		t.Fatal("expected paused after two fast samples with default times=2")
	}
}

// times=2: two consecutive over-speed samples are required to trigger
func TestWatchdogTickTriggersAfterTwoFast(t *testing.T) {
	h := &slotsHandler{}
	h.nDecoded.Store(100)
	h.processing.Store(true)
	srv := httptest.NewServer(h)
	defer srv.Close()

	p := newWatchdogPolicy(&WatchdogGroup{Enable: true, Interval: 1, MaxRate: 10, Times: 2, Command: ""}, srv.URL, "")

	p.tick(t.Context()) // sample 1: baseline n=100
	h.nDecoded.Store(200)
	p.tick(t.Context()) // sample 2: fast 1/2
	if p.paused {
		t.Fatal("should not trigger on first fast sample")
	}
	h.nDecoded.Store(300)
	p.tick(t.Context()) // sample 3: fast 2/2, trigger
	if !p.paused {
		t.Fatal("expected paused after two consecutive fast samples")
	}
}

// after a trigger one sample is skipped before resuming, and the baseline is reset
func TestWatchdogTickPausesOnce(t *testing.T) {
	h := &slotsHandler{}
	h.nDecoded.Store(100)
	h.processing.Store(true)
	srv := httptest.NewServer(h)
	defer srv.Close()

	p := newWatchdogPolicy(&WatchdogGroup{Enable: true, Interval: 1, MaxRate: 10, Times: 2, Command: ""}, srv.URL, "")

	p.tick(t.Context())
	h.nDecoded.Store(200)
	p.tick(t.Context()) // fast 1/2
	h.nDecoded.Store(300)
	p.tick(t.Context()) // fast 2/2 -> paused
	h.nDecoded.Store(301)
	p.tick(t.Context()) // paused: skip one and reset the baseline
	if p.paused {
		t.Fatal("expected unpaused after skip tick")
	}
	if p.prev.nDecoded != 301 {
		t.Fatalf("expected baseline reset to 301, got %d", p.prev.nDecoded)
	}
}

// the counter is reset when the speed drops back: an isolated over-speed window never triggers
func TestWatchdogTickRecovers(t *testing.T) {
	h := &slotsHandler{}
	h.nDecoded.Store(100)
	h.processing.Store(true)
	srv := httptest.NewServer(h)
	defer srv.Close()

	p := newWatchdogPolicy(&WatchdogGroup{Enable: true, Interval: 1, MaxRate: 10, Times: 2, Command: ""}, srv.URL, "")

	p.tick(t.Context())
	h.nDecoded.Store(200)
	p.tick(t.Context()) // fast 1/2
	h.nDecoded.Store(205)
	p.tick(t.Context()) // back to normal speed, counter reset
	h.nDecoded.Store(300)
	p.tick(t.Context()) // another over-speed, only 1/2, no trigger
	if p.paused {
		t.Fatal("single fast window after recovery should not trigger")
	}
}

// no generation (no processing in either window), no trigger
func TestWatchdogTickIdle(t *testing.T) {
	h := &slotsHandler{}
	srv := httptest.NewServer(h)
	defer srv.Close()

	p := newWatchdogPolicy(&WatchdogGroup{Enable: true, Interval: 1, MaxRate: 10, Command: ""}, srv.URL, "")

	p.tick(t.Context())
	p.tick(t.Context())
	if p.paused {
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
	p := newWatchdogPolicy(&WatchdogGroup{Enable: true, Interval: 1, MaxRate: 10, Command: ""}, "http://127.0.0.1:1", "")
	p.prev = watchdogState{processing: true, nDecoded: 100}
	p.tick(t.Context())
	if p.prev.nDecoded != 100 {
		t.Fatal("fetch failure must not reset baseline")
	}
}

// a /slots fetch failure breaks the over-speed streak: later non-consecutive fast samples must not trigger
func TestWatchdogTickFetchFailResetsStreak(t *testing.T) {
	p := newWatchdogPolicy(&WatchdogGroup{Enable: true, Interval: 1, MaxRate: 10, Times: 2, Command: ""}, "http://127.0.0.1:1", "")
	p.prev = watchdogState{processing: true, nDecoded: 100}
	p.wedges = 1 // previous sample was over speed, 1/2
	p.tick(t.Context()) // fetch fails: streak broken
	if p.wedges != 0 {
		t.Fatalf("expected streak reset on fetch failure, got wedges=%d", p.wedges)
	}
}

func TestBuildWatchdogConfigDefaults(t *testing.T) {
	wc := buildWatchdogConfig(&WatchdogGroup{Enable: true})
	if wc.interval != 2*time.Second || wc.maxRate != 200 || wc.times != 2 || wc.command != "" || wc.verbose {
		t.Fatalf("unexpected defaults: %+v", wc)
	}
}

func TestBuildWatchdogConfigOverrides(t *testing.T) {
	wc := buildWatchdogConfig(&WatchdogGroup{Enable: true, Interval: 5, MaxRate: 500, Times: 3, Command: "cmd", Verbose: true})
	if wc.interval != 5*time.Second || wc.maxRate != 500 || wc.times != 3 || wc.command != "cmd" || !wc.verbose {
		t.Fatalf("unexpected overrides: %+v", wc)
	}
}
