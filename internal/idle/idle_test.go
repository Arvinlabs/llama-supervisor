package idle

import (
	"context"
	"testing"
	"time"
)

// lazy timing: no timing before the first request, even if wall-clock time exceeds the window it must not trigger
func TestLazyIdleTrackerNotActiveBeforeFirstRequest(t *testing.T) {
	trig := 0
	lt := NewLazy(50*time.Millisecond, func(ctx context.Context) bool {
		trig++
		return true
	})
	// no requests at all, far beyond the window
	time.Sleep(120 * time.Millisecond)
	lt.ConsumeIdle(t.Context())
	if trig != 0 {
		t.Fatalf("timer must not have started before first request, got %d triggers", trig)
	}
}

func TestLazyIdleTrackerLifecycle(t *testing.T) {
	trig := 0
	lt := NewLazy(50*time.Millisecond, func(ctx context.Context) bool {
		trig++
		return true
	})

	// no timing before the first request
	if lt.ConsumeIdle(t.Context()) {
		t.Fatal("should not trigger before first request")
	}

	// the first request starts timing
	lt.OnHTTPRequest()
	time.Sleep(30 * time.Millisecond)
	lt.ConsumeIdle(t.Context())
	if trig != 0 {
		t.Fatal("should not trigger before interval")
	}

	// a request extends the window
	lt.OnHTTPRequest()
	time.Sleep(30 * time.Millisecond)
	lt.ConsumeIdle(t.Context())
	if trig != 0 {
		t.Fatal("extended window should not have expired")
	}

	// the timeout triggers
	time.Sleep(80 * time.Millisecond)
	if !lt.ConsumeIdle(t.Context()) {
		t.Fatal("should trigger after idle interval")
	}
	if trig != 1 {
		t.Fatalf("expected 1 trigger, got %d", trig)
	}

	// paused after the trigger: no requests for a long time, still no trigger
	time.Sleep(100 * time.Millisecond)
	lt.ConsumeIdle(t.Context())
	if trig != 1 {
		t.Fatalf("should stay paused, got %d triggers", trig)
	}

	// timing restarts only when a new request arrives
	lt.OnHTTPRequest()
	time.Sleep(30 * time.Millisecond)
	lt.ConsumeIdle(t.Context())
	if trig != 1 {
		t.Fatal("should not trigger before new interval")
	}
	time.Sleep(100 * time.Millisecond)
	lt.ConsumeIdle(t.Context())
	if trig != 2 {
		t.Fatalf("expected 2 triggers, got %d", trig)
	}
}

// plain timing (used by probe): timing starts at creation, re-arms automatically after a trigger, behavior unchanged
func TestIdleTrackerStillImmediate(t *testing.T) {
	trig := 0
	it := New(40*time.Millisecond, func(ctx context.Context) bool {
		trig++
		return true
	})

	// timing runs even without requests; triggers when due
	time.Sleep(60 * time.Millisecond)
	if !it.ConsumeIdle(t.Context()) || trig != 1 {
		t.Fatalf("expected immediate trigger, got %d", trig)
	}

	// re-arms automatically after a trigger and triggers again when due (does not pause even without requests)
	time.Sleep(60 * time.Millisecond)
	it.ConsumeIdle(t.Context())
	if trig != 2 {
		t.Fatalf("expected 2 triggers, got %d", trig)
	}
}
