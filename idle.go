package main

import (
	"context"
	"sync"
	"time"
)

// idleTracker tracks idle state
type idleTracker struct {
	mu       sync.Mutex
	interval time.Duration
	deadline time.Time
	active   bool // lazy mode: whether timing is in progress
	lazy     bool // timing starts only after the first request; paused after a trigger until the next request
	// onIdle is the idle timeout callback; it returns whether a command was actually run
	onIdle func(ctx context.Context) bool
}

// newIdleTracker creates an idle timer. Timing starts at creation (service startup);
// onIdle is triggered each time the idle timeout is crossed by a request, and can trigger repeatedly
func newIdleTracker(interval time.Duration, onIdle func(ctx context.Context) bool) *idleTracker {
	return &idleTracker{
		interval: interval,
		deadline: time.Now().Add(interval),
		active:   true,
		onIdle:   onIdle,
	}
}

// newLazyIdleTracker creates a lazy idle timer. Timing starts only after the first request;
// after triggering onIdle the timer is paused (no timing without requests) and resumes when the next request arrives
func newLazyIdleTracker(interval time.Duration, onIdle func(ctx context.Context) bool) *idleTracker {
	return &idleTracker{
		interval: interval,
		active:   false,
		lazy:     true,
		onIdle:   onIdle,
	}
}

// onHTTPRequest activates timing when inactive; while timing and not yet due, it keeps extending the window;
// when idle is already due it does not reset, leaving the trigger to consumeIdle
func (t *idleTracker) onHTTPRequest() {
	t.mu.Lock()
	defer t.mu.Unlock()
	now := time.Now()
	if !t.active {
		t.active = true
		t.deadline = now.Add(t.interval)
		return
	}
	if now.Before(t.deadline) {
		t.deadline = now.Add(t.interval)
	}
}

// consumeIdle checks, when a request arrives, whether the idle timeout was crossed; if so it triggers onIdle
// and returns whether a command was actually run this time
func (t *idleTracker) consumeIdle(ctx context.Context) bool {
	t.mu.Lock()
	if !t.active || time.Now().Before(t.deadline) {
		t.mu.Unlock()
		return false
	}
	// reset before running to avoid concurrent requests during the run re-triggering
	if t.lazy {
		t.active = false // pause timing after a trigger; restart on the next request
	} else {
		t.deadline = time.Now().Add(t.interval)
	}
	t.mu.Unlock()
	if t.onIdle == nil {
		return false
	}
	return t.onIdle(ctx)
}
