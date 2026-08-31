package idle

import (
	"context"
	"sync"
	"time"
)

// Tracker tracks idle state
type Tracker struct {
	mu       sync.Mutex
	interval time.Duration
	deadline time.Time
	active   bool // lazy mode: whether timing is in progress
	lazy     bool // timing starts only after the first request; paused after a trigger until the next request
	// onIdle is the idle timeout callback; it returns whether a command was actually run
	onIdle func(ctx context.Context) bool
}

// New creates an idle timer. Timing starts at creation (service startup);
// onIdle is triggered each time the idle timeout is crossed by a request, and can trigger repeatedly
func New(interval time.Duration, onIdle func(ctx context.Context) bool) *Tracker {
	return &Tracker{
		interval: interval,
		deadline: time.Now().Add(interval),
		active:   true,
		onIdle:   onIdle,
	}
}

// NewLazy creates a lazy idle timer. Timing starts only after the first request;
// after triggering onIdle the timer is paused (no timing without requests) and resumes when the next request arrives
func NewLazy(interval time.Duration, onIdle func(ctx context.Context) bool) *Tracker {
	return &Tracker{
		interval: interval,
		active:   false,
		lazy:     true,
		onIdle:   onIdle,
	}
}

// OnHTTPRequest activates timing when inactive; while timing and not yet due, it keeps extending the window;
// when idle is already due it does not reset, leaving the trigger to ConsumeIdle
func (t *Tracker) OnHTTPRequest() {
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

// ForceDue makes the idle deadline immediately due (test helper)
func (t *Tracker) ForceDue() {
	t.mu.Lock()
	t.deadline = time.Now().Add(-time.Second)
	t.mu.Unlock()
}

// ConsumeIdle checks, when a request arrives, whether the idle timeout was crossed; if so it triggers onIdle
// and returns whether a command was actually run this time
func (t *Tracker) ConsumeIdle(ctx context.Context) bool {
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
