package main

import (
	"context"
	"time"
)

// restartPolicy restart strategy: run restart.command when the idle timeout is crossed.
// Timing starts only after the first request; as long as requests keep coming the window extends;
// after a trigger timing is paused (no timing without requests) and restarts on the next request
type restartPolicy struct {
	tracker *idleTracker
	command string
}

func newRestartPolicy(g *RestartGroup, interval time.Duration) *restartPolicy {
	r := &restartPolicy{command: g.Command}
	r.tracker = newLazyIdleTracker(interval, func(ctx context.Context) bool {
		return runCommand(ctx, "restart", r.command)
	})
	return r
}

func (r *restartPolicy) onHTTPRequest() {
	r.tracker.onHTTPRequest()
}

func (r *restartPolicy) consumeIdle(ctx context.Context) bool {
	return r.tracker.consumeIdle(ctx)
}
