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
	guard   *streamGuard // arms the restart window so in-flight streams get an SSE error event
}

func newRestartPolicy(g *RestartGroup, interval time.Duration, guard *streamGuard) *restartPolicy {
	r := &restartPolicy{command: g.Command, guard: guard}
	r.tracker = newLazyIdleTracker(interval, func(ctx context.Context) bool {
		// the idle timeout can be crossed by a long streaming request (no new request needed):
		// arm the guard so the stream killed by the restart is flagged to the client
		armRestart(r.guard)
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
