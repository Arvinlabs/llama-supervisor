package main

import (
	"context"
	"time"
)

// restartPolicy 重启策略：空闲超时则执行 restart.command
type restartPolicy struct {
	tracker *idleTracker
	command string
}

func newRestartPolicy(g *RestartGroup, interval time.Duration) *restartPolicy {
	r := &restartPolicy{command: g.Command}
	r.tracker = newIdleTracker(interval, func(ctx context.Context) bool {
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

func (r *restartPolicy) stop(ctx context.Context) bool {
	return r.tracker.stop(ctx)
}
