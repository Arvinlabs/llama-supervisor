package main

import (
	"context"
	"time"
)

// restartPolicy 重启策略：空闲超时则执行 restart.command。
// 计时首次请求后才开始；请求不断则时间窗口延展；触发重启后暂停计时，无请求不计时，再次有请求才开始计时
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
