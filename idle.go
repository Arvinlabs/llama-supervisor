package main

import (
	"context"
	"time"
)

// idleTracker 空闲状态跟踪器
type idleTracker struct {
	idleTime time.Duration
	deadline time.Time
	consumed bool
	// onIdle 空闲超时回调，返回本次是否实际执行了命令
	onIdle func(ctx context.Context) bool
}

func newIdleTracker(idleTime time.Duration, onIdle func(ctx context.Context) bool) *idleTracker {
	return &idleTracker{idleTime: idleTime, onIdle: onIdle}
}

// onHTTPRequest 请求活跃期间不断重置计时；空闲已到期则不重置，留给 consumeIdle 触发
func (t *idleTracker) onHTTPRequest() {
	if t.consumed {
		return
	}
	now := time.Now()
	if !t.deadline.IsZero() && now.After(t.deadline) {
		return
	}
	t.deadline = now.Add(t.idleTime)
}

// consumeIdle 请求到来时判断空闲超时是否被跨越，跨越则触发 onIdle 并重新计时，
// 返回本次是否实际执行了命令
func (t *idleTracker) consumeIdle(ctx context.Context) bool {
	if t.consumed {
		return false
	}
	if t.deadline.IsZero() {
		t.deadline = time.Now().Add(t.idleTime)
		return false
	}
	if time.Now().Before(t.deadline) {
		return false
	}
	ran := false
	if t.onIdle != nil {
		ran = t.onIdle(ctx)
	}
	t.deadline = time.Now().Add(t.idleTime)
	t.consumed = true
	return ran
}

// stop 优雅停止时若已跨越空闲超时则触发 onIdle（触发过则不再重复）
func (t *idleTracker) stop(ctx context.Context) bool {
	if t.consumed || t.deadline.IsZero() || time.Now().Before(t.deadline) {
		return false
	}
	ran := false
	if t.onIdle != nil {
		ran = t.onIdle(ctx)
	}
	t.deadline = time.Time{}
	t.consumed = true
	return ran
}
