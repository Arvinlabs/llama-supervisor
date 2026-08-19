package main

import (
	"context"
	"sync"
	"time"
)

// idleTracker 空闲状态跟踪器
type idleTracker struct {
	mu       sync.Mutex
	interval time.Duration
	deadline time.Time
	active   bool // lazy 模式：是否已处于计时中
	lazy     bool // 首次请求后才开始计时；触发后暂停，待下次请求再计时
	// onIdle 空闲超时回调，返回本次是否实际执行了命令
	onIdle func(ctx context.Context) bool
}

// newIdleTracker 创建空闲计时器。计时从创建（服务启动）开始，
// 每次空闲超时被请求跨越时触发 onIdle，可重复触发
func newIdleTracker(interval time.Duration, onIdle func(ctx context.Context) bool) *idleTracker {
	return &idleTracker{
		interval: interval,
		deadline: time.Now().Add(interval),
		active:   true,
		onIdle:   onIdle,
	}
}

// newLazyIdleTracker 创建惰性空闲计时器。首次请求后才开始计时；
// 触发 onIdle 后暂停计时，无请求不计时，下次请求到来时重新开始计时
func newLazyIdleTracker(interval time.Duration, onIdle func(ctx context.Context) bool) *idleTracker {
	return &idleTracker{
		interval: interval,
		active:   false,
		lazy:     true,
		onIdle:   onIdle,
	}
}

// onHTTPRequest 未计时时激活计时；计时中且未到期则不断延展时间窗口；空闲已到期则不重置，留给 consumeIdle 触发
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

// consumeIdle 请求到来时判断空闲超时是否被跨越，跨越则触发 onIdle，
// 返回本次是否实际执行了命令
func (t *idleTracker) consumeIdle(ctx context.Context) bool {
	t.mu.Lock()
	if !t.active || time.Now().Before(t.deadline) {
		t.mu.Unlock()
		return false
	}
	// 先重置再执行，避免执行期间并发请求重复触发
	if t.lazy {
		t.active = false // 触发后暂停计时，下次请求再开始
	} else {
		t.deadline = time.Now().Add(t.interval)
	}
	t.mu.Unlock()
	if t.onIdle == nil {
		return false
	}
	return t.onIdle(ctx)
}
