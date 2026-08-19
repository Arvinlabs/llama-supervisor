package main

import (
	"context"
	"testing"
	"time"
)

// 惰性计时：首次请求前不计时，即使挂钟时间超过时间窗口也不应触发
func TestLazyIdleTrackerNotActiveBeforeFirstRequest(t *testing.T) {
	trig := 0
	lt := newLazyIdleTracker(50*time.Millisecond, func(ctx context.Context) bool {
		trig++
		return true
	})
	// 无任何请求，远超时间窗口
	time.Sleep(120 * time.Millisecond)
	lt.consumeIdle(t.Context())
	if trig != 0 {
		t.Fatalf("timer must not have started before first request, got %d triggers", trig)
	}
}

func TestLazyIdleTrackerLifecycle(t *testing.T) {
	trig := 0
	lt := newLazyIdleTracker(50*time.Millisecond, func(ctx context.Context) bool {
		trig++
		return true
	})

	// 首次请求前无计时
	if lt.consumeIdle(t.Context()) {
		t.Fatal("should not trigger before first request")
	}

	// 首次请求开始计时
	lt.onHTTPRequest()
	time.Sleep(30 * time.Millisecond)
	lt.consumeIdle(t.Context())
	if trig != 0 {
		t.Fatal("should not trigger before interval")
	}

	// 请求延展时间窗口
	lt.onHTTPRequest()
	time.Sleep(30 * time.Millisecond)
	lt.consumeIdle(t.Context())
	if trig != 0 {
		t.Fatal("extended window should not have expired")
	}

	// 超时触发
	time.Sleep(80 * time.Millisecond)
	if !lt.consumeIdle(t.Context()) {
		t.Fatal("should trigger after idle interval")
	}
	if trig != 1 {
		t.Fatalf("expected 1 trigger, got %d", trig)
	}

	// 触发后暂停计时：长时间无请求也不触发
	time.Sleep(100 * time.Millisecond)
	lt.consumeIdle(t.Context())
	if trig != 1 {
		t.Fatalf("should stay paused, got %d triggers", trig)
	}

	// 再次有请求才开始重新计时
	lt.onHTTPRequest()
	time.Sleep(30 * time.Millisecond)
	lt.consumeIdle(t.Context())
	if trig != 1 {
		t.Fatal("should not trigger before new interval")
	}
	time.Sleep(100 * time.Millisecond)
	lt.consumeIdle(t.Context())
	if trig != 2 {
		t.Fatalf("expected 2 triggers, got %d", trig)
	}
}

// 普通计时（probe 使用）：从创建开始计时，触发后自动重新计时，行为不变
func TestIdleTrackerStillImmediate(t *testing.T) {
	trig := 0
	it := newIdleTracker(40*time.Millisecond, func(ctx context.Context) bool {
		trig++
		return true
	})

	// 无请求也会计时，到期即触发
	time.Sleep(60 * time.Millisecond)
	if !it.consumeIdle(t.Context()) || trig != 1 {
		t.Fatalf("expected immediate trigger, got %d", trig)
	}

	// 触发后自动重新计时，到期再次触发（即使无请求也不暂停）
	time.Sleep(60 * time.Millisecond)
	it.consumeIdle(t.Context())
	if trig != 2 {
		t.Fatalf("expected 2 triggers, got %d", trig)
	}
}
