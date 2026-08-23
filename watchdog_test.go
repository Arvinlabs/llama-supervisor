package main

import (
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync/atomic"
	"testing"
	"time"
)

func TestDecideFast(t *testing.T) {
	// 无生成不判
	if decideFast(watchdogState{}, watchdogState{processing: true, nDecoded: 10000}, time.Second, 200) {
		t.Fatal("no prev processing should not trigger")
	}
	// 正常速度不判（44 t/s < 200）
	if decideFast(watchdogState{processing: true, nDecoded: 100}, watchdogState{processing: true, nDecoded: 144}, time.Second, 200) {
		t.Fatal("44 t/s should not trigger")
	}
	// 恰达上限不判（> 而非 >=）
	if decideFast(watchdogState{processing: true, nDecoded: 100}, watchdogState{processing: true, nDecoded: 300}, time.Second, 200) {
		t.Fatal("200 t/s at threshold should not trigger")
	}
	// 超速判异常（死循环场景）
	if !decideFast(watchdogState{processing: true, nDecoded: 100}, watchdogState{processing: true, nDecoded: 301}, time.Second, 200) {
		t.Fatal("201 t/s above 200 t/s should trigger")
	}
	// 本窗口结束时槽位已停也算（循环刚结束，窗口内确实超速）
	if !decideFast(watchdogState{processing: true, nDecoded: 100}, watchdogState{nDecoded: 3000}, time.Second, 200) {
		t.Fatal("burst then stop within window should still trigger")
	}
}

// slotsHandler 返回可动态设置 n_decoded 的 /slots 响应
type slotsHandler struct {
	nDecoded   atomic.Int64
	processing atomic.Bool
}

func (s *slotsHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	proc := "false"
	if s.processing.Load() {
		proc = "true"
	}
	w.Write([]byte(`[{"id":1,"is_processing":` + proc + `,"n_decoded":` + strconv.FormatInt(s.nDecoded.Load(), 10) + `}]`))
}

// 默认 times=2：单次超速不触发，连续两次超速才触发（进入 paused）
func TestWatchdogTickDefaultTimes(t *testing.T) {
	h := &slotsHandler{}
	h.nDecoded.Store(100)
	h.processing.Store(true)
	srv := httptest.NewServer(h)
	defer srv.Close()

	p := newWatchdogPolicy(&WatchdogGroup{Enable: true, Interval: 1, MaxRate: 10, Command: ""}, srv.URL)

	p.tick(t.Context()) // 采样1：基线 n=100
	h.nDecoded.Store(105)
	p.tick(t.Context()) // 采样2：5 t/s < 10，正常
	if p.paused {
		t.Fatal("normal speed should not trigger")
	}
	h.nDecoded.Store(300)
	p.tick(t.Context()) // 采样3：195 t/s > 10，fast 1/2，不触发
	if p.paused {
		t.Fatal("single fast sample should not trigger with default times=2")
	}
	h.nDecoded.Store(500)
	p.tick(t.Context()) // 采样4：fast 2/2，触发
	if !p.paused {
		t.Fatal("expected paused after two fast samples with default times=2")
	}
}

// times=2：需连续两次超速才触发
func TestWatchdogTickTriggersAfterTwoFast(t *testing.T) {
	h := &slotsHandler{}
	h.nDecoded.Store(100)
	h.processing.Store(true)
	srv := httptest.NewServer(h)
	defer srv.Close()

	p := newWatchdogPolicy(&WatchdogGroup{Enable: true, Interval: 1, MaxRate: 10, Times: 2, Command: ""}, srv.URL)

	p.tick(t.Context()) // 采样1：基线 n=100
	h.nDecoded.Store(200)
	p.tick(t.Context()) // 采样2：fast 1/2
	if p.paused {
		t.Fatal("should not trigger on first fast sample")
	}
	h.nDecoded.Store(300)
	p.tick(t.Context()) // 采样3：fast 2/2，触发
	if !p.paused {
		t.Fatal("expected paused after two consecutive fast samples")
	}
}

// 触发后暂停一次采样再恢复，重置基线
func TestWatchdogTickPausesOnce(t *testing.T) {
	h := &slotsHandler{}
	h.nDecoded.Store(100)
	h.processing.Store(true)
	srv := httptest.NewServer(h)
	defer srv.Close()

	p := newWatchdogPolicy(&WatchdogGroup{Enable: true, Interval: 1, MaxRate: 10, Times: 2, Command: ""}, srv.URL)

	p.tick(t.Context())
	h.nDecoded.Store(200)
	p.tick(t.Context()) // fast 1/2
	h.nDecoded.Store(300)
	p.tick(t.Context()) // fast 2/2 -> paused
	h.nDecoded.Store(301)
	p.tick(t.Context()) // paused：跳过一次并重置基线
	if p.paused {
		t.Fatal("expected unpaused after skip tick")
	}
	if p.prev.nDecoded != 301 {
		t.Fatalf("expected baseline reset to 301, got %d", p.prev.nDecoded)
	}
}

// 速度回落后计数清零：孤立的超速窗口不会触发
func TestWatchdogTickRecovers(t *testing.T) {
	h := &slotsHandler{}
	h.nDecoded.Store(100)
	h.processing.Store(true)
	srv := httptest.NewServer(h)
	defer srv.Close()

	p := newWatchdogPolicy(&WatchdogGroup{Enable: true, Interval: 1, MaxRate: 10, Times: 2, Command: ""}, srv.URL)

	p.tick(t.Context())
	h.nDecoded.Store(200)
	p.tick(t.Context()) // fast 1/2
	h.nDecoded.Store(205)
	p.tick(t.Context()) // 回落正常速度，计数清零
	h.nDecoded.Store(300)
	p.tick(t.Context()) // 又一次超速，仅 1/2，不触发
	if p.paused {
		t.Fatal("single fast window after recovery should not trigger")
	}
}

// 无生成期间（两窗口都无 processing）不判
func TestWatchdogTickIdle(t *testing.T) {
	h := &slotsHandler{}
	srv := httptest.NewServer(h)
	defer srv.Close()

	p := newWatchdogPolicy(&WatchdogGroup{Enable: true, Interval: 1, MaxRate: 10, Command: ""}, srv.URL)

	p.tick(t.Context())
	p.tick(t.Context())
	if p.paused {
		t.Fatal("idle backend should not trigger")
	}
}

// /slots 拉取失败不影响基线
func TestWatchdogTickFetchFail(t *testing.T) {
	p := newWatchdogPolicy(&WatchdogGroup{Enable: true, Interval: 1, MaxRate: 10, Command: ""}, "http://127.0.0.1:1")
	p.prev = watchdogState{processing: true, nDecoded: 100}
	p.tick(t.Context())
	if p.prev.nDecoded != 100 {
		t.Fatal("fetch failure must not reset baseline")
	}
}

func TestBuildWatchdogConfigDefaults(t *testing.T) {
	wc := buildWatchdogConfig(&WatchdogGroup{Enable: true})
	if wc.interval != 2*time.Second || wc.maxRate != 200 || wc.times != 2 || wc.command != "" {
		t.Fatalf("unexpected defaults: %+v", wc)
	}
}

func TestBuildWatchdogConfigOverrides(t *testing.T) {
	wc := buildWatchdogConfig(&WatchdogGroup{Enable: true, Interval: 5, MaxRate: 500, Times: 3, Command: "cmd"})
	if wc.interval != 5*time.Second || wc.maxRate != 500 || wc.times != 3 || wc.command != "cmd" {
		t.Fatalf("unexpected overrides: %+v", wc)
	}
}
