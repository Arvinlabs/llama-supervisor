package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net/http"
	"sync"
	"time"
)

// watchdogConfig 看门狗实际参数（补默认值后）
type watchdogConfig struct {
	interval time.Duration // 采样间隔(秒)，默认 2（频繁采样，及时发现高速死循环）
	maxRate  float64       // 生成速度上限(t/s)，超过则判异常
	times    int           // 连续超速几次判异常，默认 2
	command  string        // 判定异常后执行的命令(shell)
	verbose  bool          // 是否打印测速正常日志（有请求且速度正常时 info），默认 false
}

// buildWatchdogConfig 从看门狗配置分组构建实际参数（补默认值）
func buildWatchdogConfig(g *WatchdogGroup) watchdogConfig {
	wc := watchdogConfig{
		interval: 2 * time.Second,
		maxRate:  200,
		times:    2,
		command:  g.Command,
		verbose:  g.Verbose,
	}
	if g.Interval > 0 {
		wc.interval = time.Duration(g.Interval) * time.Second
	}
	if g.MaxRate > 0 {
		wc.maxRate = g.MaxRate
	}
	if g.Times > 0 {
		wc.times = g.Times
	}
	return wc
}

// watchdogState 一次 /slots 采样
type watchdogState struct {
	processing bool // 是否有槽位在生成
	nDecoded   int  // 所有槽位 n_decoded 总和
}

// decideFast 纯函数：前后两次采样间隔内平均生成速度超过 maxRate 则判异常
// （llama 陷入 "////" 式死循环时 decode 速度会异常飙高）
func decideFast(prev, cur watchdogState, elapsed time.Duration, maxRate float64) bool {
	if !prev.processing {
		return false
	}
	return float64(cur.nDecoded-prev.nDecoded)/elapsed.Seconds() > maxRate
}

// watchdogPolicy 看门狗策略：频繁采样后端 /slots，生成速度持续超过阈值（大概率输出死循环）
// 则执行 watchdog.command（类似 restart）；触发后暂停一次采样再恢复
type watchdogPolicy struct {
	mu       sync.Mutex
	config   watchdogConfig
	backend  string
	apiKey   string // 采样 /slots 时携带的 Bearer api key（空则不携带）
	prev     watchdogState
	wedges   int
	paused   bool
	lastFail string // 上次采样错误信息，仅变化时打日志
}

// newWatchdogPolicy 创建看门狗策略
func newWatchdogPolicy(g *WatchdogGroup, backend string) *watchdogPolicy {
	return &watchdogPolicy{
		config:  buildWatchdogConfig(g),
		backend: backend,
		apiKey:  g.ApiKey,
	}
}

// tick 一次采样：拉取 /slots，与上次采样比较，连续 times 次超速才执行 command，
// 避免单次突发误触发；执行后暂停一次采样（重启中），下次采样重新开始判定
func (w *watchdogPolicy) tick(ctx context.Context) {
	state, err := fetchSlots(ctx, w.backend, w.apiKey)
	if err != nil {
		if msg := err.Error(); msg != w.lastFail {
			log.Printf("[watchdog] fetch /slots failed: %v", err)
			w.lastFail = msg
		}
		return
	}
	w.lastFail = ""

	w.mu.Lock()
	if w.paused { // 上次触发后跳过一次采样，重置基线
		w.paused = false
		w.wedges = 0
		w.prev = state
		w.mu.Unlock()
		return
	}
	prev := w.prev
	w.prev = state
	w.mu.Unlock()

	if !decideFast(prev, state, w.config.interval, w.config.maxRate) {
		w.mu.Lock()
		w.wedges = 0
		w.mu.Unlock()
		// verbose 开启且有生成请求、上次也在生成时 info 一次探测到的速度（首次窗口无有效速度，不打日志）
		if w.config.verbose && state.processing && prev.processing {
			rate := float64(state.nDecoded-prev.nDecoded) / w.config.interval.Seconds()
			log.Printf("[watchdog] ok: n_decoded %d -> %d (%.0f t/s <= maxRate %gt/s)",
				prev.nDecoded, state.nDecoded, rate, w.config.maxRate)
		}
		return
	}

	w.mu.Lock()
	w.wedges++
	n := w.wedges
	need := w.config.times
	w.mu.Unlock()
	if n < need {
		return
	}

	rate := float64(state.nDecoded-prev.nDecoded) / w.config.interval.Seconds()
	log.Printf("[watchdog] abnormally fast: n_decoded %d -> %d (%.0f t/s > maxRate %gt/s), restarting backend",
		prev.nDecoded, state.nDecoded, rate, w.config.maxRate)
	w.mu.Lock()
	w.paused = true
	w.wedges = 0
	w.mu.Unlock()
	if w.config.command == "" {
		log.Print("[watchdog] no command configured, skip")
		return
	}
	runCommand(ctx, "watchdog", w.config.command)
}

// slotNextToken /slots 响应中槽位 next_token[] 元素用到的字段（llama.cpp）
type slotNextToken struct {
	NDecoded int `json:"n_decoded"` // 槽位已生成 token 数
}

// slotInfo /slots 响应中用到的字段（llama.cpp）
type slotInfo struct {
	IsProcessing bool            `json:"is_processing"`
	NextToken    []slotNextToken `json:"next_token"`
}

// fetchSlots 拉取后端 /slots，聚合出 watchdogState；apiKey 非空时携带 Bearer <key>
func fetchSlots(ctx context.Context, backend, apiKey string) (watchdogState, error) {
	cctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(cctx, http.MethodGet, backend+"/slots", nil)
	if err != nil {
		return watchdogState{}, err
	}
	if apiKey != "" {
		req.Header.Set("Authorization", "Bearer "+apiKey)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return watchdogState{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 256))
		return watchdogState{}, errors.New("backend /slots " + resp.Status + ": " + string(msg))
	}
	var slots []slotInfo
	if err := json.NewDecoder(resp.Body).Decode(&slots); err != nil {
		return watchdogState{}, err
	}
	st := watchdogState{}
	for _, s := range slots {
		for _, t := range s.NextToken {
			st.nDecoded += t.NDecoded
		}
		if s.IsProcessing {
			st.processing = true
		}
	}
	return st, nil
}
