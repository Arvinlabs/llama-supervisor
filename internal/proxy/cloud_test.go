package proxy

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Arvinlabs/llama-supervisor/internal/config"
)

// completionRecorder captures the requests one fake backend received
type completionRecorder struct {
	mu     sync.Mutex
	bodies map[string]string // path -> body
	auths  map[string]string // path -> Authorization header
	hits   map[string]int
}

func (c *completionRecorder) handler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/tokenize" {
			fakeTokenize(w, r)
			return
		}
		body, _ := io.ReadAll(r.Body)
		c.mu.Lock()
		if c.bodies == nil {
			c.bodies = map[string]string{}
			c.auths = map[string]string{}
			c.hits = map[string]int{}
		}
		c.bodies[r.URL.Path] = string(body)
		c.auths[r.URL.Path] = r.Header.Get("Authorization")
		c.hits[r.URL.Path]++
		c.mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"id":"x","choices":[{"message":{"content":"ok"}}],"usage":{"prompt_tokens":1,"completion_tokens":1,"total_tokens":2}}`))
	}
}

func (c *completionRecorder) hit(path string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.hits[path]
}

func (c *completionRecorder) body(path string) string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.bodies[path]
}

// fakeTokenize stands in for the local backend's POST /tokenize: it reports one
// token per 4 characters of the prompt text, so the routing arithmetic of the
// tests stays readable (the prompts are all latin). The estimator has no
// fallback, so every fake backend that must be able to route needs it
func fakeTokenize(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Content string `json:"content"`
	}
	body, _ := io.ReadAll(r.Body)
	_ = json.Unmarshal(body, &req)
	n := (len(req.Content) + 3) / 4
	var sb strings.Builder
	sb.WriteString(`{"tokens":[`)
	for i := 0; i < n; i++ {
		if i > 0 {
			sb.WriteString(",")
		}
		sb.WriteString("1")
	}
	sb.WriteString(`]}`)
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write([]byte(sb.String()))
}

// cloudTestConfig builds a cloud-enabled config: local n_ctx 4096 (override, no
// probing), threshold 0.9. The fake local backends serve POST /tokenize (see
// completionRecorder), which the estimator depends on exclusively
func cloudTestConfig(localURL, cloudURL string, mut func(*config.CloudGroup)) config.Config {
	g := &config.CloudGroup{
		Enable:         true,
		BaseURL:        cloudURL,
		APIKey:         "sk-cloud",
		Model:          "cloud-model",
		LocalMaxTokens: 4096,
	}
	if mut != nil {
		mut(g)
	}
	return config.Config{Backend: localURL, Cloud: g}
}

func TestCloudRoutesOversizedRequest(t *testing.T) {
	local := &completionRecorder{}
	localSrv := httptest.NewServer(local.handler())
	defer localSrv.Close()
	cloud := &completionRecorder{}
	cloudSrv := httptest.NewServer(cloud.handler())
	defer cloudSrv.Close()

	sup := New(cloudTestConfig(localSrv.URL, cloudSrv.URL, nil), context.Background())
	srv := httptest.NewServer(sup)
	defer srv.Close()

	// 6000 latin chars ~ 1500 tokens + 1024 reserve > 4096 * 0.9
	big := make([]byte, 20000)
	for i := range big {
		big[i] = 'a'
	}
	body := bytes.NewBufferString(`{"model":"local-model","messages":[{"role":"user","content":"`)
	body.Write(big)
	body.WriteString(`"}]}`)
	resp, err := http.Post(srv.URL+"/v1/chat/completions", "application/json", body)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	if cloud.hit("/v1/chat/completions") != 1 {
		t.Fatalf("cloud hits = %d, want 1", cloud.hit("/v1/chat/completions"))
	}
	if local.hit("/v1/chat/completions") != 0 {
		t.Fatalf("local hits = %d, want 0", local.hit("/v1/chat/completions"))
	}
	if got := cloud.auths["/v1/chat/completions"]; got != "Bearer sk-cloud" {
		t.Errorf("cloud Authorization = %q, want the cloud key", got)
	}
	if got := cloud.body("/v1/chat/completions"); !bytes.Contains([]byte(got), []byte(`"model":"cloud-model"`)) {
		t.Errorf("cloud body model not rewritten: %s", got[:min(len(got), 120)])
	}
}

func TestCloudKeepsSmallRequestLocal(t *testing.T) {
	local := &completionRecorder{}
	localSrv := httptest.NewServer(local.handler())
	defer localSrv.Close()
	cloud := &completionRecorder{}
	cloudSrv := httptest.NewServer(cloud.handler())
	defer cloudSrv.Close()

	sup := New(cloudTestConfig(localSrv.URL, cloudSrv.URL, nil), context.Background())
	srv := httptest.NewServer(sup)
	defer srv.Close()

	resp, err := http.Post(srv.URL+"/v1/chat/completions", "application/json",
		bytes.NewBufferString(`{"model":"m","messages":[{"role":"user","content":"hi"}]}`))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	if local.hit("/v1/chat/completions") != 1 || cloud.hit("/v1/chat/completions") != 0 {
		t.Fatalf("local hits = %d (want 1), cloud hits = %d (want 0)",
			local.hit("/v1/chat/completions"), cloud.hit("/v1/chat/completions"))
	}
}

func TestCloudRoutesOnLargeMaxTokens(t *testing.T) {
	local := &completionRecorder{}
	localSrv := httptest.NewServer(local.handler())
	defer localSrv.Close()
	cloud := &completionRecorder{}
	cloudSrv := httptest.NewServer(cloud.handler())
	defer cloudSrv.Close()

	sup := New(cloudTestConfig(localSrv.URL, cloudSrv.URL, nil), context.Background())
	srv := httptest.NewServer(sup)
	defer srv.Close()

	// tiny prompt, but max_tokens 5000 dominates the reserve: 5000+ > 3686
	resp, err := http.Post(srv.URL+"/v1/chat/completions", "application/json",
		bytes.NewBufferString(`{"model":"m","max_tokens":5000,"messages":[{"role":"user","content":"hi"}]}`))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if cloud.hit("/v1/chat/completions") != 1 || local.hit("/v1/chat/completions") != 0 {
		t.Fatalf("cloud hits = %d (want 1), local hits = %d (want 0)",
			cloud.hit("/v1/chat/completions"), local.hit("/v1/chat/completions"))
	}
}

func TestCloudStripsLlamaCppOnlyParams(t *testing.T) {
	local := &completionRecorder{}
	localSrv := httptest.NewServer(local.handler())
	defer localSrv.Close()
	cloud := &completionRecorder{}
	cloudSrv := httptest.NewServer(cloud.handler())
	defer cloudSrv.Close()

	sup := New(cloudTestConfig(localSrv.URL, cloudSrv.URL, nil), context.Background())
	srv := httptest.NewServer(sup)
	defer srv.Close()

	big := make([]byte, 20000)
	for i := range big {
		big[i] = 'b'
	}
	resp, err := http.Post(srv.URL+"/v1/chat/completions", "application/json",
		bytes.NewReader(bytes.Join([][]byte{[]byte(`{"model":"m","mirostat":1,"top_k":40,"temperature":0.7,`),
			[]byte(`"messages":[{"role":"user","content":"`), big, []byte(`"}]}`)}, nil)))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b := cloud.body("/v1/chat/completions")
	for _, f := range []string{`"mirostat"`, `"top_k"`} {
		if bytes.Contains([]byte(b), []byte(f)) {
			t.Errorf("cloud body still carries %s", f)
		}
	}
	if !bytes.Contains([]byte(b), []byte(`"temperature":0.7`)) {
		t.Errorf("cloud body lost the standard field temperature: %s", b[:min(len(b), 120)])
	}
}

func TestCloudKeepsRequestLocalWhenContextUnknown(t *testing.T) {
	local := &completionRecorder{}
	// the estimation works (completionRecorder serves /tokenize) but neither
	// /props nor /v1/models does: the context window cannot be detected
	localSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case completionsPath, "/tokenize":
			local.handler()(w, r)
		default:
			http.NotFound(w, r)
		}
	}))
	defer localSrv.Close()
	cloud := &completionRecorder{}
	cloudSrv := httptest.NewServer(cloud.handler())
	defer cloudSrv.Close()

	sup := New(cloudTestConfig(localSrv.URL, cloudSrv.URL, func(g *config.CloudGroup) {
		g.LocalMaxTokens = 0 // disable the override
	}), context.Background())
	srv := httptest.NewServer(sup)
	defer srv.Close()

	big := make([]byte, 20000)
	for i := range big {
		big[i] = 'c'
	}
	resp, err := http.Post(srv.URL+"/v1/chat/completions", "application/json",
		bytes.NewReader(bytes.Join([][]byte{[]byte(`{"model":"m","messages":[{"role":"user","content":"`), big, []byte(`"}]}`)}, nil)))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if local.hit(completionsPath) != 1 || cloud.hit(completionsPath) != 0 {
		t.Fatalf("local hits = %d (want 1), cloud hits = %d (want 0)",
			local.hit(completionsPath), cloud.hit(completionsPath))
	}
}

func TestCloudReadsNctxFromProps(t *testing.T) {
	local := &completionRecorder{}
	localSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/props":
			_, _ = w.Write([]byte(`{"n_ctx":1000}`))
		case completionsPath, "/tokenize":
			local.handler()(w, r)
		default:
			http.NotFound(w, r)
		}
	}))
	defer localSrv.Close()
	cloud := &completionRecorder{}
	cloudSrv := httptest.NewServer(cloud.handler())
	defer cloudSrv.Close()

	sup := New(cloudTestConfig(localSrv.URL, cloudSrv.URL, func(g *config.CloudGroup) {
		g.LocalMaxTokens = 0 // auto-detect: n_ctx 1000, limit 900 < any reserve
	}), context.Background())
	srv := httptest.NewServer(sup)
	defer srv.Close()

	resp, err := http.Post(srv.URL+"/v1/chat/completions", "application/json",
		bytes.NewBufferString(`{"model":"m","messages":[{"role":"user","content":"hi"}]}`))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if cloud.hit(completionsPath) != 1 || local.hit(completionsPath) != 0 {
		t.Fatalf("cloud hits = %d (want 1), local hits = %d (want 0)",
			cloud.hit(completionsPath), local.hit(completionsPath))
	}
}

func TestCloudStreamsThrough(t *testing.T) {
	local := &completionRecorder{}
	localSrv := httptest.NewServer(local.handler())
	defer localSrv.Close()
	cloud := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		fl := w.(http.Flusher)
		for _, s := range []string{
			`data: {"choices":[{"delta":{"content":"he"}}]}`,
			`data: {"choices":[{"delta":{"content":"llo"}}],"usage":{"prompt_tokens":2,"completion_tokens":2,"total_tokens":4}}`,
			`data: [DONE]`,
		} {
			_, _ = w.Write([]byte(s + "\n\n"))
			fl.Flush()
		}
	}))
	defer cloud.Close()

	sup := New(cloudTestConfig(localSrv.URL, cloud.URL, nil), context.Background())
	srv := httptest.NewServer(sup)
	defer srv.Close()

	big := make([]byte, 20000)
	for i := range big {
		big[i] = 'd'
	}
	resp, err := http.Post(srv.URL+"/v1/chat/completions", "application/json",
		bytes.NewReader(bytes.Join([][]byte{[]byte(`{"model":"m","stream":true,"messages":[{"role":"user","content":"`), big, []byte(`"}]}`)}, nil)))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if ct := resp.Header.Get("Content-Type"); ct != "text/event-stream" {
		t.Fatalf("Content-Type = %q, want text/event-stream", ct)
	}
	data, _ := io.ReadAll(resp.Body)
	for _, want := range []string{`"content":"he"`, `"content":"llo"`, "[DONE]"} {
		if !bytes.Contains(data, []byte(want)) {
			t.Errorf("stream missing %s: %s", want, data)
		}
	}
}

// bigChatBody builds a chat completion body large enough to route to the cloud
// (fakeTokenize counts one token per 4 characters, n_ctx 4096, threshold 0.9,
// reserve 1024) with fill as its message content
func bigChatBody(fill byte, n int) []byte {
	big := bytes.Repeat([]byte{fill}, n)
	return bytes.Join([][]byte{
		[]byte(`{"model":"m","messages":[{"role":"user","content":"`), big, []byte(`"}]}`),
	}, nil)
}

// a body above maxCloudRouteBody cannot be judged and must reach the local
// backend byte-identical: only the buffered prefix may be replayed, the unread
// remainder stays in the original reader (the router used to forward a truncated
// body, silently corrupting oversized requests)
func TestCloudOversizedBodyForwardedIntact(t *testing.T) {
	old := maxCloudRouteBody
	maxCloudRouteBody = 1 << 10
	defer func() { maxCloudRouteBody = old }()

	local := &completionRecorder{}
	localSrv := httptest.NewServer(local.handler())
	defer localSrv.Close()
	cloud := &completionRecorder{}
	cloudSrv := httptest.NewServer(cloud.handler())
	defer cloudSrv.Close()

	sup := New(cloudTestConfig(localSrv.URL, cloudSrv.URL, nil), context.Background())
	srv := httptest.NewServer(sup)
	defer srv.Close()

	body := bigChatBody('a', 8<<10) // over the (temporarily small) limit
	resp, err := http.Post(srv.URL+completionsPath, "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	if cloud.hit(completionsPath) != 0 || local.hit(completionsPath) != 1 {
		t.Fatalf("cloud hits = %d (want 0), local hits = %d (want 1)",
			cloud.hit(completionsPath), local.hit(completionsPath))
	}
	if got := local.body(completionsPath); got != string(body) {
		t.Fatalf("body truncated by the router: local backend got %d bytes, client sent %d",
			len(got), len(body))
	}
}

// a cloud error response is always passed through to the client unchanged
// (429/5xx included): the supervisor never retries a request the cloud answered
func TestCloudErrorStatusPassedThrough(t *testing.T) {
	for _, status := range []int{http.StatusTooManyRequests, http.StatusInternalServerError, http.StatusBadRequest} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			local := &completionRecorder{}
			localSrv := httptest.NewServer(local.handler())
			defer localSrv.Close()
			cloud := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(status)
				_, _ = w.Write([]byte(`{"error":{"message":"cloud said no","type":"server_error"}}`))
			}))
			defer cloud.Close()

			sup := New(cloudTestConfig(localSrv.URL, cloud.URL, nil), context.Background())
			srv := httptest.NewServer(sup)
			defer srv.Close()

			resp, err := http.Post(srv.URL+completionsPath, "application/json", bytes.NewReader(bigChatBody('c', 20000)))
			if err != nil {
				t.Fatal(err)
			}
			defer resp.Body.Close()
			if resp.StatusCode != status {
				t.Fatalf("status = %d, want the cloud status %d", resp.StatusCode, status)
			}
			if local.hit(completionsPath) != 0 {
				t.Fatalf("local hits = %d, want 0 (no local retry, ever)", local.hit(completionsPath))
			}
			body, _ := io.ReadAll(resp.Body)
			if !bytes.Contains(body, []byte("cloud said no")) {
				t.Fatalf("cloud response was not passed through: %s", body)
			}
		})
	}
}

// an unreachable cloud replies an OpenAI-format error object (SDK clients expect
// {"error":{...}}, not plain text) and is never retried on the local backend
func TestCloudUnavailableReturnsOpenAIError(t *testing.T) {
	local := &completionRecorder{}
	localSrv := httptest.NewServer(local.handler())
	defer localSrv.Close()
	dead := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	deadURL := dead.URL
	dead.Close()

	sup := New(cloudTestConfig(localSrv.URL, deadURL, nil), context.Background())
	srv := httptest.NewServer(sup)
	defer srv.Close()

	resp, err := http.Post(srv.URL+completionsPath, "application/json", bytes.NewReader(bigChatBody('d', 20000)))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadGateway {
		t.Fatalf("status = %d, want 502", resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); ct != "application/json" {
		t.Fatalf("Content-Type = %q, want application/json", ct)
	}
	var e struct {
		Error struct {
			Message string `json:"message"`
			Code    string `json:"code"`
		} `json:"error"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&e); err != nil {
		t.Fatalf("body is not an OpenAI error object: %v", err)
	}
	if e.Error.Code != "cloud_unavailable" || e.Error.Message == "" {
		t.Fatalf("unexpected error object: %+v", e)
	}
	if local.hit(completionsPath) != 0 {
		t.Fatalf("local hits = %d, want 0 (a failed cloud request is never retried)", local.hit(completionsPath))
	}
}

// the cloud completions are accounted by stats, in the day's cloud counters and
// through the cloud endpoint's own cache-hit field
func TestCloudCompletionAccountedInStats(t *testing.T) {
	local := &completionRecorder{}
	localSrv := httptest.NewServer(local.handler())
	defer localSrv.Close()
	cloud := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		// DeepSeek-style usage: the prefix cache hits are reported separately
		_, _ = w.Write([]byte(`{"choices":[{"message":{"content":"ok"}}],"usage":{"prompt_tokens":100,"completion_tokens":50,"total_tokens":150,"prompt_cache_hit_tokens":40}}`))
	}))
	defer cloud.Close()

	dir := t.TempDir()
	cfg := cloudTestConfig(localSrv.URL, cloud.URL, nil)
	cfg.Stats = &config.StatsGroup{Enable: true, SavePath: dir}
	sup := New(cfg, context.Background())
	if sup.observeCloud != true {
		t.Fatal("stats opted in to cloud observations, the cloud tap must be active")
	}
	srv := httptest.NewServer(sup)
	defer srv.Close()

	resp, err := http.Post(srv.URL+completionsPath, "application/json", bytes.NewReader(bigChatBody('e', 20000)))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}

	path := filepath.Join(dir, time.Now().Format("2006-01-02")+".json")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read day file: %v", err)
	}
	var d struct {
		Requests   int `json:"requests"`
		Input      int `json:"input"`
		InputCache int `json:"input_cache"`
		Output     int `json:"output"`
		Total      int `json:"total"`
		Cloud      struct {
			Requests   int `json:"requests"`
			Input      int `json:"input"`
			InputCache int `json:"input_cache"`
			Output     int `json:"output"`
			Total      int `json:"total"`
		} `json:"cloud"`
	}
	if err := json.Unmarshal(data, &d); err != nil {
		t.Fatalf("parse day file: %v", err)
	}
	if d.Requests != 1 || d.Input != 100 || d.Output != 50 || d.Total != 150 {
		t.Fatalf("totals do not include the cloud completion: %s", data)
	}
	c := d.Cloud
	if c.Requests != 1 || c.Input != 100 || c.InputCache != 40 || c.Output != 50 || c.Total != 150 {
		t.Fatalf("cloud counters not accounted: %s", data)
	}
}

// the watchdog never observes cloud traffic: with only the watchdog enabled the
// cloud tap stays off, and once stats (which opts in) is added it turns on
func TestCloudTapOnlyForStatsNotWatchdog(t *testing.T) {
	local := &completionRecorder{}
	localSrv := httptest.NewServer(local.handler())
	defer localSrv.Close()

	cfg := cloudTestConfig(localSrv.URL, localSrv.URL, nil)
	cfg.Watchdog = &config.WatchdogGroup{Enable: true}
	sup := New(cfg, context.Background())
	if !sup.observe {
		t.Fatal("the watchdog is a local consumer: the local tap must be active")
	}
	if sup.observeCloud {
		t.Fatal("the watchdog must not make the cloud tap active")
	}

	cfg.Stats = &config.StatsGroup{Enable: true, SavePath: t.TempDir()}
	if sup := New(cfg, context.Background()); !sup.observeCloud {
		t.Fatal("stats must make the cloud tap active")
	}
}

// cloud.maxConcurrency caps the in-flight cloud requests: a second request waits
// for a slot instead of hitting the cloud concurrently
func TestCloudMaxConcurrency(t *testing.T) {
	local := &completionRecorder{}
	localSrv := httptest.NewServer(local.handler())
	defer localSrv.Close()

	release := make(chan struct{})
	var hits, inflight, maxInflight int32
	cloud := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := atomic.AddInt32(&inflight, 1)
		for {
			old := atomic.LoadInt32(&maxInflight)
			if n <= old || atomic.CompareAndSwapInt32(&maxInflight, old, n) {
				break
			}
		}
		atomic.AddInt32(&hits, 1)
		<-release
		atomic.AddInt32(&inflight, -1)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"choices":[{"message":{"content":"ok"}}],"usage":{"prompt_tokens":1,"completion_tokens":1,"total_tokens":2}}`))
	}))
	defer cloud.Close()

	sup := New(cloudTestConfig(localSrv.URL, cloud.URL, func(g *config.CloudGroup) {
		g.MaxConcurrency = 1
	}), context.Background())
	srv := httptest.NewServer(sup)
	defer srv.Close()

	body := bigChatBody('f', 20000)
	post := func() {
		resp, err := http.Post(srv.URL+completionsPath, "application/json", bytes.NewReader(body))
		if err != nil {
			t.Error(err)
			return
		}
		_, _ = io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() { defer wg.Done(); post() }()
	// wait for the first request to occupy the only slot
	for i := 0; i < 200 && atomic.LoadInt32(&hits) == 0; i++ {
		time.Sleep(10 * time.Millisecond)
	}
	if atomic.LoadInt32(&hits) != 1 {
		t.Fatalf("first request did not reach the cloud")
	}
	wg.Add(1)
	go func() { defer wg.Done(); post() }()
	time.Sleep(200 * time.Millisecond)
	if got := atomic.LoadInt32(&hits); got != 1 {
		t.Fatalf("cloud hits = %d while the slot is taken, want 1 (the second request must wait)", got)
	}
	close(release)
	wg.Wait()
	if got := atomic.LoadInt32(&hits); got != 2 {
		t.Fatalf("cloud hits = %d after the slot was freed, want 2", got)
	}
	if got := atomic.LoadInt32(&maxInflight); got != 1 {
		t.Fatalf("max concurrent cloud requests = %d, want 1", got)
	}
}

// the exact estimator path end to end: the context window comes from /props and
// the prompt size from the backend's own POST /tokenize, with no localMaxTokens
// override (the local backend key is sent on both calls)
func TestCloudRoutesOnExactTokenize(t *testing.T) {
	var tokens int32 = 4000 // ~> limit (4096 * 0.9) - 1024 reserve, so it routes
	var tokenizeAuth string
	localRec := &completionRecorder{}
	localSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/props":
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"n_ctx":4096}`))
		case "/tokenize":
			tokenizeAuth = r.Header.Get("Authorization")
			n := int(atomic.LoadInt32(&tokens))
			var sb strings.Builder
			sb.WriteString(`{"tokens":[`)
			for i := 0; i < n; i++ {
				if i > 0 {
					sb.WriteString(",")
				}
				sb.WriteString("1")
			}
			sb.WriteString(`]}`)
			_, _ = w.Write([]byte(sb.String()))
		case completionsPath:
			localRec.handler()(w, r)
		default:
			http.NotFound(w, r)
		}
	}))
	defer localSrv.Close()
	cloudRec := &completionRecorder{}
	cloudSrv := httptest.NewServer(cloudRec.handler())
	defer cloudSrv.Close()

	cfg := cloudTestConfig(localSrv.URL, cloudSrv.URL, func(g *config.CloudGroup) {
		g.LocalMaxTokens = 0 // auto-detect from /props
	})
	cfg.ApiKey = "sk-local"
	sup := New(cfg, context.Background())
	if sup.cloudRoute == nil {
		t.Fatal("cloud router missing")
	}
	srv := httptest.NewServer(sup)
	defer srv.Close()

	post := func() int {
		resp, err := http.Post(srv.URL+completionsPath, "application/json", bytes.NewReader(bigChatBody('g', 200)))
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		_, _ = io.Copy(io.Discard, resp.Body)
		return resp.StatusCode
	}
	if got := post(); got != http.StatusOK {
		t.Fatalf("status = %d", got)
	}
	if cloudRec.hit(completionsPath) != 1 || localRec.hit(completionsPath) != 0 {
		t.Fatalf("a 4000-token prompt must route to the cloud: cloud=%d local=%d",
			cloudRec.hit(completionsPath), localRec.hit(completionsPath))
	}
	if tokenizeAuth != "Bearer sk-local" {
		t.Fatalf("/tokenize Authorization = %q, want the global backend key", tokenizeAuth)
	}

	atomic.StoreInt32(&tokens, 100) // fits comfortably: local
	post()
	if cloudRec.hit(completionsPath) != 1 || localRec.hit(completionsPath) != 1 {
		t.Fatalf("a 100-token prompt must stay local: cloud=%d local=%d",
			cloudRec.hit(completionsPath), localRec.hit(completionsPath))
	}
}

// the estimate margin scales the raw prompt estimate (not the output reserve)
// before the decision: a 2400-token prompt plus the 1024 reserve is 3424, under
// the limit (4096 * 0.9 = 3686), so without a margin it stays local; with a
// 1.5 margin the estimate is 3600 and 3600 + 1024 = 4624 routes it to the cloud
func TestCloudEstimateMarginRoutesOversizedRawEstimate(t *testing.T) {
	var tokens int32 = 2400
	localSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/props":
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"n_ctx":4096}`))
		case "/tokenize":
			var sb strings.Builder
			sb.WriteString(`{"tokens":[`)
			for i := 0; i < int(atomic.LoadInt32(&tokens)); i++ {
				if i > 0 {
					sb.WriteString(",")
				}
				sb.WriteString("1")
			}
			sb.WriteString(`]}`)
			_, _ = w.Write([]byte(sb.String()))
		default:
			http.NotFound(w, r)
		}
	}))
	defer localSrv.Close()
	cloudRec := &completionRecorder{}
	cloudSrv := httptest.NewServer(cloudRec.handler())
	defer cloudSrv.Close()

	sup := New(cloudTestConfig(localSrv.URL, cloudSrv.URL, func(g *config.CloudGroup) {
		g.EstimateMargin = 1.5
	}), context.Background())
	srv := httptest.NewServer(sup)
	defer srv.Close()

	resp, err := http.Post(srv.URL+completionsPath, "application/json", bytes.NewReader(bigChatBody('m', 200)))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	if got := cloudRec.hit(completionsPath); got != 1 {
		t.Fatalf("cloud hits = %d, want 1 (the margin must push the estimate over the limit)", got)
	}
}

// the size comes from the local backend's tokenizer and nowhere else: when it
// cannot count the prompt (here: no /tokenize endpoint at all), the request is
// not judged and stays local instead of being estimated by a client-side guess
func TestCloudKeepsRequestLocalWhenEstimationFails(t *testing.T) {
	local := &completionRecorder{}
	localSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/props":
			_, _ = w.Write([]byte(`{"n_ctx":4096}`))
		case completionsPath:
			local.handler()(w, r)
		default:
			http.NotFound(w, r) // no /tokenize: the exact count is unavailable
		}
	}))
	defer localSrv.Close()
	cloud := &completionRecorder{}
	cloudSrv := httptest.NewServer(cloud.handler())
	defer cloudSrv.Close()

	sup := New(cloudTestConfig(localSrv.URL, cloudSrv.URL, nil), context.Background())
	srv := httptest.NewServer(sup)
	defer srv.Close()

	resp, err := http.Post(srv.URL+completionsPath, "application/json", bytes.NewReader(bigChatBody('h', 20000)))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200 (served locally)", resp.StatusCode)
	}
	if cloud.hit(completionsPath) != 0 || local.hit(completionsPath) != 1 {
		t.Fatalf("cloud hits = %d (want 0), local hits = %d (want 1)",
			cloud.hit(completionsPath), local.hit(completionsPath))
	}
	if got := local.body(completionsPath); got != string(bigChatBody('h', 20000)) {
		t.Fatalf("the local request body was altered: %d bytes", len(got))
	}
}

// a stream whose last event carries the usage but has no trailing newline (and no
// [DONE]) must still be accounted: the tap flushes the trailing line at EOF, so
// the cloud counters of such a provider are not silently lost
func TestCloudStreamWithoutTrailingNewlineIsAccounted(t *testing.T) {
	local := &completionRecorder{}
	localSrv := httptest.NewServer(local.handler())
	defer localSrv.Close()
	cloud := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte("data: {\"choices\":[{\"delta\":{\"content\":\"hi\"}}]}\n\n"))
		// no "\n\n" terminator and no [DONE] after the usage event
		_, _ = w.Write([]byte(`data: {"choices":[],"usage":{"prompt_tokens":100,"completion_tokens":50,"total_tokens":150}}`))
	}))
	defer cloud.Close()

	dir := t.TempDir()
	cfg := cloudTestConfig(localSrv.URL, cloud.URL, nil)
	cfg.Stats = &config.StatsGroup{Enable: true, SavePath: dir}
	sup := New(cfg, context.Background())
	srv := httptest.NewServer(sup)
	defer srv.Close()

	body := bytes.Join([][]byte{
		[]byte(`{"model":"m","stream":true,"messages":[{"role":"user","content":"`),
		bytes.Repeat([]byte{'i'}, 20000), []byte(`"}]}`),
	}, nil)
	resp, err := http.Post(srv.URL+completionsPath, "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d", resp.StatusCode)
	}

	data, err := os.ReadFile(filepath.Join(dir, time.Now().Format("2006-01-02")+".json"))
	if err != nil {
		t.Fatalf("day file not written: %v", err)
	}
	var d struct {
		Requests int `json:"requests"`
		Total    int `json:"total"`
		Cloud    struct {
			Requests int `json:"requests"`
			Total    int `json:"total"`
		} `json:"cloud"`
	}
	if err := json.Unmarshal(data, &d); err != nil {
		t.Fatal(err)
	}
	if d.Requests != 1 || d.Total != 150 || d.Cloud.Requests != 1 || d.Cloud.Total != 150 {
		t.Fatalf("the trailing usage event was dropped: %s", data)
	}
}

// a client that advertises compression (curl and most SDKs send
// Accept-Encoding: gzip) must not make the completion invisible: the outbound
// request drops that header, so the transport decompresses the body and the tap
// parses the usage. Otherwise the cloud counters silently stay at 0
func TestCloudCompressedResponseIsAccounted(t *testing.T) {
	local := &completionRecorder{}
	localSrv := httptest.NewServer(local.handler())
	defer localSrv.Close()
	var sawEncoding string
	cloud := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sawEncoding = r.Header.Get("Accept-Encoding")
		payload := []byte(`{"choices":[{"message":{"content":"ok"}}],"usage":{"prompt_tokens":100,"completion_tokens":50,"total_tokens":150}}`)
		if strings.Contains(sawEncoding, "gzip") {
			var buf bytes.Buffer
			zw := gzip.NewWriter(&buf)
			_, _ = zw.Write(payload)
			_ = zw.Close()
			w.Header().Set("Content-Encoding", "gzip")
			payload = buf.Bytes()
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(payload)
	}))
	defer cloud.Close()

	dir := t.TempDir()
	cfg := cloudTestConfig(localSrv.URL, cloud.URL, nil)
	cfg.Stats = &config.StatsGroup{Enable: true, SavePath: dir}
	sup := New(cfg, context.Background())
	srv := httptest.NewServer(sup)
	defer srv.Close()

	req, _ := http.NewRequest(http.MethodPost, srv.URL+completionsPath, bytes.NewReader(bigChatBody('j', 20000)))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept-Encoding", "gzip") // the client asks for compression
	resp, err := http.DefaultTransport.RoundTrip(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	if resp.Header.Get("Content-Encoding") == "gzip" {
		if _, err := gzip.NewReader(bytes.NewReader(body)); err != nil {
			t.Fatalf("client received a gzip body that is not gzip: %v", err)
		}
	} else if !bytes.Contains(body, []byte(`"usage"`)) {
		t.Fatalf("client body is neither gzip nor plain JSON: %q", body[:min(len(body), 80)])
	}

	data, err := os.ReadFile(filepath.Join(dir, time.Now().Format("2006-01-02")+".json"))
	if err != nil {
		t.Fatalf("day file not written (a compressed response was not parsed): %v", err)
	}
	var d struct {
		Cloud struct {
			Requests int `json:"requests"`
			Total    int `json:"total"`
		} `json:"cloud"`
	}
	if err := json.Unmarshal(data, &d); err != nil {
		t.Fatal(err)
	}
	if d.Cloud.Requests != 1 || d.Cloud.Total != 150 {
		t.Fatalf("the compressed cloud completion was not accounted: %s", data)
	}
}

// some relays answer a non-stream request with an SSE body while announcing
// application/json: the buffered body is scanned for data lines at EOF, so the
// usage chunk still reaches stats
func TestCloudMislabeledStreamIsAccounted(t *testing.T) {
	local := &completionRecorder{}
	localSrv := httptest.NewServer(local.handler())
	defer localSrv.Close()
	cloud := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json") // a lie: the body is SSE
		_, _ = w.Write([]byte("data: {\"choices\":[{\"delta\":{\"content\":\"hi\"}}]}\n\n" +
			"data: {\"choices\":[],\"usage\":{\"prompt_tokens\":100,\"completion_tokens\":50,\"total_tokens\":150}}\n\n"))
	}))
	defer cloud.Close()

	dir := t.TempDir()
	cfg := cloudTestConfig(localSrv.URL, cloud.URL, nil)
	cfg.Stats = &config.StatsGroup{Enable: true, SavePath: dir}
	sup := New(cfg, context.Background())
	srv := httptest.NewServer(sup)
	defer srv.Close()

	resp, err := http.Post(srv.URL+completionsPath, "application/json", bytes.NewReader(bigChatBody('k', 20000)))
	if err != nil {
		t.Fatal(err)
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	resp.Body.Close()

	data, err := os.ReadFile(filepath.Join(dir, time.Now().Format("2006-01-02")+".json"))
	if err != nil {
		t.Fatalf("day file not written (the SSE body was not scanned): %v", err)
	}
	var d struct {
		Cloud struct {
			Requests int `json:"requests"`
			Total    int `json:"total"`
		} `json:"cloud"`
	}
	if err := json.Unmarshal(data, &d); err != nil {
		t.Fatal(err)
	}
	if d.Cloud.Requests != 1 || d.Cloud.Total != 150 {
		t.Fatalf("the mislabeled stream was not accounted: %s", data)
	}
}

// a compressed SSE stream (a CDN in front of the cloud endpoint can gzip even a
// stream) must be tapped through: the transport decompresses it transparently,
// so the usage event still reaches stats and the watchdog sees the content
func TestCloudCompressedStreamIsAccounted(t *testing.T) {
	local := &completionRecorder{}
	localSrv := httptest.NewServer(local.handler())
	defer localSrv.Close()
	cloud := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var buf bytes.Buffer
		zw := gzip.NewWriter(&buf)
		for _, line := range []string{
			"data: {\"choices\":[{\"delta\":{\"content\":\"he\"}}]}\n\n",
			"data: {\"choices\":[],\"usage\":{\"prompt_tokens\":100,\"completion_tokens\":50,\"total_tokens\":150}}\n\n",
			"data: [DONE]\n\n",
		} {
			_, _ = zw.Write([]byte(line))
		}
		_ = zw.Close()
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Content-Encoding", "gzip")
		_, _ = w.Write(buf.Bytes())
	}))
	defer cloud.Close()

	dir := t.TempDir()
	cfg := cloudTestConfig(localSrv.URL, cloud.URL, nil)
	cfg.Stats = &config.StatsGroup{Enable: true, SavePath: dir}
	sup := New(cfg, context.Background())
	srv := httptest.NewServer(sup)
	defer srv.Close()

	body := bytes.Join([][]byte{
		[]byte(`{"model":"m","stream":true,"messages":[{"role":"user","content":"`),
		bytes.Repeat([]byte{'l'}, 20000), []byte(`"}]}`),
	}, nil)
	req, _ := http.NewRequest(http.MethodPost, srv.URL+completionsPath, bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept-Encoding", "gzip")
	resp, err := http.DefaultTransport.RoundTrip(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	stream := raw
	if resp.Header.Get("Content-Encoding") == "gzip" {
		zr, zerr := gzip.NewReader(bytes.NewReader(raw))
		if zerr != nil {
			t.Fatalf("gzip body: %v", zerr)
		}
		stream, _ = io.ReadAll(zr)
	}
	if !bytes.Contains(stream, []byte(`"content":"he"`)) || !bytes.Contains(stream, []byte("[DONE]")) {
		t.Fatalf("client stream incomplete: %q", stream)
	}

	data, err := os.ReadFile(filepath.Join(dir, time.Now().Format("2006-01-02")+".json"))
	if err != nil {
		t.Fatalf("day file not written (the compressed stream was not parsed): %v", err)
	}
	var d struct {
		Cloud struct {
			Requests int `json:"requests"`
			Total    int `json:"total"`
		} `json:"cloud"`
	}
	if err := json.Unmarshal(data, &d); err != nil {
		t.Fatal(err)
	}
	if d.Cloud.Requests != 1 || d.Cloud.Total != 150 {
		t.Fatalf("the compressed cloud stream was not accounted: %s", data)
	}
}

// a cloud-routed request is not backend activity: it must not refresh the
// restart (and probe) idle timers, otherwise cloud-only traffic would keep the
// local model from ever being restarted/unloaded as idle
func TestCloudRequestIsNotBackendActivity(t *testing.T) {
	local := &completionRecorder{}
	localSrv := httptest.NewServer(local.handler())
	defer localSrv.Close()
	cloud := &completionRecorder{}
	cloudSrv := httptest.NewServer(cloud.handler())
	defer cloudSrv.Close()

	marker := filepath.Join(t.TempDir(), "restarted")
	cfg := cloudTestConfig(localSrv.URL, cloudSrv.URL, nil)
	cfg.Restart = &config.RestartGroup{
		Enable:   true,
		Interval: 1, // seconds
		Command:  "touch " + marker,
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sup := New(cfg, ctx)
	sup.StartBackground(ctx)
	srv := httptest.NewServer(sup)
	defer srv.Close()

	// cloud traffic only: the restart policy must stay untouched (its lazy timer
	// never starts), so the command does not run
	for i := 0; i < 4; i++ {
		req, _ := http.NewRequest(http.MethodPost, srv.URL+completionsPath, bytes.NewReader(bigChatBody('m', 20000)))
		req.Header.Set("Content-Type", "application/json")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		_, _ = io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		time.Sleep(400 * time.Millisecond)
	}
	if cloud.hit(completionsPath) != 4 {
		t.Fatalf("cloud hits = %d, want 4 (all routed to the cloud)", cloud.hit(completionsPath))
	}
	if _, err := os.Stat(marker); err == nil {
		t.Fatal("cloud traffic refreshed the restart idle timer: the restart command must not run")
	}

	// one local request starts the lazy idle timer; the restart command then runs
	// after the interval, exactly as it does without cloud traffic
	resp, err := http.Post(srv.URL+completionsPath, "application/json", bytes.NewReader(bigChatBody('n', 100)))
	if err != nil {
		t.Fatal(err)
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	if local.hit(completionsPath) != 1 {
		t.Fatalf("local hits = %d, want 1", local.hit(completionsPath))
	}
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(marker); err == nil {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("the local request did not start the restart idle timer")
}
