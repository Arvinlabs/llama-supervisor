package probe

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Arvinlabs/llama-supervisor/internal/config"
)

func TestBuildProbeConfigDefaults(t *testing.T) {
	pc := BuildProbeConfig(&config.ProbeGroup{Enable: true, Interval: 30, Command: "cmd"}, "")
	if pc.Model != "default" || pc.Prompt != "hi" || pc.MaxTokens != 64 || pc.RepeatLimit != 10 || pc.SuccessLimit != 20 || pc.Timeout != 5*time.Second {
		t.Fatalf("unexpected defaults: %+v", pc)
	}
}

func TestBuildProbeConfigOverrides(t *testing.T) {
	pc := BuildProbeConfig(&config.ProbeGroup{
		Enable: true, Interval: 30, Command: "cmd",
		Model: "m", Prompt: "p", MaxTokens: 10, RepeatLimit: 5, SuccessLimit: 8, Timeout: 1,
	}, "k")
	if pc.ApiKey != "k" || pc.Model != "m" || pc.Prompt != "p" || pc.MaxTokens != 10 || pc.RepeatLimit != 5 || pc.SuccessLimit != 8 || pc.Timeout != time.Second {
		t.Fatalf("unexpected overrides: %+v", pc)
	}
}

func TestBuildProbeConfigClampsAndDisablesSuccessLimit(t *testing.T) {
	// when successLimit is below repeatLimit it is raised to repeatLimit, so a degenerate stream is not declared healthy early
	if pc := BuildProbeConfig(&config.ProbeGroup{RepeatLimit: 20, SuccessLimit: 5}, ""); pc.SuccessLimit != 20 {
		t.Fatalf("expected clamp to repeatLimit, got %+v", pc)
	}
	// a negative value disables early success
	if pc := BuildProbeConfig(&config.ProbeGroup{SuccessLimit: -1}, ""); pc.SuccessLimit != 0 {
		t.Fatalf("expected disabled, got %+v", pc)
	}
}

func TestRepeatChecker(t *testing.T) {
	c := &repeatChecker{limit: 3}
	if ok, _ := c.check("content", "abb"); !ok {
		t.Fatal("run below limit should pass")
	}
	c = &repeatChecker{limit: 3}
	ok, err := c.check("content", "aaab")
	if ok || err == nil {
		t.Fatalf("run reaching limit should fail, got ok=%v err=%v", ok, err)
	}
	if !strings.Contains(err.Error(), "content") {
		t.Fatalf("error should mention field: %v", err)
	}
}

func sseStream(pairs ...[2]string) string {
	var b strings.Builder
	for _, p := range pairs {
		b.WriteString(`data: {"choices":[{"delta":{"reasoning_content":"` + p[0] + `","content":"` + p[1] + `"}}]}` + "\n\n")
	}
	b.WriteString("data: [DONE]\n\n")
	return b.String()
}

func TestProbeBackendStreamingHealthy(t *testing.T) {
	healthy, err := probeBackendStreaming(strings.NewReader(sseStream(
		[2]string{"Let ", ""},
		[2]string{"me think", ""},
		[2]string{"", "Final "},
		[2]string{"", "answer"},
	)), Config{RepeatLimit: 20})
	if !healthy || err != nil {
		t.Fatalf("expected healthy, got healthy=%v err=%v", healthy, err)
	}
}

func TestProbeBackendStreamingDegenerateReasoning(t *testing.T) {
	healthy, err := probeBackendStreaming(strings.NewReader(sseStream(
		[2]string{"aaaaa", ""},
		[2]string{"aaaaa", ""},
		[2]string{"aaaaa", ""},
		[2]string{"aaaaa", ""},
		[2]string{"aaaaa", ""},
	)), Config{RepeatLimit: 20})
	if healthy || err == nil {
		t.Fatalf("expected unhealthy, got healthy=%v err=%v", healthy, err)
	}
	if !strings.Contains(err.Error(), "reasoning_content") {
		t.Fatalf("error should mention reasoning_content: %v", err)
	}
}

func TestProbeBackendStreamingDegenerateContent(t *testing.T) {
	var pairs [][2]string
	for i := 0; i < 25; i++ {
		pairs = append(pairs, [2]string{"", "x"})
	}
	healthy, err := probeBackendStreaming(strings.NewReader(sseStream(pairs...)), Config{RepeatLimit: 20})
	if healthy || err == nil {
		t.Fatalf("expected unhealthy, got healthy=%v err=%v", healthy, err)
	}
	if !strings.Contains(err.Error(), "content") {
		t.Fatalf("error should mention content: %v", err)
	}
}

// the reasoning tail and the content head share the same character but neither reaches the limit:
// the run must not span across fields into a single sequence
func TestProbeBackendStreamingIndependentCounters(t *testing.T) {
	pairs := [][2]string{{"xxxxx", ""}}
	for i := 0; i < 15; i++ {
		pairs = append(pairs, [2]string{"", "x"})
	}
	healthy, err := probeBackendStreaming(strings.NewReader(sseStream(pairs...)), Config{RepeatLimit: 20})
	if !healthy || err != nil {
		t.Fatalf("expected healthy, got healthy=%v err=%v", healthy, err)
	}
}

func TestProbeBackendStreamingStopsAtDone(t *testing.T) {
	body := "data: [DONE]\n\n" + `data: {"choices":[{"delta":{"reasoning_content":"aaaaaaaaaaaaaaaaaaaaaaaaaaaa"}}]}` + "\n\n"
	healthy, err := probeBackendStreaming(strings.NewReader(body), Config{RepeatLimit: 20})
	if !healthy || err != nil {
		t.Fatalf("content after [DONE] must be ignored, got healthy=%v err=%v", healthy, err)
	}
}

func TestProbeBackend(t *testing.T) {
	pc := Config{RepeatLimit: 20, Timeout: time.Second}

	t.Run("healthy", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "text/event-stream")
			w.Write([]byte(sseStream([2]string{"", "ok"})))
		}))
		defer srv.Close()
		if healthy, err := probeBackend(t.Context(), srv.URL, pc); !healthy || err != nil {
			t.Fatalf("expected healthy, got healthy=%v err=%v", healthy, err)
		}
	})

	t.Run("non-200", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			http.Error(w, "boom", http.StatusInternalServerError)
		}))
		defer srv.Close()
		healthy, err := probeBackend(t.Context(), srv.URL, pc)
		if healthy || err == nil {
			t.Fatalf("expected unhealthy, got healthy=%v err=%v", healthy, err)
		}
		if !strings.Contains(err.Error(), "500") {
			t.Fatalf("error should contain status: %v", err)
		}
	})

	t.Run("connect-fail", func(t *testing.T) {
		if healthy, err := probeBackend(t.Context(), "http://127.0.0.1:1", pc); healthy || err == nil {
			t.Fatalf("expected unhealthy, got healthy=%v err=%v", healthy, err)
		}
	})
}

// early success: once the cumulative normal content reaches successLimit, return healthy immediately
// without waiting for [DONE]/generation to end
func TestProbeBackendEarlySuccess(t *testing.T) {
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		fl, _ := w.(http.Flusher)
		w.Write([]byte(`data: {"choices":[{"delta":{"content":"hello world, this is a normal answer"}}]}` + "\n\n"))
		fl.Flush()
		<-release // the backend keeps "generating" and never ends
	}))
	defer srv.Close()
	defer close(release)

	pc := Config{RepeatLimit: 20, SuccessLimit: 16, Timeout: 2 * time.Second}
	start := time.Now()
	healthy, err := probeBackend(t.Context(), srv.URL, pc)
	if !healthy || err != nil {
		t.Fatalf("expected healthy, got healthy=%v err=%v", healthy, err)
	}
	if d := time.Since(start); d > time.Second {
		t.Fatalf("expected early success, took %v", d)
	}
}

// early success must not mask degeneration: 32 characters reach successLimit, but the consecutive 'a' run already hit repeatLimit
func TestProbeBackendStreamingEarlySuccessNotMaskDegenerate(t *testing.T) {
	var pairs [][2]string
	for i := 0; i < 4; i++ {
		pairs = append(pairs, [2]string{"", "aaaaaaaa"})
	}
	healthy, err := probeBackendStreaming(strings.NewReader(sseStream(pairs...)), Config{RepeatLimit: 20, SuccessLimit: 32})
	if healthy || err == nil {
		t.Fatalf("expected unhealthy, got healthy=%v err=%v", healthy, err)
	}
}

// when the user request ctx is canceled (early disconnect), the probe policy falls back to the server-level ctx; the probe should still run normally
func TestProbePolicySurvivesRequestCtxCancel(t *testing.T) {
	got := false
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got = true
		w.Header().Set("Content-Type", "text/event-stream")
		w.Write([]byte(sseStream([2]string{"", "ok"})))
	}))
	defer srv.Close()

	p := New(t.Context(), srv.URL, &config.ProbeGroup{Enable: true, Interval: 1, Command: "true"}, time.Hour, "")
	// force the idle deadline to be due
	p.tracker.ForceDue()

	reqCtx, cancel := context.WithCancel(context.Background())
	cancel() // simulate the user disconnecting early
	healthy := p.ConsumeIdle(reqCtx)
	if !got {
		t.Fatal("probe should still run after user request ctx canceled")
	}
	if healthy {
		t.Fatal("healthy backend should not trigger command")
	}
}
