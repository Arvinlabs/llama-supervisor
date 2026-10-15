package errguard

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/Arvinlabs/llama-supervisor/internal/config"
	"github.com/Arvinlabs/llama-supervisor/internal/observe"
)

// BuildConfig: defaults, explicit cooldown, pattern compilation and a bad pattern
func TestBuildConfig(t *testing.T) {
	c, err := BuildConfig(&config.ErrGuardGroup{Enable: true, Patterns: []string{"oom", `out of memory`}, Command: "echo hi"})
	if err != nil {
		t.Fatal(err)
	}
	if c.Cooldown != 30*time.Second {
		t.Fatalf("default cooldown = %v, want 30s", c.Cooldown)
	}
	if len(c.Patterns) != 2 {
		t.Fatalf("patterns = %d, want 2", len(c.Patterns))
	}
	if c.Command != "echo hi" {
		t.Fatalf("command = %q", c.Command)
	}

	// cooldown explicitly disabled
	c, err = BuildConfig(&config.ErrGuardGroup{Enable: true, Cooldown: -1})
	if err != nil {
		t.Fatal(err)
	}
	if c.Cooldown != 0 {
		t.Fatalf("cooldown = %v, want 0 (disabled)", c.Cooldown)
	}

	// cooldown configured in seconds
	c, err = BuildConfig(&config.ErrGuardGroup{Enable: true, Cooldown: 5})
	if err != nil {
		t.Fatal(err)
	}
	if c.Cooldown != 5*time.Second {
		t.Fatalf("cooldown = %v, want 5s", c.Cooldown)
	}

	// an invalid regexp fails at build time
	if _, err := BuildConfig(&config.ErrGuardGroup{Enable: true, Patterns: []string{"["}}); err == nil {
		t.Fatal("expected an error for an invalid pattern")
	}
}

// matches: a pattern list matches the message; an empty list matches any non-empty message
func TestMatches(t *testing.T) {
	p, err := New(&config.ErrGuardGroup{Enable: true, Patterns: []string{"server_error", `out of memory`}}, context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !p.matches("CUDA out of memory") {
		t.Fatal("message should match a pattern")
	}
	if p.matches("something else") {
		t.Fatal("message must not match")
	}

	// empty pattern list: matches nothing (no trigger)
	p2, err := New(&config.ErrGuardGroup{Enable: true}, context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if p2.matches("any fault") {
		t.Fatal("empty pattern list must not match any message")
	}
	if p2.matches("") {
		t.Fatal("empty pattern list must not match an empty message")
	}
}

// waitForFile polls until path exists (the command runs in a goroutine) or the deadline
func waitForFile(t *testing.T, path string, d time.Duration) bool {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(path); err == nil {
			return true
		}
		time.Sleep(10 * time.Millisecond)
	}
	return false
}

// a matching error message runs the command; a non-matching one does not
func TestOnBackendErrorRunsCommandOnMatch(t *testing.T) {
	dir := t.TempDir()
	hit := filepath.Join(dir, "hit")
	p, err := New(&config.ErrGuardGroup{
		Enable:   true,
		Patterns: []string{`out of memory`},
		Command:  "touch " + hit,
	}, context.Background())
	if err != nil {
		t.Fatal(err)
	}

	// non-matching: no command
	p.OnBackendError(observe.ErrorInfo{Type: "server_error", Message: "rate limit exceeded"})
	if waitForFile(t, hit, 200*time.Millisecond) {
		t.Fatal("command ran for a non-matching message")
	}

	// matching: command runs
	p.OnBackendError(observe.ErrorInfo{Type: "server_error", Message: "CUDA out of memory", Code: "oom"})
	if !waitForFile(t, hit, 3*time.Second) {
		t.Fatal("command did not run for a matching message")
	}
}

// the cooldown suppresses a re-run within its window: a burst of matching errors
// (many concurrent requests hitting the same fault) triggers the command once
func TestOnBackendErrorCooldownSuppressesBurst(t *testing.T) {
	dir := t.TempDir()
	countFile := filepath.Join(dir, "count")
	p, err := New(&config.ErrGuardGroup{
		Enable:   true,
		Patterns: []string{`boom`},
		Command:  "echo 1 >> " + countFile,
		Cooldown: 30, // 30s window
	}, context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 5; i++ {
		p.OnBackendError(observe.ErrorInfo{Type: "server_error", Message: "boom"})
	}
	// wait for the single command to land
	if !waitForFile(t, countFile, 3*time.Second) {
		t.Fatal("command did not run")
	}
	// give any suppressed re-runs a chance to append, then check the count is exactly 1
	time.Sleep(300 * time.Millisecond)
	data, err := os.ReadFile(countFile)
	if err != nil {
		t.Fatal(err)
	}
	if n := len(data); n != 2 { // "1\n"
		t.Fatalf("expected exactly one command run, got %d bytes: %q", n, data)
	}
}

// cooldown disabled (<= 0): every matching error runs the command
func TestOnBackendErrorNoCooldown(t *testing.T) {
	dir := t.TempDir()
	countFile := filepath.Join(dir, "count")
	p, err := New(&config.ErrGuardGroup{
		Enable:   true,
		Patterns: []string{`boom`},
		Command:  "echo 1 >> " + countFile,
		Cooldown: -1,
	}, context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		p.OnBackendError(observe.ErrorInfo{Type: "server_error", Message: "boom"})
	}
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if data, err := os.ReadFile(countFile); err == nil && len(data) >= 6 { // "1\n1\n1\n"
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	data, _ := os.ReadFile(countFile)
	t.Fatalf("expected three command runs, got %q", data)
}

// no command configured: a match is logged only, nothing runs (and no panic)
func TestOnBackendErrorNoCommand(t *testing.T) {
	p, err := New(&config.ErrGuardGroup{Enable: true, Patterns: []string{`boom`}}, context.Background())
	if err != nil {
		t.Fatal(err)
	}
	p.OnBackendError(observe.ErrorInfo{Type: "server_error", Message: "boom"})
}
