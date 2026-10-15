// Package errguard watches the proxied chat completion responses for backend
// error payloads and runs a configured command when the error message matches a
// configured pattern list.
//
// A backend fault does not always show up as a hang or an output loop (which the
// probe / restart / watchdog policies catch): llama.cpp can also answer a request
// with an OpenAI-style error object, either as a stream event
//
//	data: {"error":{"type":"server_error","message":"..."}}
//
// or as a plain non-stream JSON body
//
//	{"error":{"type":"server_error","message":"..."}}
//
// The proxy taps every /v1/chat/completions response, extracts such an error and
// hands it here; when the message matches one of the configured regular
// expressions the command runs (e.g. a supervisorctl restart).
package errguard

import (
	"context"
	"fmt"
	"log"
	"regexp"
	"sync"
	"time"

	"github.com/Arvinlabs/llama-supervisor/internal/command"
	"github.com/Arvinlabs/llama-supervisor/internal/config"
	"github.com/Arvinlabs/llama-supervisor/internal/observe"
)

// Config effective error-guard parameters (defaults filled in)
type Config struct {
	Patterns []*regexp.Regexp // matched against the backend error message; empty matches nothing (no trigger)
	Command  string           // shell command run on a match
	Cooldown time.Duration    // minimum gap between two command runs (suppresses a burst of matching errors)
}

// BuildConfig compiles the pattern list and fills defaults (the effective cooldown
// comes from the config group, the single source of truth); it returns an error when
// a pattern is not a valid Go regular expression (so a bad config fails at startup
// rather than silently never matching)
func BuildConfig(g *config.ErrGuardGroup) (Config, error) {
	c := Config{Command: g.Command, Cooldown: g.EffectiveCooldown()}
	for _, p := range g.Patterns {
		re, err := regexp.Compile(p)
		if err != nil {
			return Config{}, fmt.Errorf("errguard pattern %q: %w", p, err)
		}
		c.Patterns = append(c.Patterns, re)
	}
	return c, nil
}

// Policy reacts to backend error events observed on proxied chat completions: it
// runs the configured command when the error message matches a pattern. It is
// safe for concurrent use (errors arrive from many concurrent request paths).
type Policy struct {
	mu       sync.Mutex
	config   Config
	ctx      context.Context // server-level ctx: the command is terminated on shutdown
	lastFire time.Time       // when the command last ran; guards the cooldown window
}

// New creates the policy from the config group; ctx is the server-level context
// the command inherits (so a shutdown kills a running command)
func New(g *config.ErrGuardGroup, ctx context.Context) (*Policy, error) {
	c, err := BuildConfig(g)
	if err != nil {
		return nil, err
	}
	return &Policy{config: c, ctx: ctx}, nil
}

// matches reports whether the error message matches any pattern; an empty
// pattern list matches nothing (no trigger)
func (p *Policy) matches(msg string) bool {
	for _, re := range p.config.Patterns {
		if re.MatchString(msg) {
			return true
		}
	}
	return false
}

// OnBackendError reacts to one observed backend error: when the message matches a
// pattern (and the cooldown has elapsed) it runs the command. The command runs in
// a goroutine: OnBackendError is called on the proxy's per-request response path
// and must not block the client. While the cooldown window is active a matching
// error is logged but the command is not re-run, so a burst of matching errors
// (many concurrent requests hitting the same backend fault) triggers once.
func (p *Policy) OnBackendError(e observe.ErrorInfo) {
	if !p.matches(e.Message) {
		return
	}
	p.mu.Lock()
	now := time.Now()
	if !p.lastFire.IsZero() && now.Before(p.lastFire.Add(p.config.Cooldown)) {
		p.mu.Unlock()
		log.Printf("[errguard] backend error matched (type=%q message=%q) but within cooldown, command not re-run", e.Type, e.Message)
		return
	}
	p.lastFire = now
	cmd := p.config.Command
	p.mu.Unlock()
	log.Printf("[errguard] backend error matched (type=%q code=%q message=%q)", e.Type, e.Code, e.Message)
	if cmd == "" {
		log.Print("[errguard] no command configured, skip")
		return
	}
	go command.RunCommand(p.ctx, "errguard", cmd)
}
