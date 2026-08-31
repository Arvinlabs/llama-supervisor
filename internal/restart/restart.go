package restart

import (
	"context"
	"time"

	"github.com/Arvinlabs/llama-supervisor/internal/command"
	"github.com/Arvinlabs/llama-supervisor/internal/config"
	"github.com/Arvinlabs/llama-supervisor/internal/idle"
)

// Policy restart strategy: run restart.command when the idle timeout is crossed.
// Timing starts only after the first request; as long as requests keep coming the window extends;
// after a trigger timing is paused (no timing without requests) and restarts on the next request
type Policy struct {
	tracker *idle.Tracker
	command string
}

func New(g *config.RestartGroup, interval time.Duration) *Policy {
	r := &Policy{command: g.Command}
	r.tracker = idle.NewLazy(interval, func(ctx context.Context) bool {
		return command.RunCommand(ctx, "restart", r.command)
	})
	return r
}

func (r *Policy) OnHTTPRequest() {
	r.tracker.OnHTTPRequest()
}

func (r *Policy) ConsumeIdle(ctx context.Context) bool {
	return r.tracker.ConsumeIdle(ctx)
}
