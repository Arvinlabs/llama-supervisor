package command

import (
	"context"
	"log"
	"os"
	"os/exec"
)

// RunCommand runs a command and waits for it to finish; the command is terminated when ctx is canceled
func RunCommand(ctx context.Context, label, cmdStr string) bool {
	if cmdStr == "" {
		return false
	}
	log.Printf("[%s] running: %s", label, cmdStr)
	c := exec.CommandContext(ctx, "sh", "-c", cmdStr)
	c.Stdout = os.Stdout
	c.Stderr = os.Stderr
	if err := c.Run(); err != nil {
		if ctx.Err() != nil {
			log.Printf("[%s] canceled: %v", label, ctx.Err())
		} else {
			log.Printf("[%s] failed: %v", label, err)
		}
		return false
	}
	log.Printf("[%s] completed", label)
	return true
}
