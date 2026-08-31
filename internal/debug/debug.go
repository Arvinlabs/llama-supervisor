package debug

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/Arvinlabs/llama-supervisor/internal/command"
	"github.com/Arvinlabs/llama-supervisor/internal/config"
)

// DefaultDebugPath is the default path of the debug endpoint
const DefaultDebugPath = "/debug/command"

// Policy debug strategy: an on-demand HTTP endpoint that runs debug.command
// synchronously and reports the result, for manually triggering a backend restart or
// inspecting the backend by hand. The command runs under the request context, so the
// client canceling the request aborts it.
// Note: like the other policies the command is plain shell and the endpoint is
// unauthenticated - keep it reachable only from trusted networks.
//
// It also carries the request dump feature: when SavePath is set, every request passed to Tap is
// saved to a plain text file named by the request time (inbound requests, debug.savePath); when
// OutSavePath is set, every request passed to TapOutbound is saved the same way (outbound
// requests, debug.outSavePath). Both are for later inspection and replay.
type Policy struct {
	path        string
	command     string
	savePath    string
	outSavePath string
}

func New(g *config.DebugGroup) *Policy {
	p := &Policy{command: g.Command, savePath: g.SavePath, outSavePath: g.OutSavePath}
	if g.Path != "" {
		p.path = g.Path
	} else {
		p.path = DefaultDebugPath
	}
	return p
}

// debugResponse is the JSON body returned by the debug endpoint
type debugResponse struct {
	Status  string `json:"status"` // "ok" or "failed"
	Elapsed string `json:"elapsed"`
}

// handle reports whether the request was the debug endpoint and, if so, serves it
func (p *Policy) Handle(w http.ResponseWriter, r *http.Request) bool {
	if r.URL.Path != p.path {
		return false
	}
	if r.Method != http.MethodGet && r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return true
	}
	if p.command == "" {
		http.Error(w, "no command configured", http.StatusBadRequest)
		return true
	}
	start := time.Now()
	ok := command.RunCommand(r.Context(), "debug", p.command)
	resp := debugResponse{Elapsed: time.Since(start).Round(time.Millisecond).String()}
	if ok {
		resp.Status = "ok"
	} else {
		resp.Status = "failed"
	}
	w.Header().Set("Content-Type", "application/json")
	if !ok {
		w.WriteHeader(http.StatusInternalServerError)
	}
	_ = json.NewEncoder(w).Encode(resp)
	return true
}
