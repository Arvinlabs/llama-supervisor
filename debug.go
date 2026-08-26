package main

import (
	"encoding/json"
	"net/http"
	"time"
)

// defaultDebugPath is the default path of the debug endpoint
const defaultDebugPath = "/debug/command"

// debugPolicy debug strategy: an on-demand HTTP endpoint that runs debug.command
// synchronously and reports the result, for manually triggering a backend restart or
// inspecting the backend by hand. The command runs under the request context, so the
// client canceling the request aborts it.
// Note: like the other policies the command is plain shell and the endpoint is
// unauthenticated - keep it reachable only from trusted networks
type debugPolicy struct {
	path    string
	command string
}

func newDebugPolicy(g *DebugGroup) *debugPolicy {
	p := &debugPolicy{command: g.Command}
	if g.Path != "" {
		p.path = g.Path
	} else {
		p.path = defaultDebugPath
	}
	return p
}

// debugResponse is the JSON body returned by the debug endpoint
type debugResponse struct {
	Status  string `json:"status"` // "ok" or "failed"
	Elapsed string `json:"elapsed"`
}

// handle reports whether the request was the debug endpoint and, if so, serves it
func (p *debugPolicy) handle(w http.ResponseWriter, r *http.Request) bool {
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
	ok := runCommand(r.Context(), "debug", p.command)
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
