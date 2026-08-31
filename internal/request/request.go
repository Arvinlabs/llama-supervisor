package request

import (
	"encoding/json"
	"net/http"

	"github.com/Arvinlabs/llama-supervisor/internal/config"
)

// plugin is the extension point of the request policy: every request-level
// sub-feature (virtual keys, prefix cache, ...) implements it and is registered as a
// plugin. A plugin transforms the outbound proxied request; when it also implements
// inboundChecker it additionally gates the inbound client request before proxying.
type plugin interface {
	ModifyRequest(*http.Request)
}

// inboundChecker is implemented by plugins that inspect and may reject the inbound
// client request; Authorize returns false to reject the request before it is proxied
type inboundChecker interface {
	Authorize(*http.Request) bool
}

// Policy is the unified request policy: it runs the inbound checkers of all
// registered plugins on the client request, then applies the plugins' outbound
// modifiers in registration order. New request-level features are added by registering
// a plugin here, without touching the proxy.
type Policy struct {
	plugins []plugin
}

// New builds the request policy from the request config group,
// registering one plugin per enabled sub-feature
func New(g *config.RequestGroup, backendKey string) *Policy {
	p := &Policy{}
	if len(g.VirtualKeys) > 0 {
		p.register(newVirtualKeyPolicy(g.VirtualKeys, backendKey))
	}
	if g.PrefixCache {
		p.register(newPrefixCachePolicy())
	}
	return p
}

// register adds a plugin to the policy
func (p *Policy) register(pl plugin) {
	p.plugins = append(p.plugins, pl)
}

// Authorize runs the inbound check of every plugin that implements inboundChecker.
// On the first rejection it writes the rejection response itself (an OpenAI-format
// 401 error) to w and returns false; the request passes when all checkers accept it
func (p *Policy) Authorize(w http.ResponseWriter, r *http.Request) bool {
	for _, pl := range p.plugins {
		if c, ok := pl.(inboundChecker); ok && !c.Authorize(r) {
			p.writeUnauthorized(w)
			return false
		}
	}
	return true
}

// writeUnauthorized writes the OpenAI-format 401 error body for a rejected inbound request
func (p *Policy) writeUnauthorized(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusUnauthorized)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"error": map[string]any{
			"message": "invalid or missing api key",
			"type":    "invalid_request_error",
			"code":    "invalid_api_key",
		},
	})
}

// ModifyRequest runs all plugins' outbound modifiers in registration order
func (p *Policy) ModifyRequest(r *http.Request) {
	for _, pl := range p.plugins {
		pl.ModifyRequest(r)
	}
}
