package main

import "net/http"

// requestPolicy is the unified request policy: it applies an ordered list of request
// modifiers to every proxied request. Each sub-feature (e.g. prefix cache) registers
// one modifier; new request-level features are added here without touching the proxy.
type requestPolicy struct {
	modifiers []func(*http.Request)
}

// newRequestPolicy builds the request policy from the request config group,
// registering one modifier per enabled sub-feature
func newRequestPolicy(g *RequestGroup) *requestPolicy {
	p := &requestPolicy{}
	if g.PrefixCache {
		p.modifiers = append(p.modifiers, newPrefixCachePolicy().modifyRequest)
	}
	return p
}

// modifyRequest runs all registered modifiers in order
func (p *requestPolicy) modifyRequest(r *http.Request) {
	for _, m := range p.modifiers {
		m(r)
	}
}
