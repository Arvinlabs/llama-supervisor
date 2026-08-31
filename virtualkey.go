package main

import (
	"log"
	"net/http"
	"strings"
)

// virtualKeyPolicy is a request plugin (requestPlugin + inboundChecker) implementing
// virtual API key authentication: clients present one of the
// configured virtual keys in the OpenAI format ("Authorization: Bearer <key>", the
// llama.cpp-style raw header value and the api_key query parameter are accepted too).
// On a match the outbound request is re-signed with the real backend API key, so the
// virtual keys never reach the backend; a missing or unknown key is rejected with an
// OpenAI-format 401 error.
type virtualKeyPolicy struct {
	keys       map[string]struct{}
	backendKey string
}

// compile-time proof that virtualKeyPolicy is a request plugin with inbound checking
var (
	_ requestPlugin  = (*virtualKeyPolicy)(nil)
	_ inboundChecker = (*virtualKeyPolicy)(nil)
)


// newVirtualKeyPolicy builds the policy from the configured key list (empty entries ignored)
func newVirtualKeyPolicy(keys []string, backendKey string) *virtualKeyPolicy {
	set := make(map[string]struct{}, len(keys))
	for _, k := range keys {
		if k != "" {
			set[k] = struct{}{}
		}
	}
	return &virtualKeyPolicy{keys: set, backendKey: backendKey}
}

// clientKey extracts the key the client presented (empty string when none was sent)
func (p *virtualKeyPolicy) clientKey(r *http.Request) string {
	if h := r.Header.Get("Authorization"); h != "" {
		// OpenAI format: "Bearer <key>"; llama.cpp also accepts the raw key without the prefix
		if k, ok := strings.CutPrefix(h, "Bearer "); ok {
			return k
		}
		return h
	}
	// llama.cpp-style api_key query parameter
	return r.URL.Query().Get("api_key")
}

// authorize reports whether the presented key is one of the configured virtual keys,
// logging the rejection on failure
func (p *virtualKeyPolicy) authorize(r *http.Request) bool {
	if _, ok := p.keys[p.clientKey(r)]; ok {
		return true
	}
	log.Printf("[auth] rejected %s %s from %s: missing or unknown virtual key",
		r.Method, r.URL.RequestURI(), r.RemoteAddr)
	return false
}

// modifyRequest re-signs the outbound request with the real backend API key: the virtual
// key is stripped, and if a backend key is configured it is sent as Bearer <key>, otherwise
// no Authorization header is sent at all. The api_key query parameter is dropped too
func (p *virtualKeyPolicy) modifyRequest(r *http.Request) {
	r.Header.Del("Authorization")
	if p.backendKey != "" {
		r.Header.Set("Authorization", "Bearer "+p.backendKey)
	}
	if q := r.URL.Query(); q.Has("api_key") {
		q.Del("api_key")
		r.URL.RawQuery = q.Encode()
	}
}

