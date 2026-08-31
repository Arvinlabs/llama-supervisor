package request

import (
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"

	"github.com/Arvinlabs/llama-supervisor/internal/config"
)

// stubPlugin adapts a plain modifier function into a request plugin
type stubPlugin struct{ modify func(*http.Request) }

func (s stubPlugin) ModifyRequest(r *http.Request) { s.modify(r) }

// plugin modifiers run in registration order
func TestRequestPolicyAppliesModifiersInOrder(t *testing.T) {
	var order []string
	p := &Policy{}
	p.register(stubPlugin{func(r *http.Request) { order = append(order, "first") }})
	p.register(stubPlugin{func(r *http.Request) { order = append(order, "second") }})
	r := httptest.NewRequest(http.MethodPost, "/x", nil)
	p.ModifyRequest(r)
	if !reflect.DeepEqual(order, []string{"first", "second"}) {
		t.Fatalf("modifier order = %v", order)
	}
}

// Authorize passes through for plugins without inbound checking (plain modifiers only)
func TestRequestPolicyAuthorizeWithoutInboundChecker(t *testing.T) {
	p := &Policy{}
	p.register(stubPlugin{func(r *http.Request) {}})
	if !p.Authorize(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/", nil)) {
		t.Fatal("expected Authorize to pass without inbound checkers")
	}
}

// only enabled sub-features register a plugin
func TestNewRequestPolicyRegistersEnabledFeatures(t *testing.T) {
	if p := New(&config.RequestGroup{PrefixCache: true}, ""); len(p.plugins) != 1 {
		t.Fatalf("expected 1 plugin with prefixCache enabled, got %d", len(p.plugins))
	}
	if p := New(&config.RequestGroup{VirtualKeys: []string{"k1"}}, ""); len(p.plugins) != 1 {
		t.Fatalf("expected 1 plugin with virtualKeys enabled, got %d", len(p.plugins))
	}
	if p := New(&config.RequestGroup{VirtualKeys: []string{"k1"}, PrefixCache: true}, ""); len(p.plugins) != 2 {
		t.Fatalf("expected 2 plugins with both enabled, got %d", len(p.plugins))
	}
	if p := New(&config.RequestGroup{}, ""); len(p.plugins) != 0 {
		t.Fatalf("expected no plugin with nothing enabled, got %d", len(p.plugins))
	}
	// Authorize passes through when the feature is disabled
	if p := New(&config.RequestGroup{}, ""); !p.Authorize(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/", nil)) {
		t.Fatal("expected Authorize to pass with virtual keys disabled")
	}
}
