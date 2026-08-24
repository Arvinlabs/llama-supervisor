package main

import (
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"
)

// modifiers run in registration order
func TestRequestPolicyAppliesModifiersInOrder(t *testing.T) {
	var order []string
	p := &requestPolicy{
		modifiers: []func(*http.Request){
			func(r *http.Request) { order = append(order, "first") },
			func(r *http.Request) { order = append(order, "second") },
		},
	}
	r := httptest.NewRequest(http.MethodPost, "/x", nil)
	p.modifyRequest(r)
	if !reflect.DeepEqual(order, []string{"first", "second"}) {
		t.Fatalf("modifier order = %v", order)
	}
}

// only enabled sub-features register a modifier
func TestNewRequestPolicyRegistersEnabledFeatures(t *testing.T) {
	if p := newRequestPolicy(&RequestGroup{PrefixCache: true}); len(p.modifiers) != 1 {
		t.Fatalf("expected 1 modifier with prefixCache enabled, got %d", len(p.modifiers))
	}
	if p := newRequestPolicy(&RequestGroup{}); len(p.modifiers) != 0 {
		t.Fatalf("expected no modifier with nothing enabled, got %d", len(p.modifiers))
	}
}
