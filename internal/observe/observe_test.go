package observe

import "testing"

// recorder collects the observations delivered to it
type recorder struct{ obs []Observation }

func (r *recorder) OnCompletion(o Observation) { r.obs = append(r.obs, o) }

func TestDraftRateAndHasDraft(t *testing.T) {
	if (Observation{}).HasDraft() {
		t.Fatal("zero observation must not have draft data")
	}
	if (Observation{}).DraftRate() != 0 {
		t.Fatal("zero observation draft rate must be 0")
	}
	o := Observation{DraftN: 1640, DraftAccepted: 1270}
	if !o.HasDraft() {
		t.Fatal("observation with draft_n>0 must have draft data")
	}
	if got := o.DraftRate(); got < 1270.0/1640.0-1e-9 || got > 1270.0/1640.0+1e-9 {
		t.Fatalf("draft rate = %v, want %v", got, 1270.0/1640.0)
	}
	if (Observation{DraftN: 0, DraftAccepted: 5}).HasDraft() {
		t.Fatal("draft_n=0 must not report draft data even with draft_n_accepted>0")
	}
}

func TestHubRegisterNotify(t *testing.T) {
	h := &Hub{}
	h.Register(nil) // nil is ignored
	if h.Len() != 0 {
		t.Fatalf("len after nil register = %d, want 0", h.Len())
	}
	a, b := &recorder{}, &recorder{}
	h.Register(a)
	h.Register(b)
	if h.Len() != 2 {
		t.Fatalf("len = %d, want 2", h.Len())
	}
	h.Notify(Observation{Total: 7})
	h.Notify(Observation{Total: 9})
	// every consumer sees every observation, in order
	for i, r := range []*recorder{a, b} {
		if len(r.obs) != 2 || r.obs[0].Total != 7 || r.obs[1].Total != 9 {
			t.Fatalf("consumer %d got %+v, want two observations (7 then 9)", i, r.obs)
		}
	}
	// no consumers: Notify is a safe no-op
	(&Hub{}).Notify(Observation{})
}
