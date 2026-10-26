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

// cloudRecorder opts in to cloud observations
type cloudRecorder struct{ recorder }

func (r *cloudRecorder) ConsumeCloud() bool { return true }

// localOnlyRecorder refuses cloud observations explicitly (same as a consumer
// that does not implement CloudConsumer at all)
type localOnlyRecorder struct{ recorder }

func (r *localOnlyRecorder) ConsumeCloud() bool { return false }

// a cloud observation is only delivered to the consumers that opted in: the
// watchdog (a plain Consumer) must never judge cloud traffic
func TestHubCloudFiltering(t *testing.T) {
	h := &Hub{}
	local, cloud, refused := &recorder{}, &cloudRecorder{}, &localOnlyRecorder{}
	h.Register(local)
	h.Register(cloud)
	h.Register(refused)
	if h.Len() != 3 {
		t.Fatalf("len = %d, want 3", h.Len())
	}
	if got := h.CloudLen(); got != 1 {
		t.Fatalf("cloud consumers = %d, want 1 (only the opting-in one)", got)
	}

	h.Notify(Observation{Total: 5})              // local: everyone
	h.Notify(Observation{Total: 9, Cloud: true}) // cloud: the opting-in consumer only
	h.Notify(Observation{Total: 11})             // local again
	if len(local.obs) != 2 || local.obs[0].Total != 5 || local.obs[1].Total != 11 {
		t.Fatalf("local-only consumer got %+v, want the two local observations", local.obs)
	}
	if len(refused.obs) != 2 {
		t.Fatalf("a consumer refusing cloud traffic got %+v, want the two local observations", refused.obs)
	}
	if len(cloud.obs) != 3 || !cloud.obs[1].Cloud || cloud.obs[1].Total != 9 {
		t.Fatalf("opting-in consumer got %+v, want all three observations with source tags", cloud.obs)
	}
}

// a cloud completion is never tracked by the stream consumers (id 0): its
// content and end are ignored, so the watchdog cannot see in-flight cloud output
func TestHubCloudStreamNotTracked(t *testing.T) {
	h := &Hub{}
	rec := &streamRecorder{}
	h.RegisterStream(rec)
	if id := h.StreamStart(true, true); id != 0 {
		t.Fatalf("cloud stream id = %d, want 0 (untracked)", id)
	}
	h.StreamContent(0, "cloud text")
	h.StreamEnd(0)
	if rec.starts != 0 || rec.contents != 0 || rec.ends != 0 {
		t.Fatalf("stream consumer saw a cloud completion: %+v", rec)
	}
	id := h.StreamStart(true, false)
	if id == 0 {
		t.Fatal("a local stream must be tracked")
	}
	h.StreamContent(id, "local text")
	h.StreamEnd(id)
	if rec.starts != 1 || rec.contents != 1 || rec.ends != 1 {
		t.Fatalf("stream consumer missed the local completion: %+v", rec)
	}
}

// streamRecorder counts the stream consumer calls
type streamRecorder struct{ starts, contents, ends int }

func (r *streamRecorder) OnStreamStart(_ int, _ bool)     { r.starts++ }
func (r *streamRecorder) OnStreamContent(_ int, _ string) { r.contents++ }
func (r *streamRecorder) OnStreamEnd(_ int)               { r.ends++ }
