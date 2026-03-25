package enrichment

import (
	"testing"
	"time"
)

var testLastSeen = time.Date(2026, 3, 25, 0, 0, 0, 0, time.UTC)

// ── parseGPURows happy-path ───────────────────────────────────────────────────

func TestParseGPURows_SingleOrchSingleGPU(t *testing.T) {
	orchs := []orchRow{{
		OrchAddress:     "0xabc",
		Org:             "daydream",
		RawCapabilities: `{"hardware":[{"pipeline":"streamdiffusion","model_id":"sdxl","gpu_info":{"0":{"id":"GPU-1","name":"RTX 4090","memory_total":25769803776}}}]}`,
		LastSeen:        testLastSeen,
	}}

	got := parseGPURows(orchs)

	if len(got) != 1 {
		t.Fatalf("want 1 row, got %d", len(got))
	}
	r := got[0]
	if r.GPUID != "GPU-1" {
		t.Errorf("GPUID: want GPU-1, got %q", r.GPUID)
	}
	if r.OrchAddress != "0xabc" {
		t.Errorf("OrchAddress: want 0xabc, got %q", r.OrchAddress)
	}
	if r.Org != "daydream" {
		t.Errorf("Org: want daydream, got %q", r.Org)
	}
	if r.GPUModel != "RTX 4090" {
		t.Errorf("GPUModel: want RTX 4090, got %q", r.GPUModel)
	}
	if r.MemoryBytes != 25769803776 {
		t.Errorf("MemoryBytes: want 25769803776, got %d", r.MemoryBytes)
	}
	if r.Pipeline != "streamdiffusion" {
		t.Errorf("Pipeline: want streamdiffusion, got %q", r.Pipeline)
	}
	if r.ModelID != "sdxl" {
		t.Errorf("ModelID: want sdxl, got %q", r.ModelID)
	}
	if !r.LastSeen.Equal(testLastSeen) {
		t.Errorf("LastSeen: want %v, got %v", testLastSeen, r.LastSeen)
	}
}

func TestParseGPURows_MultipleGPUsPerOrch(t *testing.T) {
	orchs := []orchRow{{
		OrchAddress:     "0xabc",
		Org:             "cloudspe",
		RawCapabilities: `{"hardware":[{"pipeline":"p1","model_id":"m1","gpu_info":{"0":{"id":"GPU-A","name":"RTX 5090","memory_total":1000},"1":{"id":"GPU-B","name":"RTX 5090","memory_total":1000}}}]}`,
		LastSeen:        testLastSeen,
	}}

	got := parseGPURows(orchs)

	if len(got) != 2 {
		t.Fatalf("want 2 rows, got %d", len(got))
	}
	ids := map[string]bool{got[0].GPUID: true, got[1].GPUID: true}
	if !ids["GPU-A"] || !ids["GPU-B"] {
		t.Errorf("expected GPU-A and GPU-B, got %v", ids)
	}
}

func TestParseGPURows_MultipleOrchsMultipleGPUs(t *testing.T) {
	orchs := []orchRow{
		{
			OrchAddress:     "0x1",
			Org:             "daydream",
			RawCapabilities: `{"hardware":[{"pipeline":"p","model_id":"m","gpu_info":{"0":{"id":"G1","name":"A100","memory_total":80000}}}]}`,
			LastSeen:        testLastSeen,
		},
		{
			OrchAddress:     "0x2",
			Org:             "daydream",
			RawCapabilities: `{"hardware":[{"pipeline":"p","model_id":"m","gpu_info":{"0":{"id":"G2","name":"H100","memory_total":90000},"1":{"id":"G3","name":"H100","memory_total":90000}}}]}`,
			LastSeen:        testLastSeen,
		},
	}

	got := parseGPURows(orchs)

	if len(got) != 3 {
		t.Fatalf("want 3 rows, got %d", len(got))
	}
}

func TestParseGPURows_MultipleHardwareEntriesPerOrch(t *testing.T) {
	// One orch advertising two different pipelines, each with its own GPU slot.
	orchs := []orchRow{{
		OrchAddress: "0xabc",
		Org:         "daydream",
		RawCapabilities: `{"hardware":[
			{"pipeline":"text-to-image","model_id":"sdxl","gpu_info":{"0":{"id":"GPU-X","name":"RTX 4090","memory_total":500}}},
			{"pipeline":"image-to-video","model_id":"svd","gpu_info":{"0":{"id":"GPU-X","name":"RTX 4090","memory_total":500}}}
		]}`,
		LastSeen: testLastSeen,
	}}

	got := parseGPURows(orchs)

	// Same GPU-X appears in both hardware entries → two rows (one per pipeline).
	if len(got) != 2 {
		t.Fatalf("want 2 rows (same GPU, two pipelines), got %d", len(got))
	}
	pipelines := map[string]bool{got[0].Pipeline: true, got[1].Pipeline: true}
	if !pipelines["text-to-image"] || !pipelines["image-to-video"] {
		t.Errorf("expected both pipelines, got %v", pipelines)
	}
}

// ── parseGPURows edge cases ───────────────────────────────────────────────────

func TestParseGPURows_EmptyOrchList(t *testing.T) {
	got := parseGPURows(nil)
	if len(got) != 0 {
		t.Errorf("want 0 rows for nil input, got %d", len(got))
	}
}

func TestParseGPURows_EmptyRawCapabilities(t *testing.T) {
	orchs := []orchRow{{
		OrchAddress:     "0xabc",
		Org:             "daydream",
		RawCapabilities: "",
		LastSeen:        testLastSeen,
	}}
	got := parseGPURows(orchs)
	if len(got) != 0 {
		t.Errorf("want 0 rows for empty raw_capabilities, got %d", len(got))
	}
}

func TestParseGPURows_MalformedJSON(t *testing.T) {
	orchs := []orchRow{{
		OrchAddress:     "0xabc",
		Org:             "daydream",
		RawCapabilities: `{not valid json`,
		LastSeen:        testLastSeen,
	}}
	got := parseGPURows(orchs)
	if len(got) != 0 {
		t.Errorf("want 0 rows for malformed JSON, got %d", len(got))
	}
}

func TestParseGPURows_NullJSON(t *testing.T) {
	orchs := []orchRow{{
		OrchAddress:     "0xabc",
		Org:             "daydream",
		RawCapabilities: "null",
		LastSeen:        testLastSeen,
	}}
	got := parseGPURows(orchs)
	if len(got) != 0 {
		t.Errorf("want 0 rows for null JSON, got %d", len(got))
	}
}

func TestParseGPURows_EmptyHardwareArray(t *testing.T) {
	orchs := []orchRow{{
		OrchAddress:     "0xabc",
		Org:             "daydream",
		RawCapabilities: `{"hardware":[]}`,
		LastSeen:        testLastSeen,
	}}
	got := parseGPURows(orchs)
	if len(got) != 0 {
		t.Errorf("want 0 rows for empty hardware array, got %d", len(got))
	}
}

func TestParseGPURows_EmptyGPUInfoMap(t *testing.T) {
	orchs := []orchRow{{
		OrchAddress:     "0xabc",
		Org:             "daydream",
		RawCapabilities: `{"hardware":[{"pipeline":"p","model_id":"m","gpu_info":{}}]}`,
		LastSeen:        testLastSeen,
	}}
	got := parseGPURows(orchs)
	if len(got) != 0 {
		t.Errorf("want 0 rows for empty gpu_info map, got %d", len(got))
	}
}

func TestParseGPURows_SkipsGPUWithEmptyID(t *testing.T) {
	orchs := []orchRow{{
		OrchAddress:     "0xabc",
		Org:             "daydream",
		RawCapabilities: `{"hardware":[{"pipeline":"p","model_id":"m","gpu_info":{"0":{"id":"","name":"RTX 4090","memory_total":1000},"1":{"id":"GPU-VALID","name":"RTX 4090","memory_total":1000}}}]}`,
		LastSeen:        testLastSeen,
	}}

	got := parseGPURows(orchs)

	if len(got) != 1 {
		t.Fatalf("want 1 row (empty-ID GPU skipped), got %d", len(got))
	}
	if got[0].GPUID != "GPU-VALID" {
		t.Errorf("expected GPU-VALID, got %q", got[0].GPUID)
	}
}

func TestParseGPURows_MixedValidAndMalformedOrchsSinglePass(t *testing.T) {
	// Malformed orch in the middle should not affect valid orchs.
	orchs := []orchRow{
		{OrchAddress: "0x1", Org: "a", RawCapabilities: `{"hardware":[{"pipeline":"p","model_id":"m","gpu_info":{"0":{"id":"G1","name":"A","memory_total":100}}}]}`, LastSeen: testLastSeen},
		{OrchAddress: "0x2", Org: "a", RawCapabilities: `{bad`, LastSeen: testLastSeen},
		{OrchAddress: "0x3", Org: "a", RawCapabilities: `{"hardware":[{"pipeline":"p","model_id":"m","gpu_info":{"0":{"id":"G3","name":"C","memory_total":300}}}]}`, LastSeen: testLastSeen},
	}

	got := parseGPURows(orchs)

	if len(got) != 2 {
		t.Fatalf("want 2 rows (malformed orch skipped), got %d", len(got))
	}
}

func TestParseGPURows_ZeroMemoryIsAccepted(t *testing.T) {
	orchs := []orchRow{{
		OrchAddress:     "0xabc",
		Org:             "daydream",
		RawCapabilities: `{"hardware":[{"pipeline":"p","model_id":"m","gpu_info":{"0":{"id":"G1","name":"Unknown","memory_total":0}}}]}`,
		LastSeen:        testLastSeen,
	}}
	got := parseGPURows(orchs)
	if len(got) != 1 {
		t.Fatalf("want 1 row for zero memory GPU, got %d", len(got))
	}
	if got[0].MemoryBytes != 0 {
		t.Errorf("want MemoryBytes=0, got %d", got[0].MemoryBytes)
	}
}

func TestParseGPURows_ColumnOrderMatchesTableDefinition(t *testing.T) {
	// Verifies the struct fields match the column order in migration 014:
	// gpu_id, orch_address, org, gpu_model, memory_bytes, pipeline, model_id, last_seen
	ts := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	orchs := []orchRow{{
		OrchAddress:     "0xdeadbeef",
		Org:             "test-org",
		RawCapabilities: `{"hardware":[{"pipeline":"test-pipeline","model_id":"test-model","gpu_info":{"0":{"id":"test-gpu-id","name":"Test GPU","memory_total":42}}}]}`,
		LastSeen:        ts,
	}}

	got := parseGPURows(orchs)

	if len(got) != 1 {
		t.Fatalf("want 1 row, got %d", len(got))
	}
	r := got[0]
	if r.GPUID != "test-gpu-id" {
		t.Errorf("col 1 GPUID mismatch: %q", r.GPUID)
	}
	if r.OrchAddress != "0xdeadbeef" {
		t.Errorf("col 2 OrchAddress mismatch: %q", r.OrchAddress)
	}
	if r.Org != "test-org" {
		t.Errorf("col 3 Org mismatch: %q", r.Org)
	}
	if r.GPUModel != "Test GPU" {
		t.Errorf("col 4 GPUModel mismatch: %q", r.GPUModel)
	}
	if r.MemoryBytes != 42 {
		t.Errorf("col 5 MemoryBytes mismatch: %d", r.MemoryBytes)
	}
	if r.Pipeline != "test-pipeline" {
		t.Errorf("col 6 Pipeline mismatch: %q", r.Pipeline)
	}
	if r.ModelID != "test-model" {
		t.Errorf("col 7 ModelID mismatch: %q", r.ModelID)
	}
	if !r.LastSeen.Equal(ts) {
		t.Errorf("col 8 LastSeen mismatch: want %v, got %v", ts, r.LastSeen)
	}
}
