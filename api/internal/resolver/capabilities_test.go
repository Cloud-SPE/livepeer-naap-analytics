package resolver

import "testing"

func TestBuildCapabilityIntervalTemplates_AcceptsGPUInfoArray(t *testing.T) {
	raw := `{"hardware":[{"pipeline":"text-to-image","model_id":"sdxl","gpu_info":[{"id":"gpu-a","name":"L4","memory_total":24576}]}]}`

	templates := buildCapabilityIntervalTemplates(raw)
	if len(templates) != 1 {
		t.Fatalf("expected 1 template, got %d", len(templates))
	}
	if !templates[0].HardwarePresent {
		t.Fatalf("expected hardware present")
	}
	if templates[0].GPUID != "gpu-a" {
		t.Fatalf("unexpected gpu id %q", templates[0].GPUID)
	}
}

func TestBuildCapabilityIntervalTemplates_AcceptsGPUInfoObject(t *testing.T) {
	raw := `{"hardware":[{"pipeline":"live-video-to-video","model_id":"streamdiffusion","gpu_info":{"1":{"id":"gpu-b","name":"RTX 4090","memory_total":25757220864},"0":{"id":"gpu-a","name":"RTX 5090","memory_total":34190917632}}}]}`

	templates := buildCapabilityIntervalTemplates(raw)
	if len(templates) != 2 {
		t.Fatalf("expected 2 templates, got %d", len(templates))
	}
	if !templates[0].HardwarePresent || !templates[1].HardwarePresent {
		t.Fatalf("expected all templates to have hardware present")
	}
	if templates[0].GPUID != "gpu-a" || templates[1].GPUID != "gpu-b" {
		t.Fatalf("unexpected gpu ordering: %#v", templates)
	}
}
