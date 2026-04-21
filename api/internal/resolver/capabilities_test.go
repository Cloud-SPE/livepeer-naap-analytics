package resolver

import "testing"

func TestBuiltinCapabilityByNumber_UsesRepoOwnedCatalog(t *testing.T) {
	entry, ok := builtinCapabilityByNumber("35")
	if !ok {
		t.Fatalf("expected capability 35 to resolve from repo-owned catalog")
	}
	if entry.CanonicalPipeline != "live-video-to-video" {
		t.Fatalf("canonical pipeline = %q, want live-video-to-video", entry.CanonicalPipeline)
	}
	if !entry.SupportsStream || entry.SupportsRequest {
		t.Fatalf("unexpected capability 35 support flags: %+v", entry)
	}
}

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

func TestBuildCapabilityIntervalTemplates_PerCapabilityPath_ProducesHardwareLessTemplates(t *testing.T) {
	// AI batch orchestrator: hardware null, capabilities.constraints.PerCapability present.
	raw := `{
		"local_address": "0xabc",
		"hardware": null,
		"capabilities": {
			"constraints": {
				"PerCapability": {
					"27": {"models": {"sdxl-turbo": {"warm": true, "capacity": 1}}},
					"33": {"models": {"llama-3.1-8b": {"warm": true, "capacity": 2}, "llama-3.3-70b": {"warm": false, "capacity": 1}}}
				}
			}
		}
	}`

	templates := buildCapabilityIntervalTemplates(raw)
	if len(templates) == 0 {
		t.Fatalf("expected at least one template from PerCapability, got 0")
	}
	for _, tmpl := range templates {
		if tmpl.HardwarePresent {
			t.Fatalf("PerCapability path must produce hardware-less templates, got HardwarePresent=true for pipeline=%q", tmpl.Pipeline)
		}
		if tmpl.GPUID != "" {
			t.Fatalf("PerCapability path must produce templates with no GPU ID, got %q", tmpl.GPUID)
		}
	}

	pipelineModels := make(map[string]string)
	for _, tmpl := range templates {
		pipelineModels[tmpl.Pipeline] = tmpl.Model
	}
	if _, ok := pipelineModels["text-to-image"]; !ok {
		t.Fatalf("expected text-to-image template (cap 27), got pipelines: %v", pipelineModels)
	}
	if _, ok := pipelineModels["llm"]; !ok {
		t.Fatalf("expected llm template (cap 33), got pipelines: %v", pipelineModels)
	}
}

func TestBuildCapabilityIntervalTemplates_PerCapabilityPath_IgnoresBYOCCapability37(t *testing.T) {
	// BYOC capability ids are not built-in offers. They must come from
	// hardware[] rather than PerCapability.
	raw := `{
		"hardware": null,
		"capabilities": {
			"constraints": {
				"PerCapability": {
					"37": {"models": {"openai-chat-completions": {"warm": true, "capacity": 1}}}
				}
			}
		}
	}`

	templates := buildCapabilityIntervalTemplates(raw)
	// Should fall back to the hardware-less placeholder since cap 37 is excluded.
	if len(templates) != 1 {
		t.Fatalf("expected 1 hardware-less placeholder, got %d", len(templates))
	}
	if templates[0].Pipeline != "" {
		t.Fatalf("placeholder pipeline should be empty, got %q", templates[0].Pipeline)
	}
}

func TestBuildCapabilityIntervalTemplates_PerCapabilityPath_WarmerModelsFirst(t *testing.T) {
	// Warm models should come before cold models in the template list.
	raw := `{
		"hardware": null,
		"capabilities": {
			"constraints": {
				"PerCapability": {
					"33": {
						"models": {
							"cold-model": {"warm": false, "capacity": 1},
							"warm-model": {"warm": true, "capacity": 1}
						}
					}
				}
			}
		}
	}`

	templates := buildCapabilityIntervalTemplates(raw)
	if len(templates) < 2 {
		t.Fatalf("expected 2 templates, got %d", len(templates))
	}
	if templates[0].Model != "warm-model" {
		t.Fatalf("first model = %q, want warm-model", templates[0].Model)
	}
	if templates[1].Model != "cold-model" {
		t.Fatalf("second model = %q, want cold-model", templates[1].Model)
	}
}

func TestBuildCapabilityIntervalTemplates_FallbackPlaceholderWhenNeitherHardwareNorPerCapability(t *testing.T) {
	// No hardware, no capabilities block at all.
	raw := `{"local_address": "0xabc"}`

	templates := buildCapabilityIntervalTemplates(raw)
	if len(templates) != 1 {
		t.Fatalf("expected 1 placeholder, got %d", len(templates))
	}
	if templates[0].HardwarePresent {
		t.Fatalf("fallback placeholder must be hardware-less")
	}
	if templates[0].Pipeline != "" || templates[0].Model != "" {
		t.Fatalf("fallback placeholder must have no pipeline or model")
	}
}

func TestBuildCapabilityIntervalTemplates_HardwarePathTakesPrecedenceOverPerCapability(t *testing.T) {
	// When hardware entries are present, Path 1 is used even if PerCapability also exists.
	raw := `{
		"hardware": [{"pipeline": "live-video-to-video", "model_id": "streamdiffusion", "gpu_info": [{"id": "gpu-a", "name": "L4", "memory_total": 24576}]}],
		"capabilities": {
			"constraints": {
				"PerCapability": {
					"27": {"models": {"sdxl": {"warm": true, "capacity": 1}}}
				}
			}
		}
	}`

	templates := buildCapabilityIntervalTemplates(raw)
	if len(templates) != 1 {
		t.Fatalf("expected 1 template from hardware path, got %d", len(templates))
	}
	if !templates[0].HardwarePresent {
		t.Fatalf("hardware path must produce HardwarePresent=true")
	}
	if templates[0].Pipeline != "live-video-to-video" {
		t.Fatalf("pipeline = %q, want live-video-to-video", templates[0].Pipeline)
	}
}
