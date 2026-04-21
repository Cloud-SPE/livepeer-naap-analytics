package resolver

import "strings"

func normalizeObservedHints(pipelineHint, modelHint string) (string, string) {
	pipeline := normalizeCanonicalPipeline(pipelineHint)
	model := normalizeCompatibilityModelHint(modelHint)

	if model == "" {
		if aliasModel, aliasPipeline := normalizeModelLikePipelineHint(pipelineHint); aliasModel != "" {
			model = aliasModel
			if pipeline == "" {
				pipeline = aliasPipeline
			}
		}
	}

	if pipeline == "" && model != "" {
		pipeline = inferCanonicalPipelineFromModel(model)
	}
	if pipeline != "" && model != "" && strings.EqualFold(strings.TrimSpace(pipeline), strings.TrimSpace(model)) {
		pipeline = ""
	}
	return pipeline, model
}

func normalizeCanonicalPipeline(v string) string {
	switch strings.ToLower(strings.TrimSpace(v)) {
	case "live-video-to-video",
		"text-to-image", "image-to-image", "image-to-video", "text-to-video",
		"audio-to-text", "text-to-speech",
		"upscale", "llm", "image-to-text", "segment-anything-2",
		"noop":
		return strings.ToLower(strings.TrimSpace(v))
	default:
		return ""
	}
}

func normalizeCompatibilityModelHint(v string) string {
	v = strings.TrimSpace(v)
	if v == "" {
		return ""
	}
	switch {
	case strings.EqualFold(v, "streamdiffusion-sdxl"):
		return "streamdiffusion-sdxl"
	case strings.EqualFold(v, "streamdiffusion-sdxl-v2v"):
		return "streamdiffusion-sdxl-v2v"
	case strings.EqualFold(v, "streamdiffusion-sdxl-faceid"):
		return "streamdiffusion-sdxl-faceid"
	case strings.EqualFold(v, "streamdiffusion"):
		return "streamdiffusion"
	default:
		return v
	}
}

func normalizeModelLikePipelineHint(v string) (string, string) {
	switch strings.ToLower(strings.TrimSpace(v)) {
	case "streamdiffusion-sdxl":
		return "streamdiffusion-sdxl", "live-video-to-video"
	case "streamdiffusion-sdxl-v2v":
		return "streamdiffusion-sdxl-v2v", "live-video-to-video"
	case "streamdiffusion-sdxl-faceid":
		return "streamdiffusion-sdxl-faceid", "live-video-to-video"
	case "streamdiffusion":
		return "streamdiffusion", "live-video-to-video"
	case "pip_sdxl-turbo", "pip_sd-turbo":
		return "streamdiffusion-sdxl", "live-video-to-video"
	case "pip_sdxl-turbo-v2v":
		return "streamdiffusion-sdxl-v2v", "live-video-to-video"
	case "pip_sdxl-turbo-faceid":
		return "streamdiffusion-sdxl-faceid", "live-video-to-video"
	default:
		return "", ""
	}
}

func inferCanonicalPipelineFromModel(model string) string {
	model = strings.TrimSpace(model)
	switch {
	case strings.HasPrefix(strings.ToLower(model), "streamdiffusion"):
		return "live-video-to-video"
	default:
		return ""
	}
}

