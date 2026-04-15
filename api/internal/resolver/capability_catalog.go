package resolver

// capabilityCatalogEntry is the repo-owned copy of the upstream AI capability
// semantics we depend on for resolver-side normalization. Keep it aligned with
// warehouse/seeds/capability_catalog.csv when new capability ids are adopted.
type capabilityCatalogEntry struct {
	CapabilityID      uint16
	CapabilityName    string
	CapabilityFamily  string
	CanonicalPipeline string
	ConstraintKind    string
	SupportsStream    bool
	SupportsRequest   bool
}

var capabilityCatalogEntries = []capabilityCatalogEntry{
	{CapabilityID: 27, CapabilityName: "text-to-image", CapabilityFamily: "builtin", CanonicalPipeline: "text-to-image", ConstraintKind: "model_id", SupportsRequest: true},
	{CapabilityID: 28, CapabilityName: "image-to-image", CapabilityFamily: "builtin", CanonicalPipeline: "image-to-image", ConstraintKind: "model_id", SupportsRequest: true},
	{CapabilityID: 29, CapabilityName: "image-to-video", CapabilityFamily: "builtin", CanonicalPipeline: "image-to-video", ConstraintKind: "model_id", SupportsRequest: true},
	{CapabilityID: 30, CapabilityName: "upscale", CapabilityFamily: "builtin", CanonicalPipeline: "upscale", ConstraintKind: "model_id", SupportsRequest: true},
	{CapabilityID: 31, CapabilityName: "audio-to-text", CapabilityFamily: "builtin", CanonicalPipeline: "audio-to-text", ConstraintKind: "model_id", SupportsRequest: true},
	{CapabilityID: 32, CapabilityName: "segment-anything-2", CapabilityFamily: "builtin", CanonicalPipeline: "segment-anything-2", ConstraintKind: "model_id", SupportsRequest: true},
	{CapabilityID: 33, CapabilityName: "llm", CapabilityFamily: "builtin", CanonicalPipeline: "llm", ConstraintKind: "model_id", SupportsRequest: true},
	{CapabilityID: 34, CapabilityName: "image-to-text", CapabilityFamily: "builtin", CanonicalPipeline: "image-to-text", ConstraintKind: "model_id", SupportsRequest: true},
	{CapabilityID: 35, CapabilityName: "live-video-to-video", CapabilityFamily: "builtin", CanonicalPipeline: "live-video-to-video", ConstraintKind: "model_id", SupportsStream: true},
	{CapabilityID: 36, CapabilityName: "text-to-speech", CapabilityFamily: "builtin", CanonicalPipeline: "text-to-speech", ConstraintKind: "model_id", SupportsRequest: true},
	{CapabilityID: 37, CapabilityName: "byoc", CapabilityFamily: "byoc", ConstraintKind: "external_capability_name", SupportsStream: true, SupportsRequest: true},
}

var capabilityCatalogByID = func() map[uint16]capabilityCatalogEntry {
	out := make(map[uint16]capabilityCatalogEntry, len(capabilityCatalogEntries))
	for _, entry := range capabilityCatalogEntries {
		out[entry.CapabilityID] = entry
	}
	return out
}()

func builtinCapabilityByNumber(capabilityNumber string) (capabilityCatalogEntry, bool) {
	switch capabilityNumber {
	case "27":
		return capabilityCatalogByID[27], true
	case "28":
		return capabilityCatalogByID[28], true
	case "29":
		return capabilityCatalogByID[29], true
	case "30":
		return capabilityCatalogByID[30], true
	case "31":
		return capabilityCatalogByID[31], true
	case "32":
		return capabilityCatalogByID[32], true
	case "33":
		return capabilityCatalogByID[33], true
	case "34":
		return capabilityCatalogByID[34], true
	case "35":
		return capabilityCatalogByID[35], true
	case "36":
		return capabilityCatalogByID[36], true
	default:
		return capabilityCatalogEntry{}, false
	}
}
