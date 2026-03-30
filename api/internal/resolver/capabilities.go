package resolver

import (
	"bytes"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"
)

type capabilityPayload struct {
	LocalAddress string                    `json:"local_address"`
	Hardware     []capabilityHardwareEntry `json:"hardware"`
}

type capabilityHardwareEntry struct {
	Pipeline string                `json:"pipeline"`
	ModelID  string                `json:"model_id"`
	GPUInfo  capabilityGPUInfoList `json:"gpu_info"`
}

type capabilityGPUInfo struct {
	ID          string `json:"id"`
	Name        string `json:"name"`
	MemoryTotal uint64 `json:"memory_total"`
}

type capabilityGPUInfoList []capabilityGPUInfo

func (l *capabilityGPUInfoList) UnmarshalJSON(data []byte) error {
	trimmed := bytes.TrimSpace(data)
	if len(trimmed) == 0 || bytes.Equal(trimmed, []byte("null")) {
		*l = nil
		return nil
	}

	if trimmed[0] == '[' {
		var out []capabilityGPUInfo
		if err := json.Unmarshal(trimmed, &out); err != nil {
			return err
		}
		*l = out
		return nil
	}

	if trimmed[0] == '{' {
		var keyed map[string]capabilityGPUInfo
		if err := json.Unmarshal(trimmed, &keyed); err != nil {
			return err
		}
		keys := make([]string, 0, len(keyed))
		for key := range keyed {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		out := make([]capabilityGPUInfo, 0, len(keys))
		for _, key := range keys {
			out = append(out, keyed[key])
		}
		*l = out
		return nil
	}

	return fmt.Errorf("unsupported gpu_info shape: %s", string(trimmed))
}

type capabilityIntervalTemplate struct {
	Pipeline        string
	Model           string
	GPUID           string
	GPUModelName    string
	GPUMemoryTotal  *uint64
	HardwarePresent bool
	HashSuffix      string
}

func buildCapabilityVersions(rows []capabilitySnapshot) []CapabilityVersion {
	if len(rows) == 0 {
		return nil
	}
	sortCapabilitySnapshots(rows)
	versions := make([]CapabilityVersion, 0, len(rows))

	var currentKey string
	var lastHash string
	var rank uint32
	for _, row := range rows {
		key := row.Org + "\x00" + row.OrchAddress + "\x00" + row.OrchURINorm
		if key != currentKey {
			currentKey = key
			lastHash = ""
			rank = 0
		}
		payloadHash := stableHash(row.RawPayload)
		if payloadHash == lastHash {
			continue
		}
		rank++
		versions = append(versions, CapabilityVersion{
			ID:              stableHash(row.Org, row.OrchAddress, row.OrchURINorm, row.EventID, payloadHash),
			Org:             row.Org,
			OrchAddress:     strings.ToLower(strings.TrimSpace(row.OrchAddress)),
			OrchURI:         strings.TrimSpace(row.OrchURI),
			OrchURINorm:     normalizeURL(row.OrchURINorm),
			LocalAddress:    capabilityAliasAddress(row.OrchAddress, row.LocalAddress),
			SnapshotEventID: row.EventID,
			SnapshotTS:      row.EventTS.UTC(),
			PayloadHash:     payloadHash,
			RawCapabilities: row.RawPayload,
			IsNoop:          false,
			VersionRank:     rank,
		})
		lastHash = payloadHash
	}
	return versions
}

func buildCapabilityIntervals(rows []CapabilityVersion) []CapabilityInterval {
	if len(rows) == 0 {
		return nil
	}
	out := make([]CapabilityInterval, 0, len(rows))
	templatesByPayload := make(map[string][]capabilityIntervalTemplate, len(rows)/4)
	for idx, row := range rows {
		var next *CapabilityVersion
		if idx+1 < len(rows) &&
			rows[idx+1].Org == row.Org &&
			rows[idx+1].OrchAddress == row.OrchAddress &&
			rows[idx+1].OrchURINorm == row.OrchURINorm {
			next = &rows[idx+1]
		}
		var validTo *time.Time
		if next != nil {
			ts := next.SnapshotTS.UTC()
			validTo = &ts
		}

		templates, ok := templatesByPayload[row.PayloadHash]
		if !ok {
			templates = buildCapabilityIntervalTemplates(row.RawCapabilities)
			templatesByPayload[row.PayloadHash] = templates
		}
		for _, tmpl := range templates {
			out = append(out, CapabilityInterval{
				VersionID:       row.ID,
				Org:             row.Org,
				OrchAddress:     row.OrchAddress,
				AliasAddress:    row.LocalAddress,
				OrchURI:         row.OrchURI,
				OrchURINorm:     row.OrchURINorm,
				ValidFromTS:     row.SnapshotTS.UTC(),
				ValidToTS:       validTo,
				Pipeline:        tmpl.Pipeline,
				Model:           tmpl.Model,
				GPUID:           tmpl.GPUID,
				GPUModelName:    tmpl.GPUModelName,
				GPUMemoryTotal:  tmpl.GPUMemoryTotal,
				HardwarePresent: tmpl.HardwarePresent,
				IntervalHash:    stableHash(row.ID, tmpl.HashSuffix),
				SnapshotEventID: row.SnapshotEventID,
				SnapshotTS:      row.SnapshotTS.UTC(),
			})
		}
	}
	return out
}

func buildCapabilityIntervalTemplates(raw string) []capabilityIntervalTemplate {
	payload, err := parseCapabilityPayload(raw)
	if err != nil {
		return []capabilityIntervalTemplate{{
			HardwarePresent: false,
			HashSuffix:      stableHash("hardware-less", err.Error()),
		}}
	}
	if len(payload.Hardware) == 0 {
		return []capabilityIntervalTemplate{{
			HardwarePresent: false,
			HashSuffix:      stableHash("hardware-less"),
		}}
	}

	out := make([]capabilityIntervalTemplate, 0, len(payload.Hardware))
	for _, hw := range payload.Hardware {
		pipeline := strings.TrimSpace(hw.Pipeline)
		model := strings.TrimSpace(hw.ModelID)
		if len(hw.GPUInfo) == 0 {
			out = append(out, capabilityIntervalTemplate{
				Pipeline:        pipeline,
				Model:           model,
				HardwarePresent: false,
				HashSuffix:      stableHash(pipeline, model, "hardware-less"),
			})
			continue
		}
		for _, gpu := range hw.GPUInfo {
			mem := gpu.MemoryTotal
			out = append(out, capabilityIntervalTemplate{
				Pipeline:        pipeline,
				Model:           model,
				GPUID:           strings.TrimSpace(gpu.ID),
				GPUModelName:    strings.TrimSpace(gpu.Name),
				GPUMemoryTotal:  &mem,
				HardwarePresent: true,
				HashSuffix:      stableHash(pipeline, model, gpu.ID, fmt.Sprintf("%d", gpu.MemoryTotal)),
			})
		}
	}
	return out
}

func parseCapabilityPayload(raw string) (capabilityPayload, error) {
	var payload capabilityPayload
	if strings.TrimSpace(raw) == "" {
		return payload, nil
	}
	if err := json.Unmarshal([]byte(raw), &payload); err != nil {
		return payload, err
	}
	return payload, nil
}

func capabilityAliasAddress(orchAddress, candidate string) string {
	candidate = normalizeAddress(candidate)
	if !isHexAddressIdentity(candidate) {
		return ""
	}
	if candidate == normalizeAddress(orchAddress) {
		return ""
	}
	return candidate
}
