package api

import (
	"bytes"
	"encoding/json"
	"sort"
	"strings"
)

// Optional diagnostics are isolated from strict heartbeat decoding: malformed
// counters are unobservable, never a reason to reject valid liveness or facts.
type nasHostPressure struct {
	MeminfoBytes map[string]int64 `json:"meminfo_bytes,omitempty"`
	ProcStat     map[string]int64 `json:"proc_stat,omitempty"`
	VMStat       map[string]int64 `json:"vmstat,omitempty"`
}

func validNASPressureCounter(section, name string) bool {
	switch section {
	case "meminfo_bytes":
		switch name {
		case "MemFree", "Buffers", "Cached", "SwapCached", "Active(anon)", "Inactive(anon)", "Active(file)", "Inactive(file)", "Dirty", "Writeback", "AnonPages", "Shmem", "Slab", "SReclaimable", "SUnreclaim", "SwapTotal", "SwapFree":
			return true
		}
	case "proc_stat":
		return name == "procs_running" || name == "procs_blocked"
	case "vmstat":
		if name == "pswpin" || name == "pswpout" || name == "pgmajfault" {
			return true
		}
		if len(name) > 32 || !(strings.HasPrefix(name, "pgscan_kswapd") || strings.HasPrefix(name, "pgsteal_kswapd")) {
			return false
		}
		for _, c := range name {
			if !(c >= 'a' && c <= 'z' || c >= '0' && c <= '9' || c == '_') {
				return false
			}
		}
		return true
	}
	return false
}

func (p *nasHostPressure) UnmarshalJSON(raw []byte) error {
	*p = nasHostPressure{}
	if len(raw) > 4096 {
		return nil
	}
	var sections map[string]json.RawMessage
	if json.Unmarshal(raw, &sections) != nil {
		return nil
	}
	for _, section := range []string{"meminfo_bytes", "proc_stat", "vmstat"} {
		var fields map[string]json.RawMessage
		if json.Unmarshal(sections[section], &fields) != nil {
			continue
		}
		keys := make([]string, 0, len(fields))
		for key := range fields {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		values := map[string]int64{}
		for _, key := range keys {
			if !validNASPressureCounter(section, key) {
				continue
			}
			v := bytes.TrimSpace(fields[key])
			if len(v) == 0 || len(v) > 19 || v[0] < '0' || v[0] > '9' {
				continue
			}
			var value int64
			if json.Unmarshal(v, &value) != nil || value < 0 {
				continue
			}
			if section == "vmstat" && len(values) >= 20 {
				break
			}
			values[key] = value
		}
		if len(values) == 0 {
			continue
		}
		switch section {
		case "meminfo_bytes":
			p.MeminfoBytes = values
		case "proc_stat":
			p.ProcStat = values
		case "vmstat":
			p.VMStat = values
		}
	}
	return nil
}
