package api

import (
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/daydemir/stoarama/backend/internal/util"
)

func TestNASHostPressureBoundedOptionalDecode(t *testing.T) {
	raw := `{"machine":"x86_64","mem_total_bytes":1000,"memory_diagnostics":{"meminfo_bytes":{"Cached":512,"Dirty":0,"Slab":"bad","Shmem":-1,"Buffers":1.5,"MemFree":9223372036854775808},"proc_stat":{"procs_running":10,"procs_blocked":4,"other":7},"vmstat":{"pswpin":2,"pgmajfault":7,"pgscan_kswapd_dma32":9,"pgsteal_kswapd":11,"pgscan_direct":5}}}`
	var host nasHostFacts
	if err := json.Unmarshal([]byte(raw), &host); err != nil {
		t.Fatal(err)
	}
	p := host.MemoryDiagnostics
	if host.Machine != "x86_64" || host.MemTotalBytes != 1000 || p == nil || len(p.MeminfoBytes) != 2 || p.MeminfoBytes["Cached"] != 512 || len(p.ProcStat) != 2 || p.ProcStat["procs_blocked"] != 4 || len(p.VMStat) != 4 || p.VMStat["pgscan_kswapd_dma32"] != 9 {
		t.Fatalf("unexpected facts: %+v pressure=%+v", host, p)
	}
	encoded, err := json.Marshal(host)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(encoded), `"pgsteal_kswapd":11`) {
		t.Fatalf("diagnostics not serialized: %s", encoded)
	}
	for _, bad := range []string{`42`, `"bad"`, `[]`, `{"proc_stat":false}`, `{"vmstat":{"pswpin":null,"pswpout":true,"pgmajfault":1e2}}`, `{"x":"` + strings.Repeat("a", 4096) + `"}`} {
		request := httptest.NewRequest("POST", "/", strings.NewReader(`{"host":{"machine":"x86_64","memory_diagnostics":`+bad+`}}`))
		var hb connectionHeartbeatRequest
		if err := util.DecodeJSON(request, &hb); err != nil {
			t.Fatalf("optional diagnostics rejected heartbeat for %s: %v", bad[:min(len(bad), 40)], err)
		}
		if hb.Host == nil || hb.Host.Machine != "x86_64" || validateNASHostFacts(hb.Host) != nil {
			t.Fatalf("valid host lost for %s", bad[:min(len(bad), 40)])
		}
	}
	request := httptest.NewRequest("POST", "/", strings.NewReader(`{"host":{"unknown_fact":3}}`))
	var hb connectionHeartbeatRequest
	if util.DecodeJSON(request, &hb) == nil {
		t.Fatal("global host decoding became permissive")
	}
}

func TestNASHostPressureVMFieldCap(t *testing.T) {
	fields := make([]string, 0, 30)
	for n := 0; n < 30; n++ {
		fields = append(fields, fmt.Sprintf(`"pgscan_kswapd_%02d":%d`, n, n))
	}
	var p nasHostPressure
	if err := json.Unmarshal([]byte(`{"vmstat":{`+strings.Join(fields, ",")+`}}`), &p); err != nil {
		t.Fatal(err)
	}
	if len(p.VMStat) != 20 {
		t.Fatalf("unbounded vmstat fields: %d", len(p.VMStat))
	}
	if _, ok := p.VMStat["pgscan_kswapd_19"]; !ok {
		t.Fatal("cap order not deterministic")
	}
	if _, ok := p.VMStat["pgscan_kswapd_20"]; ok {
		t.Fatal("cap exceeded")
	}
}
