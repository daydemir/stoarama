package main

import (
  "context"
  "encoding/json"
  "fmt"
  "net/http"
  "os"
  "os/exec"
  "strings"
  "time"
  "github.com/jackc/pgx/v5/pgxpool"
)

func emit(v any) { b,_:=json.Marshal(v); fmt.Println(string(b)) }
func main() {
  ctx:=context.Background(); p,e:=pgxpool.New(ctx,os.Getenv("DATABASE_URL")); if e!=nil { panic(e) }; defer p.Close()
  var fire time.Time; var status string
  if e=p.QueryRow(ctx,`select next_fire_at,status from recordings where id=426 and account_id=47`).Scan(&fire,&status); e!=nil { panic(e) }
  emit(map[string]any{"event":"armed","recording":426,"status":status,"fire":fire,"t_minus_2":fire.Add(-2*time.Hour),"t_minus_30":fire.Add(-30*time.Minute),"at":time.Now().UTC()})
  for _, gate := range []struct{name string; at time.Time}{{"t_minus_2",fire.Add(-2*time.Hour)},{"t_minus_30",fire.Add(-30*time.Minute)}} {
    if d:=time.Until(gate.at); d>0 { time.Sleep(d) }
    cmd:=exec.CommandContext(ctx,"/tmp/stoaramactl-fineplus-watch","capture","probe","--id","17325","--capture-timeout-sec","20")
    out,err:=cmd.CombinedOutput(); emit(map[string]any{"event":gate.name,"at":time.Now().UTC(),"error":fmt.Sprint(err),"output":strings.TrimSpace(string(out))})
    if err!=nil || !strings.Contains(string(out),"capture_ok") { emit(map[string]any{"event":"source_gate_failed","gate":gate.name,"at":time.Now().UTC()}); return }
  }
  if d:=time.Until(fire); d>0 { time.Sleep(d) }
  var cap int; var ready int
  if e=p.QueryRow(ctx,`select coalesce(sum(capacity),0),count(*) from recorder_droplets where state='active' and last_seen_at>now()-interval '5 minutes'`).Scan(&cap,&ready); e!=nil { panic(e) }
  var source string; if e=p.QueryRow(ctx,`select stream_url from recordings where id=426 and account_id=47`).Scan(&source); e!=nil { panic(e) }
  probe:=exec.CommandContext(ctx,"/tmp/stoaramactl-fineplus-watch","capture","probe","--id","17325","--capture-timeout-sec","20")
  probeOut,probeErr:=probe.CombinedOutput(); sourceOK:=probeErr==nil && strings.Contains(string(probeOut),"capture_ok")
  emit(map[string]any{"event":"opening_gate","at":time.Now().UTC(),"healthy_cloud_capacity":cap,"healthy_cloud_workers":ready,"source_url":source,"source_ok":sourceOK,"source_probe_error":fmt.Sprint(probeErr),"source_probe_output":strings.TrimSpace(string(probeOut))})
  if cap<=0 || ready<=0 { emit(map[string]any{"event":"resume_blocked","reason":"capacity_gate_failed","at":time.Now().UTC()}); return }
  if !sourceOK { emit(map[string]any{"event":"resume_blocked","reason":"source_gate_failed","at":time.Now().UTC()}); return }
  req,e:=http.NewRequestWithContext(ctx,http.MethodPost,strings.TrimRight(os.Getenv("BACKEND_API_URL"),"/")+"/api/v1/account/recordings/426/resume",nil); if e!=nil { panic(e) }; req.Header.Set("Authorization","Bearer "+os.Getenv("MITSCL_API_TOKEN")); resp,e:=http.DefaultClient.Do(req); if e!=nil { panic(e) }; resp.Body.Close(); emit(map[string]any{"event":"resume_attempt","status_code":resp.StatusCode,"at":time.Now().UTC()})
}
