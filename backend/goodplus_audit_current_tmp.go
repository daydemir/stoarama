package main

import (
 "context"
 "fmt"
 "os"
 "time"
 "github.com/jackc/pgx/v5/pgxpool"
)

func g(v, gap *float64, o30,o5,ov,ver *int, clips int) string { if ver==nil||*ver!=2||v==nil||gap==nil||o30==nil||o5==nil||ov==nil{return "?"}; if *ov==0&&*v>=99&&*gap<=120{return "A"};if *ov==0&&*v>=95&&*gap<=900&&*o5<=1&&*o30<=6{return "B"};if *ov==0&&*v>=90&&*gap<=1800&&*o5<=2{return "C"};if clips==0||*v==0{return "F"};if *v<80{return "E"};return "D" }
func main(){ c:=context.Background();db,e:=pgxpool.New(c,os.Getenv("DATABASE_URL"));if e!=nil{panic(e)};defer db.Close();ids:=[]int64{381,402,405};rows,e:=db.Query(c,`select j.recording_id,j.fire_at,h.coverage_pct,h.largest_gap_seconds,h.gap_over_30s_count,h.gap_over_5m_count,h.overlap_count,h.metric_version,coalesce(h.clip_count,0) from recording_jobs j left join recording_window_health h on h.job_id=j.id where j.recording_id=any($1::bigint[]) and j.kind='continuous_window' order by j.recording_id,j.fire_at`,ids);if e!=nil{panic(e)};defer rows.Close();cur:=int64(-1);for rows.Next(){var id int64;var fire time.Time;var v,gap *float64;var o30,o5,ov,ver *int;var clips int;if e:=rows.Scan(&id,&fire,&v,&gap,&o30,&o5,&ov,&ver,&clips);e!=nil{panic(e)};if id!=cur{if cur>=0{fmt.Println()};cur=id;fmt.Printf("%d ",id)};fmt.Printf("%s:%s ",fire.UTC().Format("01/02"),g(v,gap,o30,o5,ov,ver,clips))};fmt.Println()}
