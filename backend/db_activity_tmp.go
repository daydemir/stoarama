package main

import("context";"fmt";"os";"time";"github.com/jackc/pgx/v5/pgxpool")
func main(){ctx,c:=context.WithTimeout(context.Background(),5*time.Second);defer c();p,e:=pgxpool.New(ctx,os.Getenv("DATABASE_URL"));if e!=nil{panic(e)};defer p.Close();rows,e:=p.Query(ctx,`SELECT pid,state,coalesce(wait_event_type||':'||wait_event,''),extract(epoch from now()-query_start)::int,LEFT(regexp_replace(query,'\s+',' ','g'),180) FROM pg_stat_activity WHERE query ILIKE '%recording_joined%' AND state<>'idle' ORDER BY query_start`);if e!=nil{panic(e)};defer rows.Close();for rows.Next(){var pid,age int;var state,wait,q string;if e:=rows.Scan(&pid,&state,&wait,&age,&q);e!=nil{panic(e)};fmt.Printf("pid=%d state=%s wait=%s age=%ds query=%q\n",pid,state,wait,age,q)}}
