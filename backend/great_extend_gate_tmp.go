package main

import (
	"context"
	"fmt"
	"os"

	"github.com/jackc/pgx/v5/pgxpool"
)

func main() {
	ctx := context.Background(); db, err := pgxpool.New(ctx, os.Getenv("DATABASE_URL")); if err != nil { panic(err) }; defer db.Close()
	rows, err := db.Query(ctx, `SELECT r.id,r.status,r.end_at,r.next_fire_at,
COALESCE((SELECT count(*) FROM recording_jobs j WHERE j.recording_id=r.id AND j.status='pending'),0),
	COALESCE((SELECT count(*) FROM recording_jobs j WHERE j.recording_id=r.id AND j.status='leased' AND j.lease_expires_at>now()),0),
	COALESCE((SELECT count(*) FROM recording_jobs j WHERE j.recording_id=r.id AND j.handoff_until>now()),0),
	COALESCE((SELECT count(*) FROM recording_upload_intents u WHERE u.recording_id=r.id AND u.status='pending' AND u.expires_at>now()),0),
	(SELECT min(j.lease_expires_at) FROM recording_jobs j WHERE j.recording_id=r.id AND j.status='leased' AND j.lease_expires_at>now())
		FROM recordings r WHERE r.id=ANY($1::bigint[]) ORDER BY r.id`, []int64{352,353,354,378,386,387,388,389,390,392,393,394,395,396,397,398,399,400,402})
	if err != nil { panic(err) }; defer rows.Close()
	for rows.Next() { var id int64; var status string; var end,next,leaseExp any; var p,l,h,i int; if err:=rows.Scan(&id,&status,&end,&next,&p,&l,&h,&i,&leaseExp);err!=nil{panic(err)}; fmt.Printf("%d\t%s\tend=%v\tnext=%v\tp=%d l=%d h=%d i=%d lease_exp=%v\n",id,status,end,next,p,l,h,i,leaseExp) }
	if err:=rows.Err();err!=nil{panic(err)}
}
