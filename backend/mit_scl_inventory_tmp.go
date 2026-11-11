package main

import (
	"context"
	"fmt"
	"os"

	"github.com/jackc/pgx/v5/pgxpool"
)

func main() {
	ctx := context.Background()
	db, err := pgxpool.New(ctx, os.Getenv("DATABASE_URL"))
	if err != nil { panic(err) }
	defer db.Close()

	var total int
	if err := db.QueryRow(ctx, `SELECT count(*) FROM recordings WHERE account_id=47`).Scan(&total); err != nil { panic(err) }
	fmt.Printf("TOTAL\t%d\n", total)
	rows, err := db.Query(ctx, `
		SELECT id,status,mode,cron_timezone,start_at,end_at,next_fire_at
		FROM recordings WHERE account_id=47 ORDER BY id`)
	if err != nil { panic(err) }
	defer rows.Close()
	for rows.Next() {
		var id int64
		var status, mode, tz string
		var start, end, next any
		if err := rows.Scan(&id,&status,&mode,&tz,&start,&end,&next); err != nil { panic(err) }
		fmt.Printf("REC\t%d\t%s\t%s\t%s\t%v\t%v\t%v\n", id,status,mode,tz,start,end,next)
	}
	if err := rows.Err(); err != nil { panic(err) }
}
