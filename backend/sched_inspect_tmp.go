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
	rows, err := db.Query(ctx, `SELECT id,status,cron_timezone,start_at,end_at,next_fire_at,daily_window_start,daily_window_end FROM recordings WHERE id=ANY($1::bigint[]) ORDER BY id`, []int64{337,339,355,385,401,405,406,407,409,417,418,419,420,427,428,431})
	if err != nil { panic(err) }
	defer rows.Close()
	for rows.Next() {
		var id int64; var status, tz, start, end, next, ws, we any
		if err := rows.Scan(&id,&status,&tz,&start,&end,&next,&ws,&we); err != nil { panic(err) }
		fmt.Printf("%d\t%v\t%v\t%v\t%v\t%v\t%v\t%v\n", id,status,tz,start,end,next,ws,we)
	}
	if err := rows.Err(); err != nil { panic(err) }
}
