package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	p, err := pgxpool.New(ctx, os.Getenv("DATABASE_URL"))
	if err != nil { panic(err) }
	defer p.Close()
	rows, err := p.Query(ctx, `
SELECT l.pid, l.granted, l.mode, COALESCE(a.state,''), COALESCE(a.query,''),
       COALESCE(a.application_name,''), COALESCE(a.client_addr::text,''),
       COALESCE(a.wait_event_type||':'||a.wait_event,''),
       COALESCE(EXTRACT(EPOCH FROM now()-a.query_start)::int,0),
       COALESCE(EXTRACT(EPOCH FROM now()-a.backend_start)::int,0)
FROM pg_locks l LEFT JOIN pg_stat_activity a ON a.pid=l.pid
WHERE l.locktype='advisory'
ORDER BY l.granted DESC, l.pid`)
	if err != nil { panic(err) }
	defer rows.Close()
	for rows.Next() {
		var pid, age int
		var granted bool
		var mode, state, query, app, client, wait string
		var backendAge int
		if err := rows.Scan(&pid, &granted, &mode, &state, &query, &app, &client, &wait, &age, &backendAge); err != nil { panic(err) }
		fmt.Printf("pid=%d granted=%t mode=%s query_age=%ds backend_age=%ds state=%s wait=%q app=%q client=%q query=%q\n", pid, granted, mode, age, backendAge, state, wait, app, client, query)
	}
	if err := rows.Err(); err != nil { panic(err) }
}
