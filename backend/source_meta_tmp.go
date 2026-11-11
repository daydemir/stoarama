package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/jackc/pgx/v5/pgxpool"
	"os"
)

func main() {
	c := context.Background()
	p, e := pgxpool.New(c, os.Getenv("DATABASE_URL"))
	if e != nil {
		panic(e)
	}
	defer p.Close()
	for _, id := range []int{426, 438} {
		var b []byte
		e = p.QueryRow(c, `SELECT jsonb_build_object('recording',r.id,'stream',s.id,'name',s.name,'external_id',s.external_id,'source_url',s.source_url,'source_page_url',s.source_page_url,'metadata',s.metadata_jsonb,'peer_count',(SELECT count(*) FROM streams p WHERE p.provider=s.provider AND p.deleted_at IS NULL)) FROM recordings r JOIN streams s ON s.id=r.stream_id WHERE r.id=$1`, id).Scan(&b)
		if e != nil {
			panic(e)
		}
		var x any
		_ = json.Unmarshal(b, &x)
		o, _ := json.Marshal(x)
		fmt.Println(string(o))
	}
}
