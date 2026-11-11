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
	var b []byte
	e = p.QueryRow(c, `WITH u AS (SELECT relative_path,split_part(relative_path,'/',1) top FROM nas_inventory_unmatched_files WHERE connection_id=13 AND state='present'), tagged AS (SELECT *,CASE WHEN top~'^[0-9]+$' THEN 'legacy_numeric' WHEN top='#recycle' THEN 'nas_system' WHEN top LIKE '%\_%' ESCAPE '\' THEN 'naming_profile' ELSE 'other' END category FROM u), cats AS (SELECT category,count(*) files,count(DISTINCT top) roots,count(*)FILTER(WHERE relative_path LIKE '%/July/%') july,count(*)FILTER(WHERE relative_path LIKE '%/August/%') august FROM tagged GROUP BY category ORDER BY files DESC), tops AS (SELECT top,count(*) files FROM tagged GROUP BY top ORDER BY files DESC LIMIT 20) SELECT jsonb_build_object('total',(SELECT count(*) FROM u),'roots',(SELECT count(DISTINCT top) FROM u),'categories',COALESCE((SELECT jsonb_agg(to_jsonb(cats)) FROM cats),'[]'::jsonb),'top20',COALESCE((SELECT jsonb_agg(to_jsonb(tops)) FROM tops),'[]'::jsonb))`).Scan(&b)
	if e != nil {
		panic(e)
	}
	var x any
	_ = json.Unmarshal(b, &x)
	o, _ := json.MarshalIndent(x, "", "  ")
	fmt.Println(string(o))
}
