package main

import (
 "context"
 "encoding/json"
 "fmt"
 "os"
 "time"
 "github.com/jackc/pgx/v5"
 "github.com/jackc/pgx/v5/pgxpool"
)

func main() {
 ctx,cancel:=context.WithTimeout(context.Background(),25*time.Second); defer cancel()
 p,e:=pgxpool.New(ctx,os.Getenv("DATABASE_URL")); if e!=nil {panic(e)}; defer p.Close()
 tx,e:=p.BeginTx(ctx,pgx.TxOptions{AccessMode:pgx.ReadOnly}); if e!=nil {panic(e)}; defer tx.Rollback(ctx)
 rows,e:=tx.Query(ctx,os.Getenv("AUDIT_SQL")); if e!=nil {panic(e)}; defer rows.Close()
 for rows.Next(){v,e:=rows.Values();if e!=nil{panic(e)}; b,e:=json.Marshal(v);if e!=nil{panic(e)};fmt.Println(string(b))};if rows.Err()!=nil{panic(rows.Err())}
}
