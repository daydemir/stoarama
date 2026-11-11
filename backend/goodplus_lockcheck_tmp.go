package main
import("context";"fmt";"os";"github.com/jackc/pgx/v5")
func main(){c:=context.Background();p,e:=pgx.Connect(c,os.Getenv("DATABASE_URL"));if e!=nil{panic(e)};defer p.Close(c);const l int64=0x4750313557415443;var ok bool;if e=p.QueryRow(c,`select pg_try_advisory_lock($1)`,l).Scan(&ok);e!=nil{panic(e)};if ok{_,_=p.Exec(c,`select pg_advisory_unlock($1)`,l)};fmt.Printf("lock=%d held_by_other=%t\n",l,!ok)}
