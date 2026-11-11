package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/daydemir/stoarama/backend/internal/capture"
	"github.com/jackc/pgx/v5/pgxpool"
)

func hash(v string) string { s:=sha256.Sum256([]byte(v)); return hex.EncodeToString(s[:]) }

func main(){
	ctx:=context.Background(); db,err:=pgxpool.New(ctx,os.Getenv("DATABASE_URL"));if err!=nil{panic(err)};defer db.Close()
	ids:=[]int64{404,403,408,348,349,350,381,382,384,402,410,414,433,436,442}
	for _,id:=range ids{
		var provider,cat,page string
		if err:=db.QueryRow(ctx,`SELECT s.provider,s.source_url,COALESCE(s.source_page_url,'') FROM recordings r JOIN streams s ON s.id=r.stream_id WHERE r.id=$1 AND r.account_id=47`,id).Scan(&provider,&cat,&page);err!=nil{fmt.Printf("PROBE %d catalog_error=%v\n",id,err);continue}
		pc,cancel:=context.WithTimeout(ctx,35*time.Second)
		resolved,image,headers,e:=capture.ResolveCaptureInputWithHeaders(pc,provider,cat,page)
		out:=fmt.Sprintf("PROBE %d resolved=%t image=%t",id,e==nil,image)
		if e!=nil{out+=fmt.Sprintf(" resolve_error=%q",e.Error());cancel();fmt.Println(out);continue}
		seg,ce:=capture.CaptureSegmentInDirWithHeadersNoThumbnail(pc,resolved,15*time.Second,"","",headers)
		if seg.Path!=""{_ = os.RemoveAll(filepath.Dir(seg.Path))}
		out+=fmt.Sprintf(" decode_ok=%t duration_ms=%d bytes=%d codec=%s",ce==nil,seg.DurationMs,seg.SizeBytes,seg.VideoCodec)
		if ce!=nil{out+=fmt.Sprintf(" decode_error=%q",ce.Error())}
		out+=fmt.Sprintf(" source_hash=%s",hash(cat));cancel();fmt.Println(out)
	}
}
