package api

import (
	"context"
	"crypto/md5"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"maps"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/daydemir/stoarama/backend/internal/r2"
	"github.com/jackc/pgx/v5/pgxpool"
)

// delayedConn adds one database round trip of latency per client write, the
// shape of a Render instance talking to its managed PostgreSQL.
type delayedConn struct {
	net.Conn
	rtt time.Duration
}

func (c delayedConn) Write(b []byte) (int, error) {
	time.Sleep(c.rtt)
	return c.Conn.Write(b)
}

// TestPublicGlobalDeliveryPageLoadBench measures what concurrent researchers
// see: loaders each fetch metadata, catalog and growth at once, through the
// API router, a seeded PostgreSQL pool (production's 20 connections) and a
// fake R2 over HTTP. Set STOARAMA_GSS_DELIVERY_BENCH=1; override with
// STOARAMA_GSS_DELIVERY_BENCH_{LOADERS,ROUNDS,DB_RTT_MS,HEAD_MS,GET_MS}.
func TestPublicGlobalDeliveryPageLoadBench(t *testing.T) {
	if os.Getenv("STOARAMA_GSS_DELIVERY_BENCH") == "" {
		t.Skip("set STOARAMA_GSS_DELIVERY_BENCH=1")
	}
	env := func(name string, fallback int) int {
		var v int
		if _, err := fmt.Sscan(os.Getenv("STOARAMA_GSS_DELIVERY_BENCH_"+name), &v); err == nil && v > 0 {
			return v
		}
		return fallback
	}
	loaders, rounds := env("LOADERS", 20), env("ROUNDS", 5)
	rtt := time.Duration(env("DB_RTT_MS", 5)) * time.Millisecond
	db := cleanupTestPool(t)
	ctx := context.Background()
	cfg := db.Config()
	cfg.MaxConns = 20
	cfg.ConnConfig.DialFunc = func(ctx context.Context, network, addr string) (net.Conn, error) {
		conn, err := (&net.Dialer{}).DialContext(ctx, network, addr)
		return delayedConn{conn, rtt}, err
	}
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()

	bucket := &fakeCleanupBucket{objects: map[string][]byte{}}
	r2srv := httptest.NewServer(bucket)
	defer r2srv.Close()
	client, err := r2.New(ctx, r2.Config{AccessKey: "key", SecretKey: "secret", Region: "auto", Bucket: "bucket", Endpoint: r2srv.URL})
	if err != nil {
		t.Fatal(err)
	}
	reg := globalDeliveryTestRegistry()
	reg.Generation, reg.PublicDelivery, reg.SelectionScope = strings.Repeat("d", 32), true, "available"
	assets := []string{"/public/metadata.json", "/public/catalog.json", "/public/growth.json"}
	reg.Assets = map[string]globalDeliveryObject{}
	for i, name := range assets {
		body := []byte(fmt.Sprintf(`{"asset":%q,"pad":%q}`, name, strings.Repeat("x", 64<<10+i)))
		sha, etag := sha256.Sum256(body), md5.Sum(body)
		o := globalDeliveryObject{Key: globalDeliveryPrefix + "assets" + name, ETag: hex.EncodeToString(etag[:]), SHA256: hex.EncodeToString(sha[:]), SizeBytes: int64(len(body)), ContentType: "application/json", Verified: true}
		reg.Assets[name] = o
		bucket.put(o.Key, body)
	}
	for _, o := range append(slices.Collect(maps.Values(reg.Assets)), reg.Files["1"]) {
		if _, err := db.Exec(ctx, `INSERT INTO delivery_keep_holds(bucket,object_key,etag,size_bytes,kind) VALUES('stoarama',$1,$2,$3,'live_file')`, o.Key, o.ETag, o.SizeBytes); err != nil {
			t.Fatal(err)
		}
	}
	raw, err := json.Marshal(reg)
	if err != nil {
		t.Fatal(err)
	}
	bucket.put(globalDeliveryPrefix+"public/current.json", raw)
	bucket.put(globalDeliveryPrefix+"public/generations/"+reg.Generation+".json", raw)
	if _, err := db.Exec(ctx, `UPDATE gss_public_generation_pin SET generation=$1`, reg.Generation); err != nil {
		t.Fatal(err)
	}
	api := httptest.NewServer((&Server{pool: pool, r2: client}).router())
	defer api.Close()
	fetch := func(name string) time.Duration {
		began := time.Now()
		resp, err := http.Get(api.URL + publicGlobalDeliveryAPI + "/metadata?path=" + url.QueryEscape(name))
		if err != nil {
			t.Error(err)
			return 0
		}
		_, _ = io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Errorf("%s: %d", name, resp.StatusCode)
		}
		return time.Since(began)
	}
	fetch(assets[0]) // cold load
	bucket.mu.Lock()
	bucket.headDelay = time.Duration(env("HEAD_MS", 30)) * time.Millisecond
	bucket.getDelay = time.Duration(env("GET_MS", 60)) * time.Millisecond
	bucket.mu.Unlock()

	var mu sync.Mutex
	latency := map[string][]time.Duration{}
	var wg sync.WaitGroup
	for range loaders {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range rounds {
				var page sync.WaitGroup
				for _, name := range assets {
					page.Add(1)
					go func() {
						defer page.Done()
						d := fetch(name)
						mu.Lock()
						latency[name] = append(latency[name], d)
						mu.Unlock()
					}()
				}
				page.Wait()
			}
		}()
	}
	wg.Wait()
	gets, heads := bucket.counts()
	t.Logf("loaders=%d rounds=%d db_rtt=%s r2_head=%s r2_get=%s r2_gets=%d r2_heads=%d", loaders, rounds, rtt, bucket.headDelay, bucket.getDelay, gets, heads)
	for _, name := range assets {
		d := latency[name]
		slices.Sort(d)
		pct := func(p int) time.Duration { return d[(len(d)-1)*p/100] }
		t.Logf("%-22s n=%d p50=%s p95=%s max=%s", name, len(d), pct(50).Round(time.Millisecond), pct(95).Round(time.Millisecond), d[len(d)-1].Round(time.Millisecond))
	}
}
