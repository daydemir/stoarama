package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// fakeS3 implements the two calls r2prune sends: ListObjectsV2 (with prefix,
// delimiter and a deliberately tiny page size) and DeleteObjects.
type fakeS3 struct {
	mu      sync.Mutex
	objects map[string]int64
	deletes int
	page    int
}

func (f *fakeS3) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	q := r.URL.Query()
	if r.Method == http.MethodPost && q.Has("delete") {
		var req struct {
			Objects []struct{ Key string } `xml:"Object"`
		}
		body, _ := io.ReadAll(r.Body)
		if err := xml.Unmarshal(body, &req); err != nil || len(req.Objects) > deleteBatch {
			http.Error(w, "bad delete", http.StatusBadRequest)
			return
		}
		f.deletes++
		var out bytes.Buffer
		out.WriteString(`<DeleteResult>`)
		for _, o := range req.Objects {
			delete(f.objects, o.Key)
			fmt.Fprintf(&out, "<Deleted><Key>%s</Key></Deleted>", o.Key)
		}
		out.WriteString(`</DeleteResult>`)
		w.Write(out.Bytes())
		return
	}
	prefix, delim := q.Get("prefix"), q.Get("delimiter")
	keys := make([]string, 0, len(f.objects))
	for k := range f.objects {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	start, _ := strconv.Atoi(q.Get("continuation-token"))
	var entries []string // objects as "o:key", prefixes as "p:prefix", in order
	seen := map[string]bool{}
	for _, k := range keys {
		if !strings.HasPrefix(k, prefix) {
			continue
		}
		if delim != "" {
			if i := strings.Index(k[len(prefix):], delim); i >= 0 {
				p := k[:len(prefix)+i+1]
				if !seen[p] {
					seen[p] = true
					entries = append(entries, "p:"+p)
				}
				continue
			}
		}
		entries = append(entries, "o:"+k)
	}
	end := min(start+f.page, len(entries))
	var out bytes.Buffer
	fmt.Fprintf(&out, `<ListBucketResult><IsTruncated>%t</IsTruncated>`, end < len(entries))
	if end < len(entries) {
		fmt.Fprintf(&out, `<NextContinuationToken>%d</NextContinuationToken>`, end)
	}
	for _, e := range entries[start:end] {
		if strings.HasPrefix(e, "p:") {
			fmt.Fprintf(&out, `<CommonPrefixes><Prefix>%s</Prefix></CommonPrefixes>`, e[2:])
		} else {
			fmt.Fprintf(&out, `<Contents><Key>%s</Key><Size>%d</Size><ETag>"e"</ETag><LastModified>2026-11-01T00:00:00Z</LastModified></Contents>`, e[2:], f.objects[e[2:]])
		}
	}
	out.WriteString(`</ListBucketResult>`)
	w.Write(out.Bytes())
}

const day = int64(86400)

// Stream 10 keeps [w0, w0+20d); stream 20 is Keep Everything.
var w0 = time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC).Unix()

func writeFile(t *testing.T, dir, name, body string) string {
	t.Helper()
	p := filepath.Join(dir, name)
	if err := os.WriteFile(p, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}

func fixture(t *testing.T) (ruleFiles, map[string]int64) {
	dir := t.TempDir()
	objects := map[string]int64{
		"managed/acct-47/a_folder/old.mp4":                 100, // RAW outside window: delete
		"managed/acct-47/a_folder/in.mp4":                  101, // RAW inside window, not on NAS
		"managed/acct-47/a_folder/innas.mp4":               102, // RAW inside window, on NAS (controller window still protects)
		"managed/acct-47/a_folder/edge.mp4":                103, // RAW outside, but input to a window output
		"managed/acct-47/a_folder/hold.mp4":                104, // RAW outside, live stitch input
		"managed/acct-47/a_folder/late.mp4":                105, // RAW outside, becomes protected before delete
		"managed/acct-47/ke_folder/x.mp4":                  106, // Keep Everything stream
		"managed/acct-47/ke_folder/alias.mp4":              107, // alias under a KE folder
		"managed/acct-47/recordings/1/ke.mp4":              108, // KE stream, generic folder
		"managed/acct-47/recordings/2/old.mp4":             109, // generic folder is not KE: delete
		"joined/nightly-collation-v2/objects/old.mp4":      200, // collated outside: delete
		"joined/nightly-collation-v2/objects/page.mp4":     201, // page link
		"joined/best14-20261021/z.mp4":                     300, // controller unresolved prefix
		"datasets/global-street-scores-delivery/47/r.json": 5,
		"app/state.json":                                   6, // no identity: unclassified
	}
	for i := 0; i < 2500; i++ { // more than two delete batches
		objects[fmt.Sprintf("managed/acct-47/a_folder/bulk/%04d.mp4", i)] = 10
	}
	ids := &strings.Builder{}
	row := func(k kind, key string, stream, start, end, size int64, nas bool, folder string) {
		fmt.Fprintf(ids, "%s\t%s\t%d\t%d\t%d\t%d\t%s\t0\t%s\t1\n", k, key, stream, start, end, size, map[bool]string{true: "t", false: "f"}[nas], folder)
	}
	out := w0 - 30*day
	row(kindRaw, "managed/acct-47/a_folder/old.mp4", 10, out, out+60, 100, false, "a_folder")
	row(kindRaw, "managed/acct-47/a_folder/in.mp4", 10, w0+day, w0+day+60, 101, false, "a_folder")
	row(kindRaw, "managed/acct-47/a_folder/innas.mp4", 10, w0+day, w0+day+60, 102, true, "a_folder")
	row(kindRaw, "managed/acct-47/a_folder/edge.mp4", 10, w0-120, w0-60, 103, false, "a_folder")
	row(kindRaw, "managed/acct-47/a_folder/hold.mp4", 10, out, out+60, 104, false, "a_folder")
	row(kindRaw, "managed/acct-47/a_folder/late.mp4", 10, out, out+60, 105, false, "a_folder")
	row(kindRaw, "managed/acct-47/ke_folder/x.mp4", 20, out, out+60, 106, false, "ke_folder")
	row(kindRaw, "managed/acct-47/ke_folder/alias.mp4", 10, out, out+60, 107, false, "a_folder")
	row(kindRaw, "managed/acct-47/recordings/1/ke.mp4", 20, out, out+60, 108, false, "recordings")
	row(kindRaw, "managed/acct-47/recordings/2/old.mp4", 10, out, out+60, 109, false, "recordings")
	row(kindCollated, "joined/nightly-collation-v2/objects/old.mp4", 10, out, out+3600, 200, false, "a_folder")
	row(kindCollated, "joined/nightly-collation-v2/objects/page.mp4", 10, out, out+3600, 201, false, "a_folder")
	for i := 0; i < 2500; i++ {
		row(kindRaw, fmt.Sprintf("managed/acct-47/a_folder/bulk/%04d.mp4", i), 10, out, out+60, 10, false, "a_folder")
	}
	ws := time.Unix(w0, 0).UTC().Format(time.RFC3339)
	we := time.Unix(w0+20*day, 0).UTC().Format(time.RFC3339)
	controller := strings.Join([]string{
		`{"type":"meta","bucket":"stoarama","unmapped":"protect","keep_everything_stream_ids":[20]}`,
		`{"type":"stream_window","stream_id":10,"start_utc":"` + ws + `","end_utc_exclusive":"` + we + `"}`,
		`{"type":"stream_window","stream_id":20,"start_utc":"` + ws + `","end_utc_exclusive":"` + we + `"}`,
		`{"type":"key","bucket":"stoarama","key":"managed/acct-47/a_folder/hold.mp4","reasons":["live_stitch_input"]}`,
		`{"type":"prefix","bucket":"stoarama","prefix":"joined/best14-20261021/","reasons":["unresolved_best14_delivery_identity"]}`,
		`{"type":"prefix","bucket":"stoarama","prefix":"datasets/global-street-scores-delivery/47/","reasons":["live_delivery_registry_assets_and_tickets"]}`,
	}, "\n") + "\n"
	return ruleFiles{
		identities: writeFile(t, dir, "ids.tsv", ids.String()),
		dbProtect:  writeFile(t, dir, "db.tsv", "page_link\tjoined/nightly-collation-v2/objects/page.mp4\nnas_cleanup_hold\tmanaged/acct-47/a_folder/old.mp4\n"),
		closure:    writeFile(t, dir, "closure.tsv", "window_output_input\tmanaged/acct-47/a_folder/edge.mp4\n"),
		controller: writeFile(t, dir, "controller.jsonl", controller),
		picks:      writeFile(t, dir, "picks.json", `{"choices":{"10":{"choice":"none"},"20":{"choice":"all"}}}`),
		recentFrom: "2026-11-20T00:00:00Z",
	}, objects
}

func TestDecide(t *testing.T) {
	files, objects := fixture(t)
	r, err := files.load()
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]class{
		"managed/acct-47/a_folder/old.mp4":                 deleteRawOutsideWindow,
		"managed/acct-47/a_folder/in.mp4":                  protectWindowNotOnNAS,
		"managed/acct-47/a_folder/innas.mp4":               protectWindowOnNAS,
		"managed/acct-47/a_folder/edge.mp4":                protectStitchClosure,
		"managed/acct-47/a_folder/hold.mp4":                protectNASPipeline,
		"managed/acct-47/ke_folder/x.mp4":                  protectKeepEverything,
		"managed/acct-47/ke_folder/alias.mp4":              protectKeepEverything,
		"managed/acct-47/recordings/1/ke.mp4":              protectKeepEverything,
		"managed/acct-47/recordings/2/old.mp4":             deleteRawOutsideWindow,
		"joined/nightly-collation-v2/objects/old.mp4":      deleteCollatedOutsideWindow,
		"joined/nightly-collation-v2/objects/page.mp4":     protectLiveSite,
		"joined/best14-20261021/z.mp4":                     protectUnclassified,
		"datasets/global-street-scores-delivery/47/r.json": protectLiveSite,
		"app/state.json":                                   protectUnclassified,
	}
	for key, c := range want {
		got, reason := r.decide(object{Key: key, Size: objects[key], MTime: w0})
		if got != c {
			t.Errorf("%s: got %s (%s), want %s", key, got, reason, c)
		}
	}
	if c, _ := r.decide(object{Key: "managed/acct-47/a_folder/old.mp4", Size: 100, MTime: time.Now().Unix()}); c != protectRecentWrite {
		t.Errorf("recent write: got %s", c)
	}
	if c, _ := r.decide(object{Key: "managed/acct-47/a_folder/old.mp4", Size: 99, MTime: w0}); c != protectUnclassified {
		t.Errorf("size drift: got %s", c)
	}
}

func TestListPlanDeleteAgainstFakeS3(t *testing.T) {
	files, objects := fixture(t)
	fake := &fakeS3{objects: map[string]int64{}, page: 7}
	for k, v := range objects {
		fake.objects[k] = v
	}
	srv := httptest.NewServer(fake)
	defer srv.Close()
	t.Setenv("R2_ENDPOINT", srv.URL)
	t.Setenv("R2_ACCESS_KEY_ID", "k")
	t.Setenv("R2_SECRET_ACCESS_KEY", "s")
	t.Setenv("R2_BUCKET", "stoarama")
	st, err := storeFromEnv()
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	snap, err := os.Create(filepath.Join(dir, "snapshot.tsv"))
	if err != nil {
		t.Fatal(err)
	}
	n, _, err := listBucket(context.Background(), st, 8, 3, snap)
	snap.Close()
	if err != nil || n != int64(len(objects)) {
		t.Fatalf("listed %d of %d: %v", n, len(objects), err)
	}

	out := filepath.Join(dir, "plan")
	args := []string{"--snapshot", snap.Name(), "--out", out, "--identities", files.identities, "--db-protect", files.dbProtect,
		"--closure", files.closure, "--controller-protect", files.controller, "--picks", files.picks, "--recent-from", files.recentFrom}
	if err := runPlan(args); err != nil {
		t.Fatal(err)
	}
	var sum summary
	raw, _ := os.ReadFile(filepath.Join(out, "summary.json"))
	if err := json.Unmarshal(raw, &sum); err != nil {
		t.Fatal(err)
	}
	if sum.Delete.Objects != 2500+4 || sum.Listed.Objects != int64(len(objects)) {
		t.Fatalf("delete %d listed %d", sum.Delete.Objects, sum.Listed.Objects)
	}
	manifest := filepath.Join(out, "delete-manifest.jsonl")
	if got, _ := fileSHA256(manifest); got != sum.ManifestSHA256 {
		t.Fatal("summary does not pin the manifest digest")
	}

	summaryPath := filepath.Join(out, "summary.json")
	run, err := verifyReviewedInputs(manifest, summaryPath, sum.ManifestSHA256, files)
	if err != nil || run.Inputs["controller_protect"] == "" || run.RecentFrom != files.recentFrom {
		t.Fatalf("reviewed inputs must verify: %v %+v", err, run)
	}
	if _, err := verifyReviewedInputs(manifest, summaryPath, strings.Repeat("0", 64), files); err == nil {
		t.Fatal("wrong manifest digest must refuse")
	}
	other := files
	other.recentFrom = "2026-11-19T00:00:00Z"
	if _, err := verifyReviewedInputs(manifest, summaryPath, sum.ManifestSHA256, other); err == nil {
		t.Fatal("a recent-from other than the plan's must refuse")
	}

	// A key protected after the plan: the changed input refuses the run, and
	// the per-key re-check skips it even when called directly.
	f, _ := os.OpenFile(files.dbProtect, os.O_APPEND|os.O_WRONLY, 0)
	f.WriteString("nas_copy_pending\tmanaged/acct-47/a_folder/late.mp4\n")
	f.Close()
	if _, err := verifyReviewedInputs(manifest, summaryPath, sum.ManifestSHA256, files); err == nil {
		t.Fatal("a changed protect input must refuse")
	}
	ledger := filepath.Join(dir, "ledger.jsonl")
	r, err := files.load()
	if err != nil {
		t.Fatal(err)
	}
	// The fresh listing is the fake bucket as it is now, with one key rewritten.
	fresh := filepath.Join(dir, "fresh.tsv")
	ff, _ := os.Create(fresh)
	if _, _, err := listBucket(context.Background(), st, 4, 3, ff); err != nil {
		t.Fatal(err)
	}
	ff.Close()
	if r.Fresh, err = loadFresh(fresh, manifest); err != nil {
		t.Fatal(err)
	}
	rewritten := r.Fresh["managed/acct-47/a_folder/bulk/0007.mp4"]
	rewritten.ETag = "changed"
	r.Fresh["managed/acct-47/a_folder/bulk/0007.mp4"] = rewritten
	if err := deleteManifest(context.Background(), st, r, run, manifest, ledger, 4, 0); err != nil {
		t.Fatal(err)
	}
	outcomes := map[outcome]int{}
	lf, _ := os.Open(ledger)
	sc := bufio.NewScanner(lf)
	sc.Scan()
	var header runHeader
	if json.Unmarshal(sc.Bytes(), &header); header.Type != "run" || header.ManifestSHA256 != sum.ManifestSHA256 || header.RecentFrom != files.recentFrom {
		t.Fatalf("ledger header %+v", header)
	}
	for sc.Scan() {
		var row ledgerRow
		json.Unmarshal(sc.Bytes(), &row)
		outcomes[row.Outcome]++
	}
	lf.Close()
	if outcomes[outcomeDeleted] != 2502 || outcomes[outcomeSkippedProtected] != 1 || outcomes[outcomeSkippedChanged] != 1 || outcomes[outcomeFailed] != 0 {
		t.Fatalf("outcomes %v", outcomes)
	}
	if fake.deletes != 3 {
		t.Fatalf("want 3 DeleteObjects batches, got %d", fake.deletes)
	}
	for _, kept := range []string{"managed/acct-47/a_folder/late.mp4", "managed/acct-47/a_folder/in.mp4", "managed/acct-47/a_folder/innas.mp4",
		"managed/acct-47/ke_folder/x.mp4", "managed/acct-47/recordings/1/ke.mp4", "joined/nightly-collation-v2/objects/page.mp4", "joined/best14-20261021/z.mp4", "app/state.json"} {
		if _, ok := fake.objects[kept]; !ok {
			t.Errorf("protected key %s was deleted", kept)
		}
	}
	if _, ok := fake.objects["managed/acct-47/a_folder/bulk/0007.mp4"]; !ok {
		t.Error("a key changed since the plan was deleted")
	}
	if _, ok := fake.objects["managed/acct-47/a_folder/old.mp4"]; ok {
		t.Error("outside-window RAW key survived")
	}
}

func TestReleaseWindowOnLiveEvidence(t *testing.T) {
	files, _ := fixture(t)
	dir := t.TempDir()
	out := w0 - 30*day
	files.derived = writeFile(t, dir, "derived.tsv", strings.Join([]string{
		fmt.Sprintf("dup\tjoined/best14-20261021/dup.mp4\t10\t0\t0\t50\tf\t0\t\tpage_dup"),
		fmt.Sprintf("dup\tjoined/best14-20261021/kedup.mp4\t20\t0\t0\t51\tf\t0\t\tpage_dup"),
		fmt.Sprintf("col\tjoined/best14-20261021/old.mp4\t10\t%d\t%d\t52\tf\t0\t\tbest14", out, out+60),
	}, "\n")+"\n")
	ev := func(key string, match bool, verified, nasMTime int64) string {
		return fmt.Sprintf("raw\t%s\t%s\t%d\t%d\t0", key, map[bool]string{true: "t", false: "f"}[match], verified, nasMTime)
	}
	files.nasEvidence = writeFile(t, dir, "evidence.tsv", strings.Join([]string{
		ev("managed/acct-47/a_folder/innas.mp4", true, w0+2*day, w0+day), // fresh, stitched: delete
		ev("managed/acct-47/a_folder/in.mp4", true, w0+2*day, w0+3*day),  // NAS file changed after verify
		ev("managed/acct-47/a_folder/edge2.mp4", true, w0+2*day, 0),      // fresh but never stitched
	}, "\n")+"\n")
	if _, err := files.load(); err == nil {
		t.Fatal("evidence without --release nas_keep_20 must refuse")
	}
	files.release = "nas_keep_20,unresolved_best14_delivery_identity"
	f, _ := os.OpenFile(files.identities, os.O_APPEND|os.O_WRONLY, 0)
	fmt.Fprintf(f, "raw\tmanaged/acct-47/a_folder/edge2.mp4\t10\t%d\t%d\t110\tt\t0\ta_folder\t1\n", w0+day, w0+day+60)
	f.Close()
	c, _ := os.OpenFile(files.closure, os.O_APPEND|os.O_WRONLY, 0)
	c.WriteString("window_output_input\tmanaged/acct-47/a_folder/innas.mp4\n")
	c.Close()
	r, err := files.load()
	if err != nil {
		t.Fatal(err)
	}
	for key, want := range map[string]class{
		"managed/acct-47/a_folder/innas.mp4": deleteWindowOnNAS,
		"managed/acct-47/a_folder/in.mp4":    protectWindowStale,
		"managed/acct-47/a_folder/edge2.mp4": protectWindowUnjoined,
		"joined/best14-20261021/dup.mp4":     deleteDuplicateOfPageLink,
		"joined/best14-20261021/kedup.mp4":   protectKeepEverything,
		"joined/best14-20261021/old.mp4":     deleteCollatedOutsideWindow,
		"joined/best14-20261021/z.mp4":       protectUnclassified, // released prefix, still no identity
	} {
		size := map[string]int64{"managed/acct-47/a_folder/innas.mp4": 102, "managed/acct-47/a_folder/in.mp4": 101,
			"managed/acct-47/a_folder/edge2.mp4": 110, "joined/best14-20261021/dup.mp4": 50, "joined/best14-20261021/kedup.mp4": 51,
			"joined/best14-20261021/old.mp4": 52, "joined/best14-20261021/z.mp4": 300}[key]
		if got, reason := r.decide(object{Key: key, Size: size, MTime: w0}); got != want {
			t.Errorf("%s: got %s (%s), want %s", key, got, reason, want)
		}
	}
	if got, _ := r.decide(object{Key: "managed/acct-47/a_folder/innas.mp4", Size: 102, MTime: w0 + 3*day}); got != protectWindowStale {
		t.Errorf("R2 object changed after verify: got %s", got)
	}
}
