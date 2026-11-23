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

// keFixture extends the shared fixture with a KE raw clip whose bytes are in a
// stitched output, plus the coverage TSV naming it.
func keFixture(t *testing.T) (ruleFiles, *strings.Builder) {
	t.Helper()
	files, _ := fixture(t)
	extra := &strings.Builder{}
	out := w0 - 30*day
	row := func(k kind, key string, stream, size int64, folder string) {
		fmt.Fprintf(extra, "%s\t%s\t%d\t%d\t%d\t%d\tf\t0\t%s\t1\n", k, key, stream, out, out+60, size, folder)
	}
	row(kindRaw, "managed/acct-47/ke_folder/cov.mp4", 20, 120, "ke_folder")     // covered KE raw
	row(kindRaw, "managed/acct-47/recordings/1/ke2.mp4", 20, 124, "recordings") // covered raw, output absent
	row(kindRaw, "managed/acct-47/ke_folder/recent.mp4", 20, 125, "ke_folder")  // covered KE raw, recent write
	row(kindRaw, "managed/acct-47/ke_folder/page.mp4", 20, 126, "ke_folder")    // covered KE raw, page_link
	row(kindCollated, "joined/nightly-collation-v2/objects/kecol.mp4", 20, 122, "ke_folder")
	f, _ := os.OpenFile(files.identities, os.O_APPEND|os.O_WRONLY, 0)
	f.WriteString(extra.String())
	f.Close()
	p, _ := os.OpenFile(files.dbProtect, os.O_APPEND|os.O_WRONLY, 0)
	p.WriteString("page_link\tmanaged/acct-47/ke_folder/page.mp4\n")
	p.Close()
	dir := t.TempDir()
	files.keCoverage = writeFile(t, dir, "coverage.tsv", strings.Join([]string{
		"managed/acct-47/ke_folder/cov.mp4\t120\tjoined/nightly-collation-v2/objects/out.mp4\t500",
		"managed/acct-47/recordings/1/ke2.mp4\t124\tjoined/nightly-collation-v2/objects/absent.mp4\t600",
		"managed/acct-47/ke_folder/recent.mp4\t125\tjoined/nightly-collation-v2/objects/out.mp4\t500",
		"managed/acct-47/ke_folder/page.mp4\t126\tjoined/nightly-collation-v2/objects/out.mp4\t500",
		"joined/nightly-collation-v2/objects/kecol.mp4\t122\tjoined/nightly-collation-v2/objects/out.mp4\t500",
	}, "\n")+"\n")
	return files, extra
}

func TestKECoveragePlanAndDecide(t *testing.T) {
	files, _ := keFixture(t)
	dir := t.TempDir()
	snapshot := writeFile(t, dir, "snapshot.tsv", strings.Join([]string{
		"managed/acct-47/ke_folder/cov.mp4\t120\te\t" + strconv.FormatInt(w0, 10),
		"managed/acct-47/recordings/1/ke2.mp4\t124\te\t" + strconv.FormatInt(w0, 10),
		"managed/acct-47/ke_folder/x.mp4\t106\te\t" + strconv.FormatInt(w0, 10),
		"joined/nightly-collation-v2/objects/out.mp4\t500\te\t" + strconv.FormatInt(w0, 10),
	}, "\n")+"\n")
	out := filepath.Join(dir, "plan")
	args := []string{"--snapshot", snapshot, "--out", out, "--identities", files.identities, "--db-protect", files.dbProtect,
		"--closure", files.closure, "--controller-protect", files.controller, "--picks", files.picks,
		"--recent-from", files.recentFrom, "--ke-coverage", files.keCoverage}
	if err := runPlan(args); err != nil {
		t.Fatal(err)
	}
	var sum summary
	raw, _ := os.ReadFile(filepath.Join(out, "summary.json"))
	if err := json.Unmarshal(raw, &sum); err != nil {
		t.Fatal(err)
	}
	if sum.Inputs["ke_coverage"] == "" {
		t.Fatal("plan must pin the coverage file digest")
	}
	if got := sum.ByClass[deleteKeRawStitched]; got == nil || got.Objects != 1 {
		t.Fatalf("covered KE raw must be one delete, got %v", got)
	}
	if got := sum.ByClass[protectKeepEverything]; got == nil || got.Objects != 2 {
		t.Fatalf("x.mp4 and the absent-output key stay protected, got %v", got)
	}
	var rows []manifestRow
	lf, _ := os.Open(filepath.Join(out, "delete-manifest.jsonl"))
	sc := bufio.NewScanner(lf)
	for sc.Scan() {
		var row manifestRow
		if err := json.Unmarshal(sc.Bytes(), &row); err != nil {
			t.Fatal(err)
		}
		rows = append(rows, row)
	}
	lf.Close()
	if len(rows) != 1 || rows[0].Key != "managed/acct-47/ke_folder/cov.mp4" || rows[0].Class != deleteKeRawStitched ||
		rows[0].Reason != "covered_by:joined/nightly-collation-v2" {
		t.Fatalf("manifest rows %+v", rows)
	}

	r, err := files.load()
	if err != nil {
		t.Fatal(err)
	}
	r.Outputs = map[string]int64{"joined/nightly-collation-v2/objects/out.mp4": 500}
	for key, want := range map[string]class{
		"managed/acct-47/ke_folder/cov.mp4":             deleteKeRawStitched,
		"managed/acct-47/recordings/1/ke2.mp4":          protectKeepEverything, // output absent
		"joined/nightly-collation-v2/objects/kecol.mp4": protectKeepEverything, // collated KE
		"managed/acct-47/ke_folder/page.mp4":            protectLiveSite,       // page_link
		"managed/acct-47/recordings/1/ke.mp4":           protectKeepEverything, // no coverage row
	} {
		size := map[string]int64{"managed/acct-47/ke_folder/cov.mp4": 120, "managed/acct-47/recordings/1/ke2.mp4": 124,
			"joined/nightly-collation-v2/objects/kecol.mp4": 122, "managed/acct-47/ke_folder/page.mp4": 126,
			"managed/acct-47/recordings/1/ke.mp4": 108}[key]
		if got, reason := r.decide(object{Key: key, Size: size, MTime: w0}); got != want {
			t.Errorf("%s: got %s (%s), want %s", key, got, reason, want)
		}
	}
	// Output present at a different size, then the raw size differs.
	r.Outputs["joined/nightly-collation-v2/objects/out.mp4"] = 499
	if got, _ := r.decide(object{Key: "managed/acct-47/ke_folder/cov.mp4", Size: 120, MTime: w0}); got != protectKeepEverything {
		t.Errorf("output size differs: got %s", got)
	}
	r.Outputs["joined/nightly-collation-v2/objects/out.mp4"] = 500
	if got, _ := r.decide(object{Key: "managed/acct-47/ke_folder/cov.mp4", Size: 121, MTime: w0}); got != protectKeepEverything {
		t.Errorf("raw size differs: got %s", got)
	}
	if got, _ := r.decide(object{Key: "managed/acct-47/ke_folder/recent.mp4", Size: 125, MTime: time.Now().Unix()}); got != protectKeepEverything {
		t.Errorf("recent write: got %s", got)
	}
}

func TestKECoverageOffLeavesKeepEverythingAlone(t *testing.T) {
	files, _ := keFixture(t)
	files.keCoverage = ""
	r, err := files.load()
	if err != nil {
		t.Fatal(err)
	}
	r.Outputs = map[string]int64{"joined/nightly-collation-v2/objects/out.mp4": 500}
	if got, reason := r.decide(object{Key: "managed/acct-47/ke_folder/cov.mp4", Size: 120, MTime: w0}); got != protectKeepEverything || reason != "keep_everything_folder_in_key" {
		t.Fatalf("without --ke-coverage: got %s (%s)", got, reason)
	}
}

func TestKECoverageRefusesBadRows(t *testing.T) {
	files, _ := keFixture(t)
	dir := t.TempDir()
	for name, body := range map[string]string{
		"malformed":     "a\t1\tb\n",
		"zero raw size": "a\t0\tb\t2\n",
		"zero output":   "a\t1\tb\t0\n",
		"empty raw":     "\t1\tb\t2\n",
		"empty output":  "a\t1\t\t2\n",
		"duplicate":     "a\t1\tb\t2\na\t1\tb\t2\n",
		"output as raw": "a\t1\tb\t2\nb\t1\tc\t2\n",
		"manifest out":  "a\t1\tjoined/nightly-collation-v2/manifests/m.json\t2\n",
	} {
		bad := files
		bad.keCoverage = writeFile(t, dir, strings.ReplaceAll(name, " ", "_")+".tsv", body)
		if _, err := bad.load(); err == nil {
			t.Errorf("%s: must refuse", name)
		}
	}
}

func TestDeleteSkipsKECoveredKeyWhenOutputLeavesFreshListing(t *testing.T) {
	files, _ := keFixture(t)
	dir := t.TempDir()
	manifest := filepath.Join(dir, "manifest.jsonl")
	row := manifestRow{Key: "managed/acct-47/ke_folder/cov.mp4", Size: 120, ETag: "e", MTime: w0, Class: deleteKeRawStitched,
		Reason: "covered_by:joined/nightly-collation-v2"}
	f, err := os.Create(manifest)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.NewEncoder(f).Encode(row); err != nil {
		t.Fatal(err)
	}
	f.Close()
	r, err := files.load()
	if err != nil {
		t.Fatal(err)
	}
	// The fresh listing still has the raw key but no longer its output.
	r.Fresh = map[string]object{row.Key: {Key: row.Key, Size: row.Size, ETag: row.ETag, MTime: row.MTime}}
	if r.Outputs, err = scanOutputSizes(writeFile(t, dir, "fresh.tsv",
		fmt.Sprintf("%s\t120\te\t%d\n", row.Key, w0)), r.OutputKeys); err != nil {
		t.Fatal(err)
	}
	ledger := filepath.Join(dir, "ledger.jsonl")
	if err := deleteManifest(context.Background(), store{bucket: "stoarama"}, r, runHeader{}, manifest, ledger, 2, 0); err != nil {
		t.Fatal(err)
	}
	lf, _ := os.Open(ledger)
	sc := bufio.NewScanner(lf)
	sc.Scan() // run header
	outcomes := map[outcome]int{}
	for sc.Scan() {
		var got ledgerRow
		json.Unmarshal(sc.Bytes(), &got)
		outcomes[got.Outcome]++
	}
	lf.Close()
	if outcomes[outcomeSkippedProtected] != 1 || outcomes[outcomeDeleted] != 0 {
		t.Fatalf("output gone from fresh listing must skip: %v", outcomes)
	}
}
