package main

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/daydemir/stoarama/backend/internal/collation"
)

// The days scope plans exact listed days of a completed recording: a
// registered hour becomes a restore of its registered batch, clip set and
// outputs; a purged hour is reported, not planned; a registered hour without
// outputs has nothing to restore. Register leaves the registered row alone.
func TestCollationV2DaysScopeRestoresRegisteredHours(t *testing.T) {
	pool := joinedPurgeTestPool(t, "collation_v2_days")
	ctx := context.Background()
	if _, err := pool.Exec(ctx, `
CREATE TABLE recordings(id bigint PRIMARY KEY,status text,cron_timezone text,naming_profile text,folder_name text,naming_metadata_jsonb jsonb,
 daily_window_start time,daily_window_end time,clip_duration_sec numeric);
CREATE TABLE recording_joined_hours(id bigint PRIMARY KEY,recording_id bigint,hour_id text,batch_id text,local_date date,delivery_hour int);
CREATE TABLE recording_clips(id bigint PRIMARY KEY,recording_id bigint,recording_job_id bigint,capture_attempt_id uuid,capture_lease_token uuid,
 capture_sequence bigint,clip_start_at timestamptz,clip_end_at timestamptz,bucket text,object_key text,etag text,size_bytes bigint,sha256 text,purged_at timestamptz);
CREATE TABLE collation_v2_recovery_replacements(collation_hour_id bigint PRIMARY KEY,batch_id text);
CREATE TABLE recording_collation_hours(id bigint PRIMARY KEY,hour_id text UNIQUE,batch_id text,generation int,recording_id bigint,local_date date,
 delivery_hour int,status text,manifest jsonb);
CREATE TABLE recording_collation_outputs(id bigint PRIMARY KEY,collation_hour_id bigint,part int,object_key text,size_bytes bigint,sha256 text);
INSERT INTO recordings VALUES (5,'completed','UTC','stoarama_v1','f',NULL,'08:00','20:00',60),(6,'paused','UTC','stoarama_v1','f',NULL,'00:00','23:59',60);
INSERT INTO recording_clips(id,recording_id,recording_job_id,clip_start_at,clip_end_at,bucket,object_key,etag,size_bytes,sha256,purged_at) VALUES
 (101,5,1,'2026-10-01T08:00Z','2026-10-01T08:01Z','b','raw/101.mp4','e',10,repeat('1',64),NULL),
 (102,5,1,'2026-10-01T08:01Z','2026-10-01T08:02Z','b','raw/102.mp4','e',10,repeat('2',64),NULL),
 (103,5,1,'2026-10-01T08:30Z','2026-10-01T08:31Z','b','raw/103.mp4','e',10,repeat('3',64),NULL),
 (201,5,1,'2026-10-01T09:00Z','2026-10-01T09:01Z','b','raw/201.mp4','e',10,repeat('4',64),'2026-11-01T00:00Z'),
 (301,5,1,'2026-10-01T10:00Z','2026-10-01T10:01Z','b','raw/301.mp4','e',10,repeat('5',64),NULL);
`); err != nil {
		t.Fatal(err)
	}
	const batch = "best14-20261021"
	hour1, _ := collation.HourIDFor(batch, 5, "2026-10-01", 1)
	hour4, _ := collation.HourIDFor(batch, 5, "2026-10-01", 4)
	manifest := func(ids ...int64) []byte {
		var disp []collation.ClipDisposition
		for _, id := range ids {
			var c collation.Clip
			if err := pool.QueryRow(ctx, `SELECT id,bucket,object_key,size_bytes,sha256,clip_start_at FROM recording_clips WHERE id=$1`, id).
				Scan(&c.ClipID, &c.Bucket, &c.ObjectKey, &c.SizeBytes, &c.SHA256, &c.StartUTC); err != nil {
				t.Fatal(err)
			}
			c.KnownBroken = id == 102
			disp = append(disp, collation.ClipDisposition{Clip: c, Disposition: "included", Part: 1})
		}
		body, _ := json.Marshal(map[string]any{"media_tool": "ffmpeg version test", "clips": disp})
		return body
	}
	outKey := collation.ObjectKey(batch, strings.Repeat("a", 64))
	if _, err := pool.Exec(ctx, `INSERT INTO recording_collation_hours VALUES (1,$1,$2,2,5,'2026-10-01',1,'collated',$3),(2,$4,$2,2,5,'2026-10-01',4,'gap_only',$5)`,
		hour1, batch, manifest(101, 102), hour4, manifest()); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO recording_collation_outputs VALUES (1,1,1,$1,100,repeat('a',64))`, outKey); err != nil {
		t.Fatal(err)
	}
	dayList := func(lines ...string) []collationDayRequest {
		path := filepath.Join(t.TempDir(), "days.txt")
		if err := os.WriteFile(path, []byte(strings.Join(lines, "\n")+"\n"), 0o600); err != nil {
			t.Fatal(err)
		}
		days, err := readDayList(path)
		if err != nil {
			t.Fatal(err)
		}
		return days
	}
	now := time.Date(2026, 11, 25, 0, 0, 0, 0, time.UTC)
	plan, err := planCollation(ctx, pool, collationPlanOptions{scope: "days", dayList: dayList("5,2026-10-01"), now: now})
	if err != nil {
		t.Fatal(err)
	}
	hour2, _ := collation.HourIDFor(NightlyBatchID, 5, "2026-10-01", 2)
	if len(plan.work) != 10 || plan.registeredEmpty != 1 || len(plan.unavailable) != 1 || plan.unavailable[0] != hour2 {
		t.Fatalf("plan work=%d empty=%d unavailable=%v", len(plan.work), plan.registeredEmpty, plan.unavailable)
	}
	restore := plan.work[0]
	if restore.HourID != hour1 || restore.BatchID != batch || restore.Restore == nil || restore.Restore.MediaTool != "ffmpeg version test" ||
		len(restore.Restore.Outputs) != 1 || restore.Restore.Outputs[0].ObjectKey != outKey || len(restore.Clips) != 2 || !restore.Clips[1].KnownBroken {
		t.Fatalf("restore hour %+v", restore)
	}
	fresh := plan.work[1]
	if fresh.DeliveryHour != 3 || fresh.BatchID != NightlyBatchID || fresh.Restore != nil || len(fresh.Clips) != 1 || fresh.Clips[0].ClipID != 301 {
		t.Fatalf("unregistered hour %+v", fresh)
	}

	// --recovery-batch renames only the new hours: a registered hour stays a
	// restore in its own batch, and a new hour keeps its generation-1
	// supersession, looked up in the day's own batch.
	nightly3, _ := collation.HourIDFor(NightlyBatchID, 5, "2026-10-01", 3)
	gen1Hour := strings.Replace(nightly3, "__generation-2", "__generation-1", 1)
	if _, err := pool.Exec(ctx, `INSERT INTO recording_joined_hours VALUES (77,5,$1,$2,'2026-10-01',3)`, gen1Hour, NightlyBatchID); err != nil {
		t.Fatal(err)
	}
	const recoveryBatch = "recovery-best14-20261125"
	recovered, err := planCollation(ctx, pool, collationPlanOptions{scope: "days", dayList: dayList("5,2026-10-01"), now: now, recoveryBatch: recoveryBatch})
	if err != nil || len(recovered.work) != len(plan.work) {
		t.Fatalf("recovery plan %+v %v", recovered, err)
	}
	if r := recovered.work[0]; r.HourID != hour1 || r.BatchID != batch || r.Restore == nil {
		t.Fatalf("registered hour left its batch under --recovery-batch: %+v", r)
	}
	wantFresh, _ := collation.HourIDFor(recoveryBatch, 5, "2026-10-01", 3)
	if f := recovered.work[1]; f.HourID != wantFresh || f.BatchID != recoveryBatch || f.Restore != nil || f.SupersedesHourRecordID != 77 || f.SupersedesHourID != gen1Hour {
		t.Fatalf("new hour under --recovery-batch: %+v", f)
	}

	// An hour whose restore differs, once approved for this recovery batch,
	// is stitched fresh there at generation 3 and supersedes the old hour; an
	// approval for another batch changes nothing.
	if _, err := pool.Exec(ctx, `INSERT INTO collation_v2_recovery_replacements VALUES (1,$1)`, recoveryBatch); err != nil {
		t.Fatal(err)
	}
	other, err := planCollation(ctx, pool, collationPlanOptions{scope: "days", dayList: dayList("5,2026-10-01,1"), now: now, recoveryBatch: "recovery-other"})
	if err != nil || len(other.work) != 1 || other.work[0].Restore == nil || other.work[0].Replaces != nil {
		t.Fatalf("approval leaked to another batch: %+v %v", other, err)
	}
	replaced, err := planCollation(ctx, pool, collationPlanOptions{scope: "days", dayList: dayList("5,2026-10-01,1"), now: now, recoveryBatch: recoveryBatch})
	wantReplacement, _ := collation.HourIDForGeneration(recoveryBatch, 5, "2026-10-01", 1, collation.ReplacementGeneration)
	if err != nil || len(replaced.work) != 1 {
		t.Fatalf("replacement plan %+v %v", replaced, err)
	}
	if r := replaced.work[0]; r.HourID != wantReplacement || r.BatchID != recoveryBatch || r.Restore != nil || r.Replaces == nil ||
		r.Replaces.HourRecordID != 1 || r.Replaces.HourID != hour1 || r.SupersedesHourRecordID != 0 || len(r.Clips) != 2 || r.Generation() != collation.ReplacementGeneration {
		t.Fatalf("replacement hour %+v", r)
	}
	// Without the recovery batch it stays a restore.
	one, err := planCollation(ctx, pool, collationPlanOptions{scope: "days", dayList: dayList("5,2026-10-01,1"), now: now})
	if err != nil || len(one.work) != 1 || one.work[0].HourID != hour1 {
		t.Fatalf("single hour plan %+v %v", one, err)
	}
	for name, opts := range map[string]collationPlanOptions{
		"duplicate": {scope: "days", dayList: dayList("5,2026-10-01", "5,2026-10-01,3"), now: now},
		"window":    {scope: "days", dayList: dayList("6,2026-10-01"), now: now},
		"open day":  {scope: "days", dayList: dayList("5,2026-10-01"), now: time.Date(2026, 10, 1, 19, 0, 0, 0, time.UTC)},
	} {
		if _, err := planCollation(ctx, pool, opts); err == nil {
			t.Fatalf("%s planned", name)
		}
	}
	if _, err := pool.Exec(ctx, `UPDATE recording_clips SET size_bytes=11 WHERE id=102`); err != nil {
		t.Fatal(err)
	}
	if _, err := planCollation(ctx, pool, collationPlanOptions{scope: "days", dayList: dayList("5,2026-10-01,1"), now: now}); err == nil {
		t.Fatal("restore planned over a changed registered clip")
	}

	// Register never touches a registered hour, whatever the rebuild produced.
	summary, err := registerCollation(ctx, pool, collation.R2Store{}, []collation.HourWork{restore}, false)
	if err != nil || summary.Already != 1 || summary.Registered != 0 {
		t.Fatalf("register %+v %v", summary, err)
	}
	var hours, outputs int
	if err := pool.QueryRow(ctx, `SELECT (SELECT count(*) FROM recording_collation_hours),(SELECT count(*) FROM recording_collation_outputs)`).Scan(&hours, &outputs); err != nil || hours != 2 || outputs != 1 {
		t.Fatalf("rows hours=%d outputs=%d %v", hours, outputs, err)
	}
}
