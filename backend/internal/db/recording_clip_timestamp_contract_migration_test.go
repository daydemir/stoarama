package db

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

func TestRecordingClipTimestampContractMigration(t *testing.T) {
	databaseURL := strings.TrimSpace(os.Getenv("STOARAMA_TEST_DATABASE_URL"))
	if databaseURL == "" {
		t.Skip("set STOARAMA_TEST_DATABASE_URL")
	}
	ctx := context.Background()
	c, err := pgx.Connect(ctx, databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close(ctx)
	schema := fmt.Sprintf("clip_timestamp_contract_%d", time.Now().UnixNano())
	if _, err = c.Exec(ctx, "CREATE SCHEMA "+schema); err != nil {
		t.Fatal(err)
	}
	defer c.Exec(ctx, "DROP SCHEMA "+schema+" CASCADE")
	if _, err = c.Exec(ctx, "SET search_path="+schema); err != nil {
		t.Fatal(err)
	}
	if _, err = c.Exec(ctx, `CREATE TABLE recording_clips(id bigint primary key,capture_lease_token uuid,capture_sequence bigint,released_at timestamptz,purged_at timestamptz)`); err != nil {
		t.Fatal(err)
	}
	if _, err = c.Exec(ctx, `CREATE TABLE recording_jobs(id bigint primary key,recording_id bigint,lease_token uuid,status text,lease_owner text,lease_expires_at timestamptz)`); err != nil {
		t.Fatal(err)
	}
	if _, err = c.Exec(ctx, `CREATE TABLE recordings(id bigint primary key,account_id bigint)`); err != nil {
		t.Fatal(err)
	}
	if _, err = c.Exec(ctx, `CREATE TABLE nodes(id bigint primary key,account_id bigint,node_type text)`); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile("../../../infra/sql/migrations/0132_recording_clip_timestamp_contract.sql")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = c.Exec(ctx, string(raw)); err != nil {
		t.Fatal(err)
	}
	lease, attempt := "123e4567-e89b-12d3-a456-426614174000", "123e4567-e89b-12d3-a456-426614174001"
	wrongLease := "123e4567-e89b-12d3-a456-426614174099"
	contract := `{"version":1,"mode":"muxed_source_copy","audio_selection":"first_optional","tracks":[{"stream_index":0,"media_type":"video","time_base_num":1,"time_base_den":1000,"first_timestamp":0,"last_timestamp":1000,"last_duration":40,"unit_count":26,"codec_signature_sha256":"` + strings.Repeat("a", 64) + `"}]}`
	var valid bool
	if err = c.QueryRow(ctx, `SELECT valid_recording_clip_timestamp_contract($1::jsonb)`, contract).Scan(&valid); err != nil || !valid {
		t.Fatalf("valid timestamp contract rejected: valid=%v err=%v", valid, err)
	}
	baseTrack := map[string]any{
		"stream_index": 0, "media_type": "video", "time_base_num": 1, "time_base_den": 1000,
		"first_timestamp": 0, "last_timestamp": 1000, "last_duration": 40, "unit_count": 26,
		"codec_signature_sha256": strings.Repeat("a", 64),
	}
	for _, key := range []string{"stream_index", "media_type", "time_base_num", "time_base_den", "first_timestamp", "last_timestamp", "last_duration", "unit_count", "codec_signature_sha256"} {
		t.Run("missing track "+key, func(t *testing.T) {
			track := make(map[string]any, len(baseTrack))
			for k, v := range baseTrack {
				track[k] = v
			}
			delete(track, key)
			payload, marshalErr := json.Marshal(map[string]any{"version": 1, "mode": "muxed_source_copy", "audio_selection": "first_optional", "tracks": []any{track}})
			if marshalErr != nil {
				t.Fatal(marshalErr)
			}
			var accepted bool
			if queryErr := c.QueryRow(ctx, `SELECT valid_recording_clip_timestamp_contract($1::jsonb)`, payload).Scan(&accepted); queryErr != nil {
				t.Fatal(queryErr)
			}
			if accepted {
				t.Fatalf("incomplete contract accepted: %s", payload)
			}
		})
	}
	for _, key := range []string{"version", "mode", "audio_selection", "tracks"} {
		t.Run("missing contract "+key, func(t *testing.T) {
			payloadMap := map[string]any{"version": 1, "mode": "muxed_source_copy", "audio_selection": "first_optional", "tracks": []any{baseTrack}}
			delete(payloadMap, key)
			payload, marshalErr := json.Marshal(payloadMap)
			if marshalErr != nil {
				t.Fatal(marshalErr)
			}
			var accepted bool
			if queryErr := c.QueryRow(ctx, `SELECT valid_recording_clip_timestamp_contract($1::jsonb)`, payload).Scan(&accepted); queryErr != nil {
				t.Fatal(queryErr)
			}
			if accepted {
				t.Fatalf("incomplete contract accepted: %s", payload)
			}
		})
	}
	audioTrack := map[string]any{
		"stream_index": 1, "media_type": "audio", "time_base_num": 1, "time_base_den": 48000,
		"first_timestamp": 0, "last_timestamp": 1024, "last_duration": 1024, "unit_count": 2,
		"codec_signature_sha256": strings.Repeat("b", 64), "sample_rate": 48000, "last_sample_count": 1024,
	}
	for _, key := range []string{"sample_rate", "last_sample_count"} {
		t.Run("missing audio "+key, func(t *testing.T) {
			audio := make(map[string]any, len(audioTrack))
			for k, v := range audioTrack {
				audio[k] = v
			}
			delete(audio, key)
			payload, marshalErr := json.Marshal(map[string]any{"version": 1, "mode": "muxed_source_copy", "audio_selection": "first_optional", "tracks": []any{baseTrack, audio}})
			if marshalErr != nil {
				t.Fatal(marshalErr)
			}
			var accepted bool
			if queryErr := c.QueryRow(ctx, `SELECT valid_recording_clip_timestamp_contract($1::jsonb)`, payload).Scan(&accepted); queryErr != nil {
				t.Fatal(queryErr)
			}
			if accepted {
				t.Fatalf("incomplete audio contract accepted: %s", payload)
			}
		})
	}
	if _, err = c.Exec(ctx, `INSERT INTO recording_clips(id,capture_lease_token,capture_sequence,capture_attempt_id,timestamp_contract_version,timestamp_contract,timestamp_contract_status) VALUES(1,$1,1,$2,'continuous-source-pts-v1',$3,'per_clip_probe_complete')`, lease, attempt, contract); err != nil {
		t.Fatal(err)
	}
	if _, err = c.Exec(ctx, `UPDATE recording_clips SET timestamp_contract_status='per_clip_probe_unknown',timestamp_contract_version=NULL,timestamp_contract=NULL,timestamp_contract_reason='probe_unavailable' WHERE id=1`); err == nil {
		t.Fatal("timestamp provenance mutation succeeded")
	}
	if _, err = c.Exec(ctx, `UPDATE recording_clips SET capture_sequence=2 WHERE id=1`); err == nil {
		t.Fatal("capture sequence mutation succeeded")
	}
	if _, err = c.Exec(ctx, `INSERT INTO recording_clips(id,capture_lease_token,capture_sequence,capture_attempt_id,timestamp_contract_status) VALUES(2,$1,2,$2,'per_clip_probe_unknown')`, lease, attempt); err == nil {
		t.Fatal("unknown timestamp provenance without a reason succeeded")
	}
	if _, err = c.Exec(ctx, `INSERT INTO recording_clips(id,capture_lease_token,capture_sequence,capture_attempt_id,timestamp_contract_version,timestamp_contract,timestamp_contract_status) VALUES(3,$1,3,$2,'continuous-source-pts-v1','{"version":1,"mode":"muxed_source_copy","audio_selection":"first_optional","tracks":[]}','per_clip_probe_complete')`, lease, attempt); err == nil {
		t.Fatal("complete timestamp provenance without track evidence succeeded")
	}
	if _, err = c.Exec(ctx, `UPDATE recording_clips SET released_at=now(),purged_at=now() WHERE id=1`); err != nil {
		t.Fatalf("legitimate lifecycle update rejected: %v", err)
	}
	if _, err = c.Exec(ctx, `INSERT INTO recordings VALUES(9,42)`); err != nil {
		t.Fatal(err)
	}
	if _, err = c.Exec(ctx, `INSERT INTO recording_jobs VALUES(7,9,$1,'leased','node:3',now()+interval '1 hour')`, lease); err != nil {
		t.Fatal(err)
	}
	if _, err = c.Exec(ctx, `INSERT INTO recording_jobs VALUES
		(8,9,$1,'leased','node:3',now()+interval '1 hour'),
		(9,9,$2,'pending','node:3',now()+interval '1 hour'),
		(10,9,$2,'leased','node:4',now()+interval '1 hour'),
		(11,9,$2,'leased','node:3',now()-interval '1 second')`, lease, wrongLease); err != nil {
		t.Fatal(err)
	}
	if _, err = c.Exec(ctx, `INSERT INTO nodes VALUES(3,42,'relay')`); err != nil {
		t.Fatal(err)
	}
	if _, err = c.Exec(ctx, `INSERT INTO recording_timestamp_contract_admissions(recording_job_id,lease_token,node_id,account_id,recording_id,policy_version) VALUES(7,$1,3,42,9,'continuous-source-pts-v1')`, lease); err != nil {
		t.Fatal(err)
	}
	if _, err = c.Exec(ctx, `INSERT INTO recording_timestamp_contract_admissions(recording_job_id,lease_token,node_id,account_id,recording_id,policy_version) VALUES(8,$1,3,42,9,'continuous-source-pts-v1')`, wrongLease); err == nil {
		t.Fatal("wrong lease token admission succeeded")
	}
	if _, err = c.Exec(ctx, `INSERT INTO recording_timestamp_contract_admissions(recording_job_id,lease_token,node_id,account_id,recording_id,policy_version) VALUES(9,$1,3,42,9,'continuous-source-pts-v1')`, wrongLease); err == nil {
		t.Fatal("non-leased admission succeeded")
	}
	if _, err = c.Exec(ctx, `INSERT INTO recording_timestamp_contract_admissions(recording_job_id,lease_token,node_id,account_id,recording_id,policy_version) VALUES(10,$1,3,42,9,'continuous-source-pts-v1')`, wrongLease); err == nil {
		t.Fatal("wrong owner admission succeeded")
	}
	if _, err = c.Exec(ctx, `INSERT INTO recording_timestamp_contract_admissions(recording_job_id,lease_token,node_id,account_id,recording_id,policy_version) VALUES(11,$1,3,42,9,'continuous-source-pts-v1')`, wrongLease); err == nil {
		t.Fatal("expired admission succeeded")
	}
	if _, err = c.Exec(ctx, `INSERT INTO recording_timestamp_contract_admissions(recording_job_id,lease_token,node_id,account_id,recording_id,policy_version,admitted_at) VALUES(8,$1,3,42,9,'continuous-source-pts-v1',now()-interval '1 minute')`, lease); err == nil {
		t.Fatal("backdated admission succeeded")
	}
	if _, err = c.Exec(ctx, `UPDATE recording_timestamp_contract_admissions SET node_id=4`); err == nil {
		t.Fatal("admission mutation succeeded")
	}
	if _, err = c.Exec(ctx, `DELETE FROM recording_timestamp_contract_admissions`); err == nil {
		t.Fatal("admission delete succeeded")
	}
	if _, err = c.Exec(ctx, `TRUNCATE recording_timestamp_contract_admissions`); err == nil {
		t.Fatal("admission truncate succeeded")
	}
}
