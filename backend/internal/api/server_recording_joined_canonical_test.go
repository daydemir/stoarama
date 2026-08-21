package api

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/daydemir/stoarama/backend/internal/joinedauth"
	"github.com/daydemir/stoarama/backend/internal/joinedrecording"
	"github.com/daydemir/stoarama/backend/internal/r2"
	"github.com/daydemir/stoarama/backend/internal/recordingnaming"
	"github.com/jackc/pgx/v5"
)

// This uses the real migration in a disposable PostgreSQL schema when the
// standard repository test URL is configured.
func TestJoinedCanonicalLedgerPublicationFeedAndExactAck(t *testing.T) {
	s, pool, cleanup := testIdentityServer(t)
	defer cleanup()
	s.cfg.JoinedRecordingEnabled = true
	s.cfg.JoinedWorkerBootstrapToken = "joined-bootstrap-key"
	s.cfg.JoinedWorkerSigningKey = "joined-worker-signing-key"
	s.cfg.R2Endpoint = "https://output.example.test"
	s.cfg.R2Bucket = "joined-output"
	s.cfg.R2Region = "auto"
	s.cfg.R2AccessKeyID = "output-key"
	s.cfg.R2SecretAccessKey = "output-secret"
	ctx := context.Background()
	_, accountID := seedUserOrg(t, pool, "joined-canonical@example.test", false)

	var storageID int64
	if err := pool.QueryRow(ctx, `INSERT INTO storage_destinations(account_id,name,endpoint,region,bucket,
		access_key_id,secret_access_key_enc,status) VALUES($1,'joined-test','https://source.example.test','auto',
		'clips','key',$2,'verified') RETURNING id`, accountID, []byte{1}).Scan(&storageID); err != nil {
		t.Fatal(err)
	}
	var apiKeyID int64
	if err := pool.QueryRow(ctx, `INSERT INTO account_api_keys(account_id,key_prefix,secret_hash,label,scopes)
		VALUES($1,'sir_joined','joined-test-hash','NAS',ARRAY['stoarama.pull']) RETURNING id`, accountID).Scan(&apiKeyID); err != nil {
		t.Fatal(err)
	}
	var connectionID int64
	if err := pool.QueryRow(ctx, `INSERT INTO connections(account_id,kind,label,api_key_id,joined_protocol_version)
		VALUES($1,'nas_pull','NAS',$2,1) RETURNING id`, accountID, apiKeyID).Scan(&connectionID); err != nil {
		t.Fatal(err)
	}
	firstDate := time.Date(2026, time.May, 4, 8, 0, 0, 0, time.UTC)
	var recordingID int64
	if err := pool.QueryRow(ctx, `INSERT INTO recordings(account_id,storage_destination_id,name,stream_url,cron_timezone,
		mode,daily_window_start,daily_window_end,delivery,start_at,end_at) VALUES($1,$2,'joined-recording',
		'https://example.test/live.m3u8','UTC','continuous','08:00','20:00','nas_pull',$3,$4) RETURNING id`,
		accountID, storageID, firstDate, firstDate.AddDate(0, 0, 14)).Scan(&recordingID); err != nil {
		t.Fatal(err)
	}
	qualification := joinedrecording.QualificationWindow{RecordingID: recordingID, Timezone: "UTC",
		FrozenAt: firstDate.AddDate(0, 0, 15)}
	jobIDs := make([]int64, 14)
	for day := 0; day < 14; day++ {
		start := firstDate.AddDate(0, 0, day)
		if err := pool.QueryRow(ctx, `INSERT INTO recording_jobs(recording_id,fire_at,scheduled_for,clip_duration_sec,status,
			idempotency_key,kind,window_end_at,completed_at) VALUES($1,$2,$2,60,'done',$3,'continuous_window',$4,$4)
			RETURNING id`, recordingID, start, fmt.Sprintf("joined:%d:%d", recordingID, day), start.Add(12*time.Hour)).Scan(&jobIDs[day]); err != nil {
			t.Fatal(err)
		}
		qualification.Days = append(qualification.Days, joinedrecording.QualifiedDay{LocalDate: start.Format("2006-01-02"),
			JobID: jobIDs[day], WindowStart: start, WindowEnd: start.Add(12 * time.Hour), CompletedAt: start.Add(12 * time.Hour),
			QualityTier: "good+"})
	}
	sourceDate := firstDate.AddDate(0, 0, 12)
	sources := make([]joinedrecording.SourceClip, 3)
	for i := range sources {
		start := sourceDate.Add(time.Duration(50+i*20) * time.Minute)
		key, etag, sha := fmt.Sprintf("raw/source-%d.mp4", i+1), fmt.Sprintf("etag-%d", i+1), strings.Repeat(fmt.Sprintf("%x", i+1), 64)
		var clipID int64
		if err := pool.QueryRow(ctx, `INSERT INTO recording_clips(recording_id,recording_job_id,storage_destination_id,
			endpoint,bucket,object_key,display_path,mime_type,container,size_bytes,etag,sha256,duration_ms,video_codec,
			audio_present,fire_at,clip_start_at,clip_end_at,created_at) VALUES($1,$2,$3,'https://source.example.test',
			'clips',$4,$4,'video/mp4','mp4',10,$5,$6,60000,'h264',false,$7,$7,$8,$7) RETURNING id`, recordingID,
			jobIDs[12], storageID, key, etag, sha, start, start.Add(time.Minute)).Scan(&clipID); err != nil {
			t.Fatal(err)
		}
		sources[i] = joinedrecording.SourceClip{ClipID: clipID, RecordingID: recordingID, RecordingJobID: jobIDs[12],
			Provider: "s3_compatible", Endpoint: "https://source.example.test", Region: "auto", Bucket: "clips", StartUTC: start,
			EndUTC: start.Add(time.Minute), Object: joinedrecording.ObjectIdentity{Key: key, ETag: etag, SizeBytes: 10, SHA256: sha}}
	}
	qualification, err := joinedrecording.SealQualificationWindow(qualification)
	if err != nil {
		t.Fatal(err)
	}
	mediaTool, err := joinedrecording.SealMediaToolEvidence(joinedrecording.MediaToolEvidence{
		FFmpegVersion: "ffmpeg pinned", FFmpegSHA256: strings.Repeat("e", 64),
		FFprobeVersion: "ffprobe pinned", FFprobeSHA256: strings.Repeat("f", 64)})
	if err != nil {
		t.Fatal(err)
	}
	metadata := recordingnaming.Metadata{PlazaID: "1", Continent: "Europe", Country: "Italy", City: "Bevagna", PlazaName: "Piazza"}
	folder, err := recordingnaming.BuildFolderName(recordingnaming.ProfilePlazaHourlyV1, recordingID, metadata, "")
	if err != nil {
		t.Fatal(err)
	}
	batchID := "goodplus-20260821-generation-1"
	qualificationJSON, _ := json.Marshal(qualification)
	mediaToolJSON, _ := json.Marshal(mediaTool)
	metadataJSON, _ := json.Marshal(metadata)
	var batchRecordID int64
	if _, err := pool.Exec(ctx, `INSERT INTO recording_joined_batches(account_id,connection_id,batch_id,generation,
		policy_version,eligibility_cutoff,media_tool,media_tool_sha256,freeze_request,freeze_request_sha256,
		frozen_denominator_sha256,expected_recordings,expected_stream_days,expected_scheduled_hours,
		expected_source_clips,expected_source_bytes,freeze_started_at)
		VALUES($1,$2,'preseed-generation-1',1,$3,$4,$5,$6,'{"schema_version":1}',$7,$8,1,14,168,0,0,now())`,
		accountID, connectionID, joinedrecording.PlanPolicyVersion, qualification.FrozenAt, mediaToolJSON,
		mediaTool.IdentitySHA256, strings.Repeat("9", 64), strings.Repeat("8", 64)); err == nil {
		t.Fatal("batch insert preseeded freeze evidence")
	}
	if err := pool.QueryRow(ctx, `INSERT INTO recording_joined_batches(account_id,connection_id,batch_id,generation,
		policy_version,eligibility_cutoff,media_tool,media_tool_sha256,freeze_request,freeze_request_sha256,
		frozen_denominator_sha256,expected_recordings,expected_stream_days,expected_scheduled_hours,
		expected_source_clips,expected_source_bytes)
		VALUES($1,$2,$3,1,$4,$5,$6,$7,'{"schema_version":1}',$8,$9,1,14,168,3,30) RETURNING id`,
		accountID, connectionID, batchID, joinedrecording.PlanPolicyVersion, qualification.FrozenAt, mediaToolJSON,
		mediaTool.IdentitySHA256, strings.Repeat("a", 64), strings.Repeat("b", 64)).Scan(&batchRecordID); err != nil {
		t.Fatal(err)
	}
	var batchRecordingID int64
	if err := pool.QueryRow(ctx, `INSERT INTO recording_joined_batch_recordings(batch_record_id,account_id,connection_id,
		batch_id,recording_id,priority_ordinal,timezone,folder_name,naming_metadata,first_local_date,last_local_date,
		qualification,qualification_sha256,qualification_policy_version,authoritative_job_ids)
		VALUES($1,$2,$3,$4,$5,1,'UTC',$6,$7,$8,$8::date+13,$9,$10,'good-plus-v1',$11) RETURNING id`,
		batchRecordID, accountID, connectionID, batchID, recordingID, folder, metadataJSON, firstDate.Format("2006-01-02"),
		qualificationJSON, qualification.EvidenceSHA, jobIDs).Scan(&batchRecordingID); err != nil {
		t.Fatal(err)
	}
	type ledgerFixture struct {
		artifactID, streamDayID          int64
		scopeID, relativePath, objectKey string
		bytes                            []byte
		sha                              string
	}
	ledgers := make([]ledgerFixture, 0, 14)
	var insertFinalSource func(pgx.Tx) error
	for day := 0; day < 14; day++ {
		date := firstDate.AddDate(0, 0, day)
		request := joinedrecording.PlanRequest{BatchID: batchID, Generation: 1, RecordingID: recordingID,
			Timezone: "UTC", LocalDate: date.Format("2006-01-02"), Qualification: qualification}
		if day == 11 {
			request.NextDayFirst = &sources[0]
		} else if day == 12 {
			request.Sources = sources
		} else if day == 13 {
			request.PreviousDayLast = &sources[len(sources)-1]
		}
		var draft joinedrecording.StreamDayDraft
		var err error
		if day == 12 {
			draft, err = joinedrecording.AllocateStreamDay(request)
		} else {
			draft, err = joinedrecording.BuildGapOnlyStreamDay(request, date.Format("2006-01-02"))
		}
		if err != nil {
			t.Fatal(err)
		}
		ledger, err := joinedrecording.SealStreamDayAllocation(draft)
		if err != nil {
			t.Fatal(err)
		}
		ledgerBytes, ledgerSHA, err := joinedrecording.CanonicalAllocationLedgerArtifact(ledger)
		if err != nil {
			t.Fatal(err)
		}
		var streamDayID int64
		if err := pool.QueryRow(ctx, `INSERT INTO recording_joined_stream_days(batch_record_id,batch_recording_id,
			account_id,connection_id,batch_id,recording_id,local_date,date_ordinal,recording_job_id,scheduled_start_at,
			scheduled_end_at,source_clip_count,source_bytes,source_manifest_sha256,ledger_sha256,ledger_bytes,
			ledger_artifact_sha256) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17) RETURNING id`,
			batchRecordID, batchRecordingID, accountID, connectionID, batchID, recordingID, date.Format("2006-01-02"),
			day+1, jobIDs[day], date, date.Add(12*time.Hour), len(request.Sources), int64(len(request.Sources)*10),
			ledger.SourceClaimSHA256, ledger.LedgerSHA256, ledgerBytes, ledgerSHA).Scan(&streamDayID); err != nil {
			t.Fatal(err)
		}
		for hour := 1; hour <= 12; hour++ {
			hourID := fmt.Sprintf("%s__recording-%d__date-%s__hour-%02d__generation-1", batchID, recordingID,
				date.Format("2006-01-02"), hour)
			scheduledStart := date.Add(time.Duration(hour-1) * time.Hour)
			if _, err := pool.Exec(ctx, `INSERT INTO recording_joined_hours(batch_record_id,stream_day_id,account_id,
				connection_id,batch_id,recording_id,hour_id,local_date,delivery_hour,clock_hour,scheduled_start_at,
				scheduled_end_at,priority_ordinal,source_clip_count,source_bytes,source_claim_sha256)
				VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16)`, batchRecordID, streamDayID,
				accountID, connectionID, batchID, recordingID, hourID, date.Format("2006-01-02"), hour, hour+7,
				scheduledStart, scheduledStart.Add(time.Hour), int64(day*12+hour), len(ledger.Hours[hour-1].SourceClipIDs),
				int64(len(ledger.Hours[hour-1].SourceClipIDs)*10), ledger.HourSourceSHA256[hour-1]); err != nil {
				t.Fatal(err)
			}
		}
		if day == 12 {
			insertSources := func(tx pgx.Tx, swapHours, alterSeam bool, firstOrdinal, lastOrdinal int) error {
				dayOrdinal := 0
				for hourIndex, hour := range draft.Hours {
					for hourOrdinal, source := range hour.Sources {
						dayOrdinal++
						if dayOrdinal < firstOrdinal || dayOrdinal > lastOrdinal {
							continue
						}
						seam, _ := json.Marshal(source.SeamToPrevious)
						if alterSeam && dayOrdinal == 2 {
							seam = []byte(`{"verdict":"fabricated","reason":"fabricated","signed_gap_nanoseconds":0}`)
						}
						deliveryHour := hourIndex + 1
						if swapHours {
							deliveryHour = 3 - deliveryHour
						}
						if _, err := tx.Exec(ctx, `INSERT INTO recording_joined_sources(batch_record_id,stream_day_id,
						hour_record_id,account_id,connection_id,recording_id,recording_job_id,clip_id,storage_destination_id,
						day_ordinal,hour_ordinal,provider,endpoint,region,bucket,object_key,version_id,etag,size_bytes,sha256,
						start_at,end_at,seam_to_previous,clip_created_at,released_at)
						SELECT $1,$2,h.id,$3,$4,rc.recording_id,rc.recording_job_id,rc.id,rc.storage_destination_id,$5,$6,
						sd.provider,rc.endpoint,sd.region,rc.bucket,rc.object_key,'',rc.etag,rc.size_bytes,rc.sha256,
						rc.clip_start_at,rc.clip_end_at,$7,rc.created_at,rc.released_at
						FROM recording_joined_hours h, recording_clips rc JOIN storage_destinations sd ON sd.id=rc.storage_destination_id
						WHERE h.stream_day_id=$2 AND h.delivery_hour=$8 AND rc.id=$9`, batchRecordID, streamDayID,
							accountID, connectionID, dayOrdinal, hourOrdinal+1, seam, deliveryHour, source.ClipID); err != nil {
							return err
						}
					}
				}
				return nil
			}
			insertFinalSource = func(tx pgx.Tx) error {
				return insertSources(tx, false, false, len(sources), len(sources))
			}
			for _, invalid := range []struct {
				name       string
				swap, seam bool
			}{{name: "swapped hour membership", swap: true}, {name: "ledger source evidence", seam: true}} {
				badTx, err := pool.Begin(ctx)
				if err != nil {
					t.Fatal(err)
				}
				if err := insertSources(badTx, invalid.swap, invalid.seam, 1, len(sources)); err != nil {
					t.Fatal(err)
				}
				if _, err := badTx.Exec(ctx, `SELECT validate_recording_joined_stream_day($1)`, streamDayID); err == nil {
					t.Fatalf("accepted invalid %s", invalid.name)
				}
				_ = badTx.Rollback(ctx)
			}
			goodTx, err := pool.Begin(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if err := insertSources(goodTx, false, false, 1, len(sources)-1); err != nil {
				t.Fatal(err)
			}
			if err := goodTx.Commit(ctx); err != nil {
				t.Fatal(err)
			}
		}
		for i, boundary := range ledger.Boundaries {
			if _, err := pool.Exec(ctx, `INSERT INTO recording_joined_day_boundaries(stream_day_id,boundary_kind,ordinal,
				previous_delivery_hour,next_delivery_hour,previous_clip_id,next_clip_id,previous_presentation_end_at,
				next_presentation_start_at,signed_gap_nanoseconds,scheduled_at,actual_seam_at,boundary_skew_nanoseconds,
				allocation_decision,verdict,reason) VALUES($1,'cross_hour',$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15)`,
				streamDayID, i+1, boundary.PreviousHour, boundary.NextHour, boundary.PreviousClipID, boundary.NextClipID,
				boundary.PreviousPresentationEndUTC, boundary.NextPresentationStartUTC, boundary.SignedGapNanoseconds,
				boundary.ScheduledUTC, boundary.ActualSeamUTC, boundary.BoundarySkewNanoseconds, boundary.AllocationDecision,
				boundary.Verdict, boundary.Reason); err != nil {
				t.Fatal(err)
			}
		}
		for i, boundary := range ledger.CrossDayBoundaries {
			if _, err := pool.Exec(ctx, `INSERT INTO recording_joined_day_boundaries(stream_day_id,boundary_kind,ordinal,
				previous_clip_id,next_clip_id,previous_presentation_end_at,next_presentation_start_at,signed_gap_nanoseconds,
				scheduled_previous_end_at,scheduled_next_start_at,boundary_skew_nanoseconds,allocation_decision,verdict,reason)
				VALUES($1,'cross_day',$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)`, streamDayID, i+1,
				boundary.PreviousClipID, boundary.NextClipID, boundary.PreviousPresentationEndUTC, boundary.NextPresentationStartUTC,
				boundary.SignedGapNanoseconds, boundary.ScheduledPreviousEndUTC, boundary.ScheduledNextStartUTC,
				boundary.BoundarySkewNanoseconds, boundary.AllocationDecision, boundary.Verdict, boundary.Reason); err != nil {
				t.Fatal(err)
			}
		}
		var artifactID int64
		if err := pool.QueryRow(ctx, `SELECT nextval('recording_joined_artifacts_id_seq')`).Scan(&artifactID); err != nil {
			t.Fatal(err)
		}
		relativePath, objectKey, err := joinedrecording.CanonicalAllocationLedgerPaths(batchID, recordingID, date.Format("2006-01-02"))
		if err != nil {
			t.Fatal(err)
		}
		ledgers = append(ledgers, ledgerFixture{artifactID: artifactID, streamDayID: streamDayID,
			scopeID:      fmt.Sprintf("%s__recording-%d__date-%s__generation-1", batchID, recordingID, date.Format("2006-01-02")),
			relativePath: relativePath, objectKey: objectKey, bytes: ledgerBytes, sha: ledgerSHA})
	}
	for day, ledger := range ledgers {
		if day >= 12 {
			continue
		}
		if _, err := pool.Exec(ctx, `SELECT validate_recording_joined_stream_day($1)`, ledger.streamDayID); err != nil {
			t.Fatalf("validate stream day %d: %v", day+1, err)
		}
	}
	if _, err := pool.Exec(ctx, `UPDATE recording_joined_batches SET state='frozen',frozen_at=clock_timestamp()
		WHERE id=$1`, batchRecordID); err == nil {
		t.Fatal("batch froze without a separate freeze-start statement")
	}
	repeatableFreeze, err := pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := repeatableFreeze.Exec(ctx, `UPDATE recording_joined_batches SET freeze_started_at=clock_timestamp()
		WHERE id=$1`, batchRecordID); err == nil {
		t.Fatal("repeatable-read batch freeze was accepted")
	}
	_ = repeatableFreeze.Rollback(ctx)

	waitForBlocker := func(waiterPID, blockerPID int32) {
		waitCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		for waitCtx.Err() == nil {
			var blocked bool
			if err := pool.QueryRow(waitCtx, `SELECT $2::INTEGER=ANY(pg_blocking_pids($1::INTEGER))`,
				waiterPID, blockerPID).Scan(&blocked); err != nil {
				if waitCtx.Err() != nil {
					break
				}
				t.Fatal(err)
			}
			if blocked {
				return
			}
		}
		t.Fatalf("backend %d was not blocked by backend %d", waiterPID, blockerPID)
	}
	childTx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var childPID int32
	if err := childTx.QueryRow(ctx, `SELECT pg_backend_pid()`).Scan(&childPID); err != nil {
		t.Fatal(err)
	}
	if insertFinalSource == nil {
		t.Fatal("source-bearing fixture did not expose its final source")
	}
	if err := insertFinalSource(childTx); err != nil {
		t.Fatal(err)
	}
	freezeConn, err := pool.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var freezePID int32
	if err := freezeConn.QueryRow(ctx, `SELECT pg_backend_pid()`).Scan(&freezePID); err != nil {
		t.Fatal(err)
	}
	freezeTx, err := freezeConn.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	freezeResult := make(chan error, 1)
	go func() {
		command, updateErr := freezeTx.Exec(ctx, `UPDATE recording_joined_batches SET freeze_started_at=clock_timestamp()
			WHERE id=$1`, batchRecordID)
		if updateErr == nil && command.RowsAffected() != 1 {
			updateErr = fmt.Errorf("freeze start rows=%d", command.RowsAffected())
		}
		freezeResult <- updateErr
	}()
	waitForBlocker(freezePID, childPID)
	if err := childTx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if err := <-freezeResult; err != nil {
		t.Fatalf("freeze start after child commit: %v", err)
	}
	if _, err := freezeTx.Exec(ctx, `SELECT validate_recording_joined_stream_day($1)`, ledgers[12].streamDayID); err != nil {
		t.Fatalf("freeze did not see committed source child: %v", err)
	}
	if command, err := freezeTx.Exec(ctx, `UPDATE recording_joined_batches SET state='frozen',frozen_at=clock_timestamp()
		WHERE id=$1`, batchRecordID); err != nil || command.RowsAffected() != 1 {
		t.Fatalf("freeze after child commit rows=%d err=%v", command.RowsAffected(), err)
	}

	lateConn, err := pool.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var latePID int32
	if err := lateConn.QueryRow(ctx, `SELECT pg_backend_pid()`).Scan(&latePID); err != nil {
		t.Fatal(err)
	}
	lateResult := make(chan error, 1)
	go func() {
		_, insertErr := lateConn.Exec(ctx, `INSERT INTO recording_joined_freeze_exclusions(batch_record_id,batch_recording_id,
			recording_id,reason_code,evidence_sha256,canonical_evidence) VALUES($1,$2,$3,'late_source',$4,'{"late":true}')`,
			batchRecordID, batchRecordingID, recordingID, strings.Repeat("d", 64))
		lateResult <- insertErr
	}()
	waitForBlocker(latePID, freezePID)
	if err := freezeTx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if err := <-lateResult; err == nil {
		t.Fatal("frozen batch accepted a child that waited on freeze")
	}
	lateConn.Release()
	freezeConn.Release()
	for _, ledger := range ledgers {
		if _, err := pool.Exec(ctx, `INSERT INTO recording_joined_artifacts(id,batch_record_id,account_id,connection_id,
			batch_id,scope_kind,scope_id,stream_day_id,artifact_kind,ordinal,relative_path,object_key,content_type,
			expected_size_bytes,expected_sha256,canonical_bytes,publication_state) VALUES($1,$2,$3,$4,$5,'ledger',$6,$7,
			'allocation_ledger',1,$8,$9,'application/json',$10,$11,$12,'sealed')`, ledger.artifactID, batchRecordID,
			accountID, connectionID, batchID, ledger.scopeID, ledger.streamDayID, ledger.relativePath, ledger.objectKey,
			len(ledger.bytes), ledger.sha, ledger.bytes); err != nil {
			t.Fatal(err)
		}
	}
	ledgerArtifactID, ledgerRelative, ledgerObject := ledgers[0].artifactID, ledgers[0].relativePath, ledgers[0].objectKey
	ledgerBytes, ledgerArtifactSHA := ledgers[0].bytes, ledgers[0].sha

	claimToken, err := joinedauth.MintClaim(s.cfg.JoinedWorkerSigningKey, batchID, time.Now().Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	claimBody, _ := json.Marshal(joinedrecording.PublicationClaimRequest{ProtocolVersion: 1, BatchID: batchID, WorkerID: "worker-1"})
	claimReq := httptest.NewRequest(http.MethodPost, "/api/v1/recording/joined/publication/claim", bytes.NewReader(claimBody))
	claimReq.Header.Set("Authorization", "Bearer "+claimToken)
	claimRec := httptest.NewRecorder()
	s.requireJoinedWorkerAuth(http.HandlerFunc(s.handleJoinedPublicationClaim)).ServeHTTP(claimRec, claimReq)
	var publication joinedrecording.PublicationClaimResponse
	if claimRec.Code != http.StatusOK || json.Unmarshal(claimRec.Body.Bytes(), &publication) != nil ||
		publication.Ledger == nil || publication.Ledger.ArtifactID != ledgerArtifactID {
		t.Fatalf("publication claim status=%d body=%s", claimRec.Code, claimRec.Body.String())
	}
	headKeys := []string{}
	s.joinedOutputStorage = joinedOutputStoreStub{head: r2.ObjectHead{ETag: "ledger-etag", SizeBytes: int64(len(ledgerBytes))},
		headKeys: &headKeys}
	published := joinedrecording.PublishedLedger{ArtifactID: ledgerArtifactID, ObjectKey: ledgerObject, ETag: "ledger-etag",
		SizeBytes: int64(len(ledgerBytes)), SHA256: ledgerArtifactSHA}
	wrongPublished := published
	wrongPublished.ObjectKey += ".foreign"
	wrongFinalizeBody, _ := json.Marshal(joinedrecording.FinalizeLedgerRequest{ProtocolVersion: 1, Published: wrongPublished})
	wrongFinalizeReq := httptest.NewRequest(http.MethodPost, "/api/v1/recording/joined/publication/ledger/finalize",
		bytes.NewReader(wrongFinalizeBody))
	wrongFinalizeReq.Header.Set("Authorization", "Bearer "+publication.Ledger.OperationToken)
	wrongFinalizeRec := httptest.NewRecorder()
	s.requireJoinedWorkerAuth(http.HandlerFunc(s.handleJoinedFinalizeLedger)).ServeHTTP(wrongFinalizeRec, wrongFinalizeReq)
	if wrongFinalizeRec.Code != http.StatusConflict || len(headKeys) != 0 {
		t.Fatalf("foreign finalize status=%d head_keys=%v", wrongFinalizeRec.Code, headKeys)
	}
	finalizeBody, _ := json.Marshal(joinedrecording.FinalizeLedgerRequest{ProtocolVersion: 1, Published: published})
	finalizeReq := httptest.NewRequest(http.MethodPost, "/api/v1/recording/joined/publication/ledger/finalize", bytes.NewReader(finalizeBody))
	finalizeReq.Header.Set("Authorization", "Bearer "+publication.Ledger.OperationToken)
	finalizeRec := httptest.NewRecorder()
	s.requireJoinedWorkerAuth(http.HandlerFunc(s.handleJoinedFinalizeLedger)).ServeHTTP(finalizeRec, finalizeReq)
	if finalizeRec.Code != http.StatusNoContent {
		t.Fatalf("finalize status=%d body=%s", finalizeRec.Code, finalizeRec.Body.String())
	}
	if len(headKeys) != 1 || headKeys[0] != ledgerObject {
		t.Fatalf("finalize HEAD keys=%v", headKeys)
	}
	var gapManifestCount int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM recording_joined_artifacts
		WHERE stream_day_id=$1 AND artifact_kind='hour_manifest' AND publication_state='sealed'`, ledgers[0].streamDayID).
		Scan(&gapManifestCount); err != nil || gapManifestCount != 12 {
		t.Fatalf("server gap-only manifests=%d err=%v", gapManifestCount, err)
	}
	principal := accountPrincipal{AccountID: accountID, APIKeyID: &apiKeyID, KeyScopes: []string{accountScopePull}}
	feedReq := httptest.NewRequest(http.MethodGet, "/api/v1/account/joined", nil)
	feedReq = feedReq.WithContext(context.WithValue(feedReq.Context(), accountPrincipalContextKey, principal))
	feedRec := httptest.NewRecorder()
	s.handleAccountJoined(feedRec, feedReq)
	var feed struct {
		Item *joinedArtifactItem `json:"item"`
	}
	if feedRec.Code != http.StatusOK || json.Unmarshal(feedRec.Body.Bytes(), &feed) != nil || feed.Item == nil ||
		feed.Item.ArtifactID != ledgerArtifactID || feed.Item.ConnectionID != connectionID {
		t.Fatalf("feed status=%d body=%s", feedRec.Code, feedRec.Body.String())
	}
	wrongAck, _ := json.Marshal(joinedAckRequest{ArtifactID: ledgerArtifactID, RelativePath: "wrong.json",
		SizeBytes: int64(len(ledgerBytes)), SHA256: ledgerArtifactSHA})
	wrongReq := httptest.NewRequest(http.MethodPost, "/api/v1/account/joined/ack", bytes.NewReader(wrongAck))
	wrongReq = wrongReq.WithContext(context.WithValue(wrongReq.Context(), accountPrincipalContextKey, principal))
	wrongRec := httptest.NewRecorder()
	s.handleAccountJoinedAck(wrongRec, wrongReq)
	if wrongRec.Code != http.StatusConflict {
		t.Fatalf("wrong ACK status=%d body=%s", wrongRec.Code, wrongRec.Body.String())
	}
	exactAck, _ := json.Marshal(joinedAckRequest{ArtifactID: ledgerArtifactID, RelativePath: ledgerRelative,
		SizeBytes: int64(len(ledgerBytes)), SHA256: ledgerArtifactSHA})
	for attempt := 0; attempt < 2; attempt++ {
		req := httptest.NewRequest(http.MethodPost, "/api/v1/account/joined/ack", bytes.NewReader(exactAck))
		req = req.WithContext(context.WithValue(req.Context(), accountPrincipalContextKey, principal))
		rec := httptest.NewRecorder()
		s.handleAccountJoinedAck(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("exact ACK attempt=%d status=%d body=%s", attempt, rec.Code, rec.Body.String())
		}
	}
	// Publish the remaining ledgers through the same fenced DB transitions so
	// the source-bearing day becomes eligible for the actual worker handlers.
	for i := 1; i <= 12; i++ {
		token := fmt.Sprintf("00000000-0000-0000-0000-%012d", i+10)
		if _, err := pool.Exec(ctx, `UPDATE recording_joined_artifacts SET publication_state='publishing',
			publication_attempt_count=1,publication_token=$2,publication_claimed_by='fixture-publisher',
			publication_lease_expires_at=now()+interval '5 minutes',publication_heartbeat_at=now() WHERE id=$1`,
			ledgers[i].artifactID, token); err != nil {
			t.Fatal(err)
		}
		if _, err := pool.Exec(ctx, `UPDATE recording_joined_artifacts SET publication_state='published',
			publication_token=NULL,publication_claimed_by=NULL,publication_lease_expires_at=NULL,publication_heartbeat_at=NULL,
			finalized_token=$2,etag='fixture-ledger-etag',version_id='',published_at=now() WHERE id=$1`,
			ledgers[i].artifactID, token); err != nil {
			t.Fatal(err)
		}
	}
	ackLock, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var filesBefore, bytesBefore int64
	if err := ackLock.QueryRow(ctx, `SELECT joined_files_pulled,joined_bytes_pulled
		FROM connections WHERE id=$1 FOR UPDATE`, connectionID).Scan(&filesBefore, &bytesBefore); err != nil {
		t.Fatal(err)
	}
	concurrentAck := joinedAckRequest{ArtifactID: ledgers[1].artifactID, RelativePath: ledgers[1].relativePath,
		SizeBytes: int64(len(ledgers[1].bytes)), SHA256: ledgers[1].sha}
	concurrentAckBody, _ := json.Marshal(concurrentAck)
	ackDone := make(chan *httptest.ResponseRecorder, 2)
	for range 2 {
		go func() {
			req := httptest.NewRequest(http.MethodPost, "/api/v1/account/joined/ack", bytes.NewReader(concurrentAckBody))
			req = req.WithContext(context.WithValue(req.Context(), accountPrincipalContextKey, principal))
			rec := httptest.NewRecorder()
			s.handleAccountJoinedAck(rec, req)
			ackDone <- rec
		}()
	}
	blockedCtx, cancelBlocked := context.WithTimeout(ctx, 5*time.Second)
	defer cancelBlocked()
	for {
		var blocked int
		if err := pool.QueryRow(blockedCtx, `SELECT count(*) FROM pg_stat_activity a
			WHERE cardinality(pg_blocking_pids(a.pid))>0
			  AND a.query LIKE '%SELECT id FROM connections WHERE account_id=%'`).Scan(&blocked); err != nil {
			_ = ackLock.Rollback(ctx)
			t.Fatal(err)
		}
		if blocked >= 2 {
			break
		}
		if blockedCtx.Err() != nil {
			_ = ackLock.Rollback(ctx)
			t.Fatal("concurrent ACK handlers did not reach the connection fence")
		}
	}
	if err := ackLock.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		select {
		case rec := <-ackDone:
			if rec.Code != http.StatusOK {
				t.Fatalf("concurrent ACK status=%d body=%s", rec.Code, rec.Body.String())
			}
		case <-time.After(5 * time.Second):
			t.Fatal("concurrent ACK handler deadlocked")
		}
	}
	var ackRows int
	var filesAfter, bytesAfter int64
	if err := pool.QueryRow(ctx, `SELECT (SELECT count(*) FROM recording_joined_artifact_acks
		WHERE artifact_id=$1 AND connection_id=$2),joined_files_pulled,joined_bytes_pulled
		FROM connections WHERE id=$2`, ledgers[1].artifactID, connectionID).Scan(&ackRows, &filesAfter, &bytesAfter); err != nil ||
		ackRows != 1 || filesAfter != filesBefore+1 || bytesAfter != bytesBefore+int64(len(ledgers[1].bytes)) {
		t.Fatalf("concurrent ACK rows=%d files=%d/%d bytes=%d/%d err=%v", ackRows, filesAfter, filesBefore,
			bytesAfter, bytesBefore, err)
	}
	sourceClaimBody, _ := json.Marshal(joinedrecording.WorkClaimRequest{ProtocolVersion: 1, BatchID: batchID, WorkerID: "source-worker"})
	sourceClaimReq := httptest.NewRequest(http.MethodPost, "/api/v1/recording/joined/claim", bytes.NewReader(sourceClaimBody))
	sourceClaimReq.Header.Set("Authorization", "Bearer "+claimToken)
	sourceClaimRec := httptest.NewRecorder()
	s.requireJoinedWorkerAuth(http.HandlerFunc(s.handleJoinedClaim)).ServeHTTP(sourceClaimRec, sourceClaimReq)
	var sourceClaim joinedrecording.PreflightHourClaim
	if sourceClaimRec.Code != http.StatusOK || json.Unmarshal(sourceClaimRec.Body.Bytes(), &sourceClaim) != nil || len(sourceClaim.Sources) != 1 {
		t.Fatalf("source claim status=%d body=%s expected=%+v", sourceClaimRec.Code, sourceClaimRec.Body.String(), sources)
	}
	mediaSHA := strings.Repeat("3", 64)
	sealRequest := joinedrecording.SealHourRequest{ProtocolVersion: 1, HourID: sourceClaim.HourID,
		SourceClaimSHA256: sourceClaim.SourceClaimSHA256, AccountedSources: sourceClaim.Sources,
		Media: []joinedrecording.SealHourMedia{{Ordinal: 1, SourceClipIDs: []int64{sourceClaim.Sources[0].ClipID},
			SizeBytes: 100, SHA256: mediaSHA, Verification: joinedCanonicalPassingVerification()}}}
	sealBody, _ := json.Marshal(sealRequest)
	sealReq := httptest.NewRequest(http.MethodPost, "/api/v1/recording/joined/hour/seal", bytes.NewReader(sealBody))
	sealReq.Header.Set("Authorization", "Bearer "+sourceClaim.OperationToken)
	sealRec := httptest.NewRecorder()
	s.requireJoinedWorkerAuth(http.HandlerFunc(s.handleJoinedSealHour)).ServeHTTP(sealRec, sealReq)
	var sealed joinedrecording.WorkerClaim
	if sealRec.Code != http.StatusOK || json.Unmarshal(sealRec.Body.Bytes(), &sealed) != nil ||
		len(sealed.MediaArtifactIDs) != 1 || sealed.HourManifestArtifactID <= 0 {
		t.Fatalf("source seal status=%d body=%s", sealRec.Code, sealRec.Body.String())
	}
	var mediaRows, mediaSourceRows, dispositionRows, manifestRows int
	if err := pool.QueryRow(ctx, `SELECT
		(SELECT count(*) FROM recording_joined_artifacts WHERE hour_record_id=h.id AND artifact_kind='media'),
		(SELECT count(*) FROM recording_joined_media_sources ms JOIN recording_joined_artifacts a ON a.id=ms.artifact_id
			WHERE a.hour_record_id=h.id),
		(SELECT count(*) FROM recording_joined_hour_dispositions WHERE hour_record_id=h.id),
		(SELECT count(*) FROM recording_joined_artifacts WHERE hour_record_id=h.id AND artifact_kind='hour_manifest')
		FROM recording_joined_hours h WHERE h.hour_id=$1`, sealed.HourID).
		Scan(&mediaRows, &mediaSourceRows, &dispositionRows, &manifestRows); err != nil ||
		mediaRows != 1 || mediaSourceRows != 1 || dispositionRows != 1 || manifestRows != 1 {
		t.Fatalf("atomic source seal media=%d media_sources=%d dispositions=%d manifests=%d err=%v",
			mediaRows, mediaSourceRows, dispositionRows, manifestRows, err)
	}
	mediaOutput := sealed.Plan.Outputs[0]
	mediaETag, manifestETag := "media-etag", "manifest-etag"
	hourHeadKeys := []string{}
	s.joinedOutputStorage = joinedOutputStoreStub{heads: map[string]r2.ObjectHead{
		mediaOutput.ObjectKey:         {ETag: mediaETag, SizeBytes: mediaOutput.ExpectedSize},
		sealed.Plan.CoverageObjectKey: {ETag: manifestETag, SizeBytes: sealed.HourManifestExpectedSize},
	}, headKeys: &hourHeadKeys}
	publishedHour := joinedrecording.PublishedHour{HourID: sealed.HourID, RecordingID: sealed.Plan.RecordingID,
		LocalDate: sealed.Plan.LocalDate, LocalHour: sealed.Plan.LocalHour,
		Outputs: []joinedrecording.PublishedOutput{{ArtifactID: sealed.MediaArtifactIDs[0], ObjectKey: mediaOutput.ObjectKey,
			ETag: mediaETag, SizeBytes: mediaOutput.ExpectedSize, SHA256: mediaOutput.ExpectedSHA}},
		HourManifestObjectKey: sealed.Plan.CoverageObjectKey, HourManifestETag: manifestETag,
		HourManifestSizeBytes: sealed.HourManifestExpectedSize, HourManifestSHA256: sealed.HourManifestExpectedSHA}
	wrongHour := publishedHour
	wrongHour.Outputs = append([]joinedrecording.PublishedOutput{}, publishedHour.Outputs...)
	wrongHour.Outputs[0].SHA256 = strings.Repeat("4", 64)
	wrongHourBody, _ := json.Marshal(joinedrecording.FinalizeHourRequest{ProtocolVersion: 1, Published: wrongHour})
	wrongHourReq := httptest.NewRequest(http.MethodPost, "/api/v1/recording/joined/publication/hour/finalize",
		bytes.NewReader(wrongHourBody))
	wrongHourReq.Header.Set("Authorization", "Bearer "+sealed.OperationToken)
	wrongHourRec := httptest.NewRecorder()
	s.requireJoinedWorkerAuth(http.HandlerFunc(s.handleJoinedFinalizeHour)).ServeHTTP(wrongHourRec, wrongHourReq)
	if wrongHourRec.Code != http.StatusConflict || len(hourHeadKeys) != 0 {
		t.Fatalf("wrong hour finalize status=%d head_keys=%v body=%s", wrongHourRec.Code, hourHeadKeys, wrongHourRec.Body.String())
	}
	publishHourBody, _ := json.Marshal(joinedrecording.FinalizeHourRequest{ProtocolVersion: 1, Published: publishedHour})
	for attempt := 0; attempt < 2; attempt++ {
		req := httptest.NewRequest(http.MethodPost, "/api/v1/recording/joined/publication/hour/finalize", bytes.NewReader(publishHourBody))
		req.Header.Set("Authorization", "Bearer "+sealed.OperationToken)
		rec := httptest.NewRecorder()
		s.requireJoinedWorkerAuth(http.HandlerFunc(s.handleJoinedFinalizeHour)).ServeHTTP(rec, req)
		if rec.Code != http.StatusNoContent {
			t.Fatalf("source hour finalize attempt=%d status=%d body=%s", attempt, rec.Code, rec.Body.String())
		}
	}
	ackArtifact := func(ack joinedAckRequest) *httptest.ResponseRecorder {
		body, _ := json.Marshal(ack)
		req := httptest.NewRequest(http.MethodPost, "/api/v1/account/joined/ack", bytes.NewReader(body))
		req = req.WithContext(context.WithValue(req.Context(), accountPrincipalContextKey, principal))
		rec := httptest.NewRecorder()
		s.handleAccountJoinedAck(rec, req)
		return rec
	}
	manifestAck := joinedAckRequest{ArtifactID: sealed.HourManifestArtifactID,
		RelativePath: "coverage/hours/" + sealed.HourID + ".json", SizeBytes: sealed.HourManifestExpectedSize,
		SHA256: sealed.HourManifestExpectedSHA}
	if rec := ackArtifact(manifestAck); rec.Code == http.StatusOK {
		t.Fatalf("manifest ACK bypassed ledger status=%d body=%s", rec.Code, rec.Body.String())
	}
	sourceLedgerAck := joinedAckRequest{ArtifactID: ledgers[12].artifactID, RelativePath: ledgers[12].relativePath,
		SizeBytes: int64(len(ledgers[12].bytes)), SHA256: ledgers[12].sha}
	if rec := ackArtifact(sourceLedgerAck); rec.Code != http.StatusOK {
		t.Fatalf("source ledger ACK status=%d body=%s", rec.Code, rec.Body.String())
	}
	if rec := ackArtifact(manifestAck); rec.Code != http.StatusOK {
		t.Fatalf("source manifest ACK status=%d body=%s", rec.Code, rec.Body.String())
	}
	mediaAck := joinedAckRequest{ArtifactID: sealed.MediaArtifactIDs[0], RelativePath: mediaOutput.RelativePath,
		SizeBytes: mediaOutput.ExpectedSize, SHA256: mediaOutput.ExpectedSHA}
	if rec := ackArtifact(mediaAck); rec.Code != http.StatusOK {
		t.Fatalf("source media ACK status=%d body=%s", rec.Code, rec.Body.String())
	}
	if _, err := pool.Exec(ctx, `INSERT INTO recording_joined_freeze_exclusions(batch_record_id,batch_recording_id,
		recording_id,reason_code,evidence_sha256,canonical_evidence) VALUES($1,$2,$3,'later_source',$4,'{"late":true}')`,
		batchRecordID, batchRecordingID, recordingID, strings.Repeat("e", 64)); err == nil {
		t.Fatal("frozen batch accepted a late exclusion")
	}
	if _, err := pool.Exec(ctx, `INSERT INTO recording_joined_artifacts(batch_record_id,account_id,connection_id,batch_id,
		scope_kind,scope_id,artifact_kind,relative_path,object_key,content_type,expected_size_bytes,expected_sha256,
		canonical_bytes,publication_state,finalized_token,etag,version_id,published_at)
		VALUES($1,$2,$3,$4,'batch_index',$4,'batch_index','coverage/batch.json',$5,'application/json',2,$6,'{}',
		'published',$7,'etag','',now())`, batchRecordID, accountID, connectionID, batchID,
		"joined/"+batchID+"/coverage/batch.json", strings.Repeat("d", 64), "00000000-0000-0000-0000-000000000001"); err == nil {
		t.Fatal("artifact was born published")
	}
	var incompleteID int64
	if err := pool.QueryRow(ctx, `INSERT INTO recording_joined_batches(account_id,connection_id,batch_id,generation,
		policy_version,eligibility_cutoff,media_tool,media_tool_sha256,freeze_request,freeze_request_sha256,
		frozen_denominator_sha256,expected_recordings,expected_stream_days,expected_scheduled_hours,
		expected_source_clips,expected_source_bytes) VALUES($1,$2,'incomplete-generation-1',1,$3,$4,$5,$6,
		'{"schema_version":1}',$7,$8,1,14,168,0,0) RETURNING id`, accountID, connectionID,
		joinedrecording.PlanPolicyVersion, qualification.FrozenAt, mediaToolJSON, mediaTool.IdentitySHA256,
		strings.Repeat("e", 64), strings.Repeat("f", 64)).Scan(&incompleteID); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `UPDATE recording_joined_batches SET state='frozen',frozen_at=$2 WHERE id=$1`,
		incompleteID, qualification.FrozenAt); err == nil {
		t.Fatal("incomplete joined batch froze")
	}
	malformedTx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	malformedManifest := []byte(`{"status":"gap_only","source_count":0,"sources":[],"source_dispositions":[],"media":[],"scheduled_gap":{"reason_code":"scheduled_source_gap"}}`)
	command, malformedErr := malformedTx.Exec(ctx, `UPDATE recording_joined_hours SET state='sealed',
		source_only_sha256=source_claim_sha256,canonical_plan='{"expected_output_count":0}',manifest_bytes=$2,
		manifest_sha256=encode(sha256($2),'hex'),sealed_at=now() WHERE stream_day_id=$1 AND delivery_hour=1`,
		ledgers[1].streamDayID, malformedManifest)
	if malformedErr == nil && command.RowsAffected() == 1 {
		_, malformedErr = malformedTx.Exec(ctx, `INSERT INTO recording_joined_artifacts(batch_record_id,account_id,
			connection_id,batch_id,scope_kind,scope_id,stream_day_id,hour_record_id,artifact_kind,ordinal,relative_path,
			object_key,content_type,expected_size_bytes,expected_sha256,canonical_bytes,publication_state)
			SELECT batch_record_id,account_id,connection_id,batch_id,'hour',hour_id,stream_day_id,id,'hour_manifest',1,
			'coverage/hours/'||hour_id||'.json','joined/'||batch_id||'/coverage/hours/'||hour_id||'.json',
			'application/json',$2,encode(sha256($3),'hex'),$3,'sealed' FROM recording_joined_hours
			WHERE stream_day_id=$1 AND delivery_hour=1`, ledgers[1].streamDayID, len(malformedManifest), malformedManifest)
	}
	if malformedErr == nil {
		malformedErr = malformedTx.Commit(ctx)
	} else {
		_ = malformedTx.Rollback(ctx)
	}
	if malformedErr == nil {
		t.Fatal("malformed source-free seal committed")
	}
	var malformedState string
	var malformedArtifacts int
	if err := pool.QueryRow(ctx, `SELECT state,(SELECT count(*) FROM recording_joined_artifacts a
		WHERE a.hour_record_id=h.id) FROM recording_joined_hours h WHERE h.stream_day_id=$1 AND h.delivery_hour=1`,
		ledgers[1].streamDayID).Scan(&malformedState, &malformedArtifacts); err != nil || malformedState != "pending" || malformedArtifacts != 0 {
		t.Fatalf("malformed source-free seal leaked state=%s artifacts=%d err=%v", malformedState, malformedArtifacts, err)
	}
	sealTx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var pendingHourID int64
	if err := sealTx.QueryRow(ctx, `SELECT id FROM recording_joined_hours WHERE stream_day_id=$1 AND delivery_hour=1`,
		ledgers[1].streamDayID).Scan(&pendingHourID); err != nil {
		t.Fatal(err)
	}
	leaseToken := "00000000-0000-0000-0000-000000000002"
	if _, err := sealTx.Exec(ctx, `UPDATE recording_joined_hours SET state='leased',attempt_count=1,claim_token=$2,
		claimed_by='worker-atomicity-test',lease_expires_at=date_trunc('second',now()+interval '5 minutes'),heartbeat_at=now()
		WHERE id=$1`, pendingHourID, leaseToken); err == nil {
		t.Fatal("source-free hour entered worker lease path")
	}
	_ = sealTx.Rollback(ctx)
	var state string
	var strayMedia int
	if err := pool.QueryRow(ctx, `SELECT state,(SELECT count(*) FROM recording_joined_artifacts
		WHERE hour_record_id=h.id AND artifact_kind='media') FROM recording_joined_hours h WHERE id=$1`, pendingHourID).
		Scan(&state, &strayMedia); err != nil || state != "pending" || strayMedia != 0 {
		t.Fatalf("rejected source-free lease state=%s stray_media=%d err=%v", state, strayMedia, err)
	}
	if _, err := pool.Exec(ctx, `UPDATE recording_joined_hours SET state='sealed',source_only_sha256=source_claim_sha256,
		canonical_plan='{}',manifest_bytes='{}',manifest_sha256=encode(sha256('{}'::bytea),'hex'),sealed_at=now()
		WHERE stream_day_id=$1 AND delivery_hour=2`, ledgers[12].streamDayID); err == nil {
		t.Fatal("source-bearing hour bypassed its worker lease")
	}
	partialTx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	partialToken := "00000000-0000-0000-0000-000000000003"
	if _, err := partialTx.Exec(ctx, `UPDATE recording_joined_hours SET state='leased',attempt_count=1,claim_token=$2,
		claimed_by='partial-seal-test',lease_expires_at=now()+interval '5 minutes',heartbeat_at=now()
		WHERE stream_day_id=$1 AND delivery_hour=2`, ledgers[12].streamDayID, partialToken); err != nil {
		t.Fatal(err)
	}
	partialSHA := strings.Repeat("5", 64)
	if _, err := partialTx.Exec(ctx, `INSERT INTO recording_joined_artifacts(batch_record_id,account_id,connection_id,
		batch_id,scope_kind,scope_id,stream_day_id,hour_record_id,artifact_kind,ordinal,relative_path,object_key,
		content_type,content_id,expected_size_bytes,expected_sha256)
		SELECT batch_record_id,account_id,connection_id,batch_id,'hour',hour_id,stream_day_id,id,'media',1,
		'partial.mp4','joined/'||batch_id||'/objects/'||$2||'.mp4','video/mp4',$2,1,$2
		FROM recording_joined_hours WHERE stream_day_id=$1 AND delivery_hour=2`, ledgers[12].streamDayID, partialSHA); err != nil {
		t.Fatal(err)
	}
	if err := partialTx.Commit(ctx); err == nil {
		t.Fatal("partial source-hour seal committed")
	}
	var partialState string
	var partialArtifacts int
	if err := pool.QueryRow(ctx, `SELECT state,(SELECT count(*) FROM recording_joined_artifacts a
		WHERE a.hour_record_id=h.id) FROM recording_joined_hours h WHERE h.stream_day_id=$1 AND h.delivery_hour=2`,
		ledgers[12].streamDayID).Scan(&partialState, &partialArtifacts); err != nil || partialState != "pending" || partialArtifacts != 0 {
		t.Fatalf("partial source-hour seal leaked state=%s artifacts=%d err=%v", partialState, partialArtifacts, err)
	}
}

func joinedCanonicalPassingVerification() joinedrecording.Verification {
	track := &joinedrecording.TrackFingerprint{MediaType: "video", PacketCount: 1,
		PacketChainSHA256: strings.Repeat("a", 64), PacketTimingSHA256: strings.Repeat("b", 64),
		PacketTimeBases: []string{"1/1"}, FirstPacketPTSSeconds: "0", LastPacketPTSSeconds: "0",
		FirstPacketDTSSeconds: "0", LastPacketDTSSeconds: "0", PacketDurationSeconds: "1",
		DecodeTimelineSpanSeconds: "1", DecodedFrames: 1, TimestampStatus: "monotonic"}
	sourceTrack := *track
	sourceTrack.TimestampStatus = "source_clips_independent"
	return joinedrecording.Verification{Status: "passed", PacketPayloadOrderStatus: "passed",
		DecodedFrameTotalsStatus: "passed", DecodedAudioTotalsStatus: "passed", OutputTimestampStatus: "passed",
		StrictDecodeStatus: "passed", SourceFingerprint: joinedrecording.MediaFingerprint{DurationSeconds: 60,
			Tracks: map[string]*joinedrecording.TrackFingerprint{"video": &sourceTrack}},
		OutputFingerprint: joinedrecording.MediaFingerprint{DurationSeconds: 60,
			Tracks: map[string]*joinedrecording.TrackFingerprint{"video": track}}}
}
