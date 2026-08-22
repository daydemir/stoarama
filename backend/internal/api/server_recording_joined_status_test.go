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

	"github.com/google/uuid"

	"github.com/daydemir/stoarama/backend/internal/joinedauth"
	"github.com/daydemir/stoarama/backend/internal/joinedrecording"
)

func TestJoinedAdminBatchStatusIsBoundedOrderedReadOnlyAndAdminOnly(t *testing.T) {
	fixture := newJoinedHistoricalTier1Fixture(t, "joined-status@example.test")
	defer fixture.cleanup()
	ctx := context.Background()
	path := "/api/v1/recording/joined/batches/status?batch_id=" + fixture.req.BatchID
	router := fixture.s.router()
	call := func(cookie, token, requestPath string) *httptest.ResponseRecorder {
		request := httptest.NewRequest(http.MethodGet, requestPath, nil)
		if cookie != "" {
			request.AddCookie(&http.Cookie{Name: accountSessionCookie, Value: cookie})
		}
		if token != "" {
			request.Header.Set("Authorization", "Bearer "+token)
		}
		response := httptest.NewRecorder()
		router.ServeHTTP(response, request)
		return response
	}

	if response := call(fixture.sessionToken, "", path); response.Code != http.StatusNotFound {
		t.Fatalf("pre-apply status=%d body=%s", response.Code, response.Body.String())
	}
	req := fixture.req
	req.Apply, req.ExpectedRequestSHA256 = true, fixture.plan.RequestSHA256
	if response, _ := fixture.call(req); response.Code != http.StatusOK {
		t.Fatalf("apply status=%d body=%s", response.Code, response.Body.String())
	}
	fixture.s.cfg.ServiceToken = "generic-service-credential-32-bytes"

	claim := mintJoinedClaimForTest(t, fixture.s, req.BatchID)
	operation, err := joinedauth.MintOperation(fixture.s.cfg.JoinedWorkerSigningKey, req.BatchID,
		joinedauth.SubjectHour, "foreign-hour", uuid.New(), joinedauth.OperationPreflight, time.Now().Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	for name, token := range map[string]string{
		"missing":          "",
		"generic service":  fixture.s.cfg.ServiceToken,
		"joined bootstrap": fixture.s.cfg.JoinedWorkerBootstrapToken,
		"joined claim":     claim,
		"joined operation": operation,
	} {
		t.Run("rejects "+name, func(t *testing.T) {
			if response := call("", token, path); response.Code != http.StatusUnauthorized {
				t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
			}
		})
	}
	memberUserID, memberAccountID := seedUserOrg(t, fixture.pool, "joined-status-member@example.test", false)
	memberToken := "joined-status-member-session"
	insertSession(t, fixture.pool, memberAccountID, memberUserID, memberToken)
	if response := call(memberToken, "", path); response.Code != http.StatusForbidden {
		t.Fatalf("member status=%d body=%s", response.Code, response.Body.String())
	}
	for _, badPath := range []string{
		"/api/v1/recording/joined/batches/status",
		"/api/v1/recording/joined/batches/status?batch_id=bad_id",
		path + "&batch_id=" + req.BatchID,
		path + "&extra=true",
	} {
		if response := call(fixture.sessionToken, "", badPath); response.Code != http.StatusBadRequest {
			t.Fatalf("bad path %q status=%d body=%s", badPath, response.Code, response.Body.String())
		}
	}
	fixture.s.cfg.JoinedRecordingControlPlaneEnabled = false
	if response := call(fixture.sessionToken, "", path); response.Code != http.StatusServiceUnavailable {
		t.Fatalf("disabled status=%d body=%s", response.Code, response.Body.String())
	}
	fixture.s.cfg.JoinedRecordingControlPlaneEnabled = true

	if _, err := fixture.pool.Exec(ctx, `UPDATE accounts SET role='admin' WHERE id=$1`, fixture.accountID); err != nil {
		t.Fatal(err)
	}
	_, _, adminAPIKey, err := mintAccountAPIKey(ctx, fixture.pool, fixture.accountID, "joined-status", accountScopeRead, nil)
	if err != nil {
		t.Fatal(err)
	}
	status := call("", adminAPIKey, path)
	var before joinedAdminBatchStatusResponse
	if status.Code != http.StatusOK || json.Unmarshal(status.Body.Bytes(), &before) != nil {
		t.Fatalf("admin API-key status=%d body=%s", status.Code, status.Body.String())
	}
	assertJoinedAdminBatchStatus(t, before, req.BatchID, fixture.plan.FrozenDenominatorSHA256, "pending", "")
	if before.FreezeStartedAt != nil || before.FrozenAt != nil {
		t.Fatalf("building batch has freeze times: started=%v frozen=%v", before.FreezeStartedAt, before.FrozenAt)
	}

	// A raw clip that appears after the immutable batch snapshot must not affect
	// this endpoint's stored source counts.
	lateStart := fixture.clipStart.Add(time.Hour)
	if _, err := fixture.pool.Exec(ctx, `INSERT INTO recording_clips(recording_id,recording_job_id,storage_destination_id,
		endpoint,bucket,object_key,display_path,mime_type,container,size_bytes,etag,sha256,duration_ms,video_codec,
		audio_present,fire_at,clip_start_at,clip_end_at,created_at,released_at) VALUES($1,$2,$3,$4,'clips','raw/late.mp4',
		'raw/late.mp4','video/mp4','mp4',20,'etag-late',$5,60000,'h264',false,$6,$6,$7,$6,$7)`,
		joinedrecording.Tier1RecordingIDs[0], fixture.firstJobID, fixture.storageID, joinedTestSourceEndpoint,
		strings.Repeat("b", 64), lateStart, lateStart.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	afterRaw := call(fixture.sessionToken, "", path)
	var unchanged joinedAdminBatchStatusResponse
	if afterRaw.Code != http.StatusOK || json.Unmarshal(afterRaw.Body.Bytes(), &unchanged) != nil ||
		unchanged.StreamDays[0].SourceCount != 1 || unchanged.StreamDays[0].SourceBytes != 10 {
		t.Fatalf("live raw clip changed status=%d body=%s", afterRaw.Code, afterRaw.Body.String())
	}

	fixture.s.cfg.R2Endpoint = joinedTestSourceEndpoint
	fixture.s.cfg.R2Region = "auto"
	fixture.s.cfg.R2Bucket = "clips"
	fixture.s.joinedFreezeSourceStore = &joinedFreezeStoreStub{bucket: "clips"}
	fixture.s.joinedFreezeTransport = &joinedFreezeTransportStub{etag: "etag-one"}
	sealRequest := joinedSealStreamDayRequest{ProtocolVersion: joinedrecording.JoinedProtocolVersion,
		BatchID: req.BatchID, RecordingID: joinedrecording.Tier1RecordingIDs[0], LocalDate: "2026-08-01"}
	body, _ := json.Marshal(sealRequest)
	httpRequest := httptest.NewRequest(http.MethodPost, "/api/v1/recording/joined/stream-days/seal", bytes.NewReader(body))
	httpRequest.AddCookie(&http.Cookie{Name: accountSessionCookie, Value: fixture.sessionToken})
	sealed := httptest.NewRecorder()
	router.ServeHTTP(sealed, httpRequest)
	var receipt joinedSealStreamDayResponse
	if sealed.Code != http.StatusOK || json.Unmarshal(sealed.Body.Bytes(), &receipt) != nil || !lowerHexSHA256(receipt.SealRequestSHA) {
		t.Fatalf("seal status=%d body=%s", sealed.Code, sealed.Body.String())
	}

	afterSeal := call("", adminAPIKey, path)
	var got joinedAdminBatchStatusResponse
	if afterSeal.Code != http.StatusOK || json.Unmarshal(afterSeal.Body.Bytes(), &got) != nil {
		t.Fatalf("post-seal status=%d body=%s", afterSeal.Code, afterSeal.Body.String())
	}
	assertJoinedAdminBatchStatus(t, got, req.BatchID, fixture.plan.FrozenDenominatorSHA256, "sealed", receipt.SealRequestSHA)
	for i := 1; i < len(got.StreamDays); i++ {
		if got.StreamDays[i].State != "pending" || got.StreamDays[i].SealRequestSHA256 != "" {
			t.Fatalf("day %d changed: %+v", i, got.StreamDays[i])
		}
	}
}

func assertJoinedAdminBatchStatus(t *testing.T, got joinedAdminBatchStatusResponse, batchID, denominator, firstState, firstSealSHA string) {
	t.Helper()
	if got.ProtocolVersion != joinedrecording.JoinedProtocolVersion || got.BatchID != batchID || got.State != "building" ||
		got.FrozenDenominatorSHA256 != denominator || got.ExpectedStreamDays != 462 ||
		got.ExpectedScheduledHours != 5544 || len(got.StreamDays) != 462 {
		t.Fatalf("status header=%+v stream_days=%d", got, len(got.StreamDays))
	}
	for i, day := range got.StreamDays {
		wantRecording := joinedrecording.Tier1RecordingIDs[i/14]
		wantDate := fmt.Sprintf("2026-08-%02d", i%14+1)
		if day.RecordingID != wantRecording || day.LocalDate != wantDate {
			t.Fatalf("day %d=%+v want recording=%d date=%s", i, day, wantRecording, wantDate)
		}
	}
	first := got.StreamDays[0]
	if first.State != firstState || first.SourceCount != 1 || first.SourceBytes != 10 || first.SealRequestSHA256 != firstSealSHA {
		t.Fatalf("first day=%+v", first)
	}
}
