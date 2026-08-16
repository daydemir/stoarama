package dropletpool

import (
	"context"
	"strings"
	"testing"
	"time"
)

func validDedicatedSpec() DedicatedCanarySpec {
	return DedicatedCanarySpec{
		RecordingID: 335, Owner: "stoarama-canary-test", TTL: 8 * time.Hour,
		OperatorAccountID: 7, Region: "nyc1", Size: "s-2vcpu-4gb", Image: "123",
		ProjectID: "project", FirewallID: "firewall", BackendAPIURL: "https://api.example.test",
	}
}

func TestValidateDedicatedCanarySpecRequiresDisposableInputs(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*DedicatedCanarySpec)
		want   string
	}{
		{"recording", func(s *DedicatedCanarySpec) { s.RecordingID = 0 }, "recording id"},
		{"owner", func(s *DedicatedCanarySpec) { s.Owner = "" }, "owner"},
		{"ttl", func(s *DedicatedCanarySpec) { s.TTL = time.Minute }, "ttl"},
		{"region", func(s *DedicatedCanarySpec) { s.Region = "" }, "region"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			spec := validDedicatedSpec()
			tc.mutate(&spec)
			err := validateDedicatedCanarySpec(spec)
			if err == nil || !strings.Contains(strings.ToLower(err.Error()), tc.want) {
				t.Fatalf("error=%v want substring %q", err, tc.want)
			}
		})
	}
}

func TestValidateDedicatedCanarySpecAcceptsCapacityIsolatedRequest(t *testing.T) {
	if err := validateDedicatedCanarySpec(validDedicatedSpec()); err != nil {
		t.Fatal(err)
	}
}

func TestDedicatedCanaryReservationIsExclusiveAndDisposable(t *testing.T) {
	pool, cleanup := testDropletPoolDB(t)
	defer cleanup()
	ctx := context.Background()
	accountID := insertForecastAccount(t, pool)
	destID := insertForecastDestination(t, pool, accountID)
	var recordingID int64
	if err := pool.QueryRow(ctx, `
		INSERT INTO recordings (
		  account_id, storage_destination_id, name, stream_url, source_kind,
		  mode, cron_expr, cron_timezone, clip_duration_sec, status, next_fire_at,
		  start_at, capture_via
		) VALUES ($1,$2,'stoarama-canary-335-5m','https://example.test/live.m3u8',
		          'hls_live','sampled','* * * * *','UTC',300,'active',now(),now()-interval '1 hour','cloud')
		RETURNING id
	`, accountID, destID).Scan(&recordingID); err != nil {
		t.Fatal(err)
	}
	store := NewStore(pool)
	reservation, err := store.CreateDedicatedCanaryReservation(ctx, recordingID, "test-owner", time.Hour)
	if err != nil {
		t.Fatalf("create reservation: %v", err)
	}
	if reservation.WorkerName == "" || reservation.State != "active" {
		t.Fatalf("unexpected reservation: %+v", reservation)
	}
	if _, err := store.CreateDedicatedCanaryReservation(ctx, recordingID, "other-owner", time.Hour); err == nil {
		t.Fatal("second active reservation was accepted")
	}
	if _, err := store.CreateDedicatedCanaryReservation(ctx, recordingID+1, "test-owner", time.Hour); err == nil {
		t.Fatal("missing recording was accepted")
	}
}

func TestDedicatedCanaryReservationAdmissionRules(t *testing.T) {
	tests := []struct {
		name       string
		captureVia string
		status     string
		recording  string
		work       bool
		want       string
	}{
		{"non-cloud", "relay", "active", "stoarama-canary-relay", false, "cloud recording"},
		{"inactive", "cloud", "completed", "stoarama-canary-inactive", false, "not active"},
		{"not explicitly disposable", "cloud", "active", "ordinary-recording", false, "explicitly disposable"},
		{"pending work", "cloud", "active", "stoarama-canary-pending", true, "pending or leased work"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			pool, cleanup := testDropletPoolDB(t)
			defer cleanup()
			ctx := context.Background()
			accountID := insertForecastAccount(t, pool)
			destID := insertForecastDestination(t, pool, accountID)
			var recordingID int64
			if err := pool.QueryRow(ctx, `
				INSERT INTO recordings (
				  account_id, storage_destination_id, name, stream_url, source_kind,
				  mode, cron_expr, cron_timezone, clip_duration_sec, status, next_fire_at,
				  start_at, capture_via
				) VALUES ($1,$2,$3,'https://example.test/live.m3u8','hls_live',
				          'sampled','* * * * *','UTC',300,$4,now(),now()-interval '1 hour',$5)
				RETURNING id
			`, accountID, destID, tc.recording, tc.status, tc.captureVia).Scan(&recordingID); err != nil {
				t.Fatal(err)
			}
			if tc.work {
				if _, err := pool.Exec(ctx, `
					INSERT INTO recording_jobs(recording_id,fire_at,scheduled_for,clip_duration_sec,status,idempotency_key)
					VALUES($1,now(),now(),300,'pending',$2)
				`, recordingID, tc.name); err != nil {
					t.Fatal(err)
				}
			}
			_, err := NewStore(pool).CreateDedicatedCanaryReservation(ctx, recordingID, "test-owner", time.Hour)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error=%v want substring %q", err, tc.want)
			}
		})
	}
}

func TestReleaseDedicatedCanaryFencesAndDrainsExactWorker(t *testing.T) {
	pool, cleanup := testDropletPoolDB(t)
	defer cleanup()
	ctx := context.Background()
	accountID := insertForecastAccount(t, pool)
	destID := insertForecastDestination(t, pool, accountID)
	var recordingID int64
	if err := pool.QueryRow(ctx, `
		INSERT INTO recordings (
		  account_id, storage_destination_id, name, stream_url, source_kind,
		  mode, cron_expr, cron_timezone, clip_duration_sec, status, next_fire_at,
		  start_at, capture_via
		) VALUES ($1,$2,'stoarama-canary-release','https://example.test/live.m3u8',
		          'hls_live','sampled','* * * * *','UTC',300,'active',now(),now()-interval '1 hour','cloud')
		RETURNING id
	`, accountID, destID).Scan(&recordingID); err != nil {
		t.Fatal(err)
	}
	store := NewStore(pool)
	reservation, err := store.CreateDedicatedCanaryReservation(ctx, recordingID, "test-owner", time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO recorder_droplets (name, region, size, capacity, state, pool_role)
		VALUES ($1,'nyc1','s-2vcpu-4gb',1,'active','dedicated_canary')
	`, reservation.WorkerName); err != nil {
		t.Fatal(err)
	}
	if _, err := store.ReleaseDedicatedCanary(ctx, reservation.ID, "wrong-owner"); err == nil || !strings.Contains(err.Error(), "owner fence") {
		t.Fatalf("wrong owner release error=%v, want owner fence", err)
	}
	if _, err := store.ReleaseDedicatedCanary(ctx, reservation.ID, "test-owner"); err != nil {
		t.Fatal(err)
	}
	var state, reservationState string
	if err := pool.QueryRow(ctx, `SELECT state FROM recorder_droplets WHERE name=$1`, reservation.WorkerName).Scan(&state); err != nil {
		t.Fatal(err)
	}
	if state != "draining" {
		t.Fatalf("worker state=%q want draining", state)
	}
	if err := pool.QueryRow(ctx, `SELECT state FROM recording_dedicated_canary_reservations WHERE id=$1`, reservation.ID).Scan(&reservationState); err != nil {
		t.Fatal(err)
	}
	if reservationState != "released" {
		t.Fatalf("reservation state=%q want released", reservationState)
	}
}
