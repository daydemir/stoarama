package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/daydemir/stoarama/backend/internal/config"
	"github.com/daydemir/stoarama/backend/internal/r2"
	"github.com/jackc/pgx/v5"
)

func TestR2StitchStorageVerificationRefusesBeforeDependencies(t *testing.T) {
	s := &Server{} // Any SQL, encryption or S3 work would fail this test.
	for _, tc := range []struct {
		name, body string
		handler    http.HandlerFunc
	}{
		{"account", `{"provider":"s3_compatible"}`, s.handleAccountStorageDestinationsCreate},
		{"account-r2", `{"provider":"r2"}`, s.handleAccountStorageDestinationsCreate},
		{"account-default", `{}`, s.handleAccountStorageDestinationsCreate},
		{"admin", `{"provider":"s3_compatible"}`, s.handleAdminStorageDestinationCreate},
		{"admin-default", `{}`, s.handleAdminStorageDestinationCreate},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, "/storage", strings.NewReader(tc.body))
			req = req.WithContext(context.WithValue(req.Context(), accountPrincipalContextKey, accountPrincipal{AccountID: 47}))
			rr := httptest.NewRecorder()
			tc.handler(rr, req)
			if rr.Code != http.StatusConflict {
				t.Fatalf("status=%d body=%s", rr.Code, rr.Body.String())
			}
		})
	}
	if err := verifyStorageDestination(context.Background(), r2.Config{}, ""); err != r2.ErrLegacyDeleteRetired {
		t.Fatal(err)
	}
}

func TestR2StitchRetiredNASHandlersPreserveDurableRows(t *testing.T) {
	pool, cleanup := testAccountClipsPool(t)
	defer cleanup()
	ctx := context.Background()
	for _, file := range []string{"0154_nas_upload_probes.sql", "0157_nas_restore_requests.sql"} {
		raw, err := os.ReadFile("../../../infra/sql/migrations/" + file)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := pool.Exec(ctx, string(raw)); err != nil {
			t.Fatal(err)
		}
	}
	var connectionID, recordingID, clipID int64
	if err := pool.QueryRow(ctx, `INSERT INTO connections(account_id,kind,api_key_id) VALUES(47,'nas_pull',123) RETURNING id`).Scan(&connectionID); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `INSERT INTO recordings(account_id,name,delivery) VALUES(47,'retirement test','nas_pull') RETURNING id`).Scan(&recordingID); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `INSERT INTO recording_clips(recording_id,size_bytes,sha256,object_key,display_path,clip_start_at,clip_end_at,purged_at) VALUES($1,10,repeat('a',64),'managed/test.mp4','test.mp4',now()-interval '1 day',now()-interval '1 day'+interval '1 minute',now()) RETURNING id`, recordingID).Scan(&clipID); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO nas_restore_requests(connection_id,clip_id,recording_id,target,object_key,source_object_key,relative_path,content_type,size_bytes,sha256,label) VALUES($1,$2,$3,'original','managed/test.mp4','managed/test.mp4','test.mp4','video/mp4',10,repeat('a',64),'retained')`, connectionID, clipID, recordingID); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO nas_upload_probes(connection_id,object_prefix,size_bytes,streams,expires_at) VALUES($1,'nas-probes/retained',10,1,now()-interval '1 day')`, connectionID); err != nil {
		t.Fatal(err)
	}
	snapshot := func() string {
		t.Helper()
		var raw string
		if err := pool.QueryRow(ctx, `SELECT jsonb_build_array((SELECT jsonb_agg(to_jsonb(p) ORDER BY id) FROM nas_upload_probes p),(SELECT jsonb_agg(to_jsonb(r) ORDER BY id) FROM nas_restore_requests r),(SELECT jsonb_agg(to_jsonb(c) ORDER BY id) FROM recording_clips c))::text`).Scan(&raw); err != nil {
			t.Fatal(err)
		}
		return raw
	}
	before := snapshot()
	s := &Server{pool: pool, r2: new(r2.Client), cfg: config.Config{NASUploadProbeEnabled: true}}
	for _, tc := range []struct {
		name    string
		handler http.HandlerFunc
		status  int
	}{
		{"probe request", s.handleAccountConnectionUploadProbe, http.StatusOK},
		{"probe report", s.handleAccountConnectionUploadProbeResult, http.StatusConflict},
		{"restore lease", s.handleAccountConnectionRestoreLease, http.StatusOK},
		{"restore report", s.handleAccountConnectionRestoreResult, http.StatusConflict},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, key := range []*int64{nil, ptrInt64(999), ptrInt64(123)} {
				req := httptest.NewRequest(http.MethodPost, "/retired", strings.NewReader(`{}`))
				req = req.WithContext(context.WithValue(req.Context(), accountPrincipalContextKey, accountPrincipal{AccountID: 47, APIKeyID: key}))
				rr := httptest.NewRecorder()
				tc.handler(rr, req)
				want := tc.status
				if key == nil || *key != 123 {
					want = http.StatusForbidden
				}
				if rr.Code != want {
					t.Fatalf("status=%d want=%d body=%s", rr.Code, want, rr.Body.String())
				}
				if key != nil && *key == 123 && tc.name == "probe request" {
					var response nasUploadProbeResponse
					if err := json.Unmarshal(rr.Body.Bytes(), &response); err != nil || response.Enabled || response.Due || response.RetryAfterSec != 3600 {
						t.Fatalf("response=%s err=%v", rr.Body, err)
					}
				}
				if key != nil && *key == 123 && tc.name == "restore lease" {
					var response nasRestoreLeaseResponse
					if err := json.Unmarshal(rr.Body.Bytes(), &response); err != nil || response.Tasks == nil || len(response.Tasks) != 0 || response.RetryAfterSec != 120 {
						t.Fatalf("response=%s err=%v", rr.Body, err)
					}
				}
			}
		})
	}
	if after := snapshot(); after != before {
		t.Fatal("retired handlers changed durable probe, restore or clip state")
	}
}

type managedReadRow struct{ scan func(...any) error }

func (r managedReadRow) Scan(dst ...any) error { return r.scan(dst...) }

type managedReadStore struct {
	t       *testing.T
	missing bool
	calls   int
}

func (m *managedReadStore) QueryRow(ctx context.Context, query string, args ...any) pgx.Row {
	m.t.Helper()
	m.calls++
	if !strings.HasPrefix(query, "SELECT id,key_prefix FROM storage_destinations") || !strings.Contains(query, "account_id=$1 AND managed AND status='verified'") || len(args) != 1 || args[0] != int64(47) {
		m.t.Fatal("managed lookup changed scope or mutated storage", query, args)
	}
	return managedReadRow{func(dst ...any) error {
		if m.missing {
			return pgx.ErrNoRows
		}
		*dst[0].(*int64) = 91
		*dst[1].(*string) = "stored/existing/path"
		return nil
	}}
}

func TestR2StitchManagedResolutionIsExistingReadOnly(t *testing.T) {
	s := &Server{} // No cipher, object client or billing provider is available.
	store := &managedReadStore{t: t}
	id, path, err := s.resolveManagedDestination(context.Background(), store, 47)
	if err != nil || id != 91 || path != "stored/existing/path" || store.calls != 1 {
		t.Fatal(id, path, err)
	}
	store.missing = true
	if _, _, err := s.resolveManagedDestination(context.Background(), store, 47); err != errManagedUnavailable {
		t.Fatal("missing storage was implicitly provisioned", err)
	}
}
