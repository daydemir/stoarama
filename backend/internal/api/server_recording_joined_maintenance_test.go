package api

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/daydemir/stoarama/backend/internal/config"
)

func TestExpiredAttemptMaintenanceCadenceSkipsWithoutDatabaseWork(t *testing.T) {
	const batch = "batch-1"
	hour := batch + "__recording-1__date-2026-08-01__hour-01__generation-1"
	s := &Server{cfg: config.Config{JoinedRecordingControlPlaneEnabled: true, JoinedRecordingProtocolVersion: 1,
		JoinedRecordingBatchID: batch, JoinedRecordingWorkScope: config.JoinedWorkScopeSingleCanary,
		JoinedRecordingCanaryHourIDs: hour, JoinedWorkerBootstrapToken: strings.Repeat("b", 32),
		JoinedWorkerSigningKey: strings.Repeat("s", 32)}, joinedAttemptReconcileAt: time.Now().UTC()}
	req := httptest.NewRequest(http.MethodPost, "/api/v1/recording/joined/maintenance/reconcile-expired",
		strings.NewReader(`{"batch_id":"batch-1"}`))
	rec := httptest.NewRecorder()
	s.handleJoinedReconcileExpiredAttempts(rec, req)
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `"skipped":true`) {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
	}
}
