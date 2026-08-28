package api

import (
	"bytes"
	"errors"
	"log"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
)

func TestJoinedDBErrorObservabilityIsSanitizedAndFailClosed(t *testing.T) {
	secretFragments := []string{
		"postgres://secret-user:secret-password@database.example/private",
		"joined/private/object-key.mp4",
		"UPDATE recording_joined_artifacts SET publication_state",
		"secret constraint detail",
	}
	err := wrappedPGErrorForTest(&pgconn.PgError{
		Code:           "40001",
		Message:        secretFragments[0],
		Detail:         secretFragments[3],
		Where:          secretFragments[1],
		InternalQuery:  secretFragments[2],
		TableName:      "recording_joined_artifacts",
		ConstraintName: "secret_constraint",
	})

	var logs bytes.Buffer
	oldOutput, oldFlags, oldPrefix := log.Writer(), log.Flags(), log.Prefix()
	log.SetOutput(&logs)
	log.SetFlags(0)
	log.SetPrefix("")
	t.Cleanup(func() {
		log.SetOutput(oldOutput)
		log.SetFlags(oldFlags)
		log.SetPrefix(oldPrefix)
	})

	recorder := httptest.NewRecorder()
	const workerID = "postgres://worker:secret@database.example/private"
	writeJoinedDBError(recorder, http.StatusConflict, "claim joined publication", "publication_claim", "lease_update",
		"tier1-2026-08", workerID, "artifact", 471, err)

	const wantLog = "joined_db_error operation=\"publication_claim\" stage=\"lease_update\" sqlstate=\"40001\" " +
		"sqlstate_class=\"40\" batch_id=\"tier1-2026-08\" " +
		"worker_id_sha256=95483bbce8a471fcf63557429398a8f9dfe87cb81552339d011934df0e122a17 " +
		"subject_kind=\"artifact\" subject_id=471\n"
	if got := logs.String(); got != wantLog {
		t.Fatalf("sanitized log=%q want=%q", got, wantLog)
	}
	for _, secret := range secretFragments {
		if strings.Contains(logs.String(), secret) || strings.Contains(recorder.Body.String(), secret) {
			t.Fatalf("database detail escaped redaction: %q", secret)
		}
	}
	if strings.Contains(logs.String(), workerID) {
		t.Fatalf("worker correlation value escaped hashing: %q", logs.String())
	}
	if strings.Contains(logs.String(), "artifact_id=") || strings.Contains(logs.String(), "hour_record_id=") {
		t.Fatalf("subject identifier was mislabeled: %q", logs.String())
	}
	if recorder.Code != http.StatusConflict || recorder.Body.String() != "{\"error\":\"claim joined publication\"}\n" {
		t.Fatalf("fail-closed response status=%d body=%q", recorder.Code, recorder.Body.String())
	}
}

func TestJoinedDBErrorObservabilityDoesNotRenderNonPostgresErrors(t *testing.T) {
	err := errors.New("postgres://secret-user:secret-password@database.example/private")
	line := joinedDBErrorLogLine("hour_claim", "consume_one_shot", "tier1-2026-08", "worker-safe", "hour", 27, err)
	if strings.Contains(line, err.Error()) {
		t.Fatalf("non-PostgreSQL error rendered in log: %q", line)
	}
	if !strings.Contains(line, `sqlstate="none" sqlstate_class="none"`) {
		t.Fatalf("non-PostgreSQL classification missing: %q", line)
	}
	if !strings.Contains(line, `subject_kind="hour" subject_id=27`) || strings.Contains(line, "artifact_id=") {
		t.Fatalf("hour subject identifier mislabeled: %q", line)
	}
}

func wrappedPGErrorForTest(err error) error {
	return errors.Join(errors.New("joined publication conflict"), err)
}
