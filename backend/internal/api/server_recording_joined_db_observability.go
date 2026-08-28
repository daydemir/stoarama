package api

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"log"
	"net/http"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/daydemir/stoarama/backend/internal/util"
)

func joinedDBErrorCode(err error) (string, string) {
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) || len(pgErr.Code) != 5 {
		return "none", "none"
	}
	return pgErr.Code, pgErr.Code[:2]
}

func joinedDBErrorLogLine(operation, stage, batchID, workerID, subjectKind string, subjectID int64, err error) string {
	sqlstate, class := joinedDBErrorCode(err)
	workerIDHash := sha256.Sum256([]byte(workerID))
	return fmt.Sprintf("joined_db_error operation=%q stage=%q sqlstate=%q sqlstate_class=%q batch_id=%q worker_id_sha256=%x subject_kind=%q subject_id=%d",
		operation, stage, sqlstate, class, batchID, workerIDHash, subjectKind, subjectID)
}

func writeJoinedDBError(w http.ResponseWriter, status int, publicMessage, operation, stage, batchID, workerID string,
	subjectKind string, subjectID int64, err error) {
	log.Print(joinedDBErrorLogLine(operation, stage, batchID, workerID, subjectKind, subjectID, err))
	util.WriteError(w, status, publicMessage)
}
