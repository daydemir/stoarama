package api

import (
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

func joinedDBErrorLogLine(operation, stage, batchID, workerID string, artifactID int64, err error) string {
	sqlstate, class := joinedDBErrorCode(err)
	return fmt.Sprintf("joined_db_error operation=%q stage=%q sqlstate=%q sqlstate_class=%q batch_id=%q worker_id=%q artifact_id=%d",
		operation, stage, sqlstate, class, batchID, workerID, artifactID)
}

func writeJoinedDBError(w http.ResponseWriter, status int, publicMessage, operation, stage, batchID, workerID string,
	artifactID int64, err error) {
	log.Print(joinedDBErrorLogLine(operation, stage, batchID, workerID, artifactID, err))
	util.WriteError(w, status, publicMessage)
}
