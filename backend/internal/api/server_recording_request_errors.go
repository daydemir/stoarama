package api

import (
	"context"
	"errors"
	"net"
	"net/http"

	"github.com/daydemir/stoarama/backend/internal/util"
)

// A body read deadline is a transport failure, not invalid clip metadata.
// Existing recorder clients retry 408 while preserving the segment and intent.
// Keep malformed JSON/unknown fields at 400 so permanent mistakes fail closed.
func writeRecordingRequestDecodeError(w http.ResponseWriter, err error) {
	var networkErr net.Error
	if errors.Is(err, context.DeadlineExceeded) || (errors.As(err, &networkErr) && networkErr.Timeout()) {
		util.WriteError(w, http.StatusRequestTimeout, "recording request body read timed out; retry the same request")
		return
	}
	util.WriteError(w, http.StatusBadRequest, err.Error())
}
