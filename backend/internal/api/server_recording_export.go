package api

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/daydemir/stoarama/backend/internal/r2"
	"github.com/daydemir/stoarama/backend/internal/recordingnaming"
	"github.com/daydemir/stoarama/backend/internal/util"
)

// clipDestination is the decrypted location + credentials snapshot for one
// clip, joined from recording_clips -> storage_destinations under the account
// scope. It mirrors the decrypt->client->op pattern in handleRecordingUploadIntent.
type clipDestination struct {
	objectKey          string
	thumbnailObjectKey string
	sizeBytes          int64
	region             string
	bucket             string
	endpoint           string
	accessKeyID        string
	secretEnc          []byte
}

// buildClipClient decrypts the destination secret and builds an r2 client for a
// clip's bucket, mirroring handleRecordingUploadIntent.
func (s *Server) buildClipClient(r *http.Request, d clipDestination) (*r2.Client, error) {
	return s.buildClipClientCtx(r.Context(), d)
}

// handleAccountRecordingClipDownload presigns a GET against the clip's bucket so
// a browser-session researcher can download one clip. Ownership + location are
// resolved in a single SELECT scoped to the account; a purged clip is 410 Gone.
func (s *Server) handleAccountRecordingClipDownload(w http.ResponseWriter, r *http.Request) {
	principal, ok := accountPrincipalFromContext(r.Context())
	if !ok {
		util.WriteError(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	if s.secrets == nil {
		util.WriteError(w, http.StatusServiceUnavailable, "storage credential key is unset")
		return
	}
	recordingID, ok := parseInt64Path(w, r, "id")
	if !ok {
		return
	}
	clipID, ok := parseInt64Path(w, r, "clipId")
	if !ok {
		return
	}

	var (
		d           clipDestination
		purgedAt    *time.Time
		releasedAt  *time.Time
		displayPath string
		recordingNm string
		clipStartAt time.Time
		namingProf  string
		folderName  string
	)
	err := s.pool.QueryRow(r.Context(), `
		SELECT c.object_key, c.thumbnail_object_key, c.size_bytes, c.purged_at, c.released_at,
		       sd.region, sd.bucket, sd.endpoint, sd.access_key_id, sd.secret_access_key_enc,
		       c.display_path, r.name, c.clip_start_at, r.naming_profile, r.folder_name
		FROM recording_clips c
		JOIN recordings r ON r.id = c.recording_id
		JOIN storage_destinations sd ON sd.id = c.storage_destination_id
		WHERE c.id=$1 AND c.recording_id=$2 AND r.account_id=$3
	`, clipID, recordingID, principal.AccountID).Scan(
		&d.objectKey, &d.thumbnailObjectKey, &d.sizeBytes, &purgedAt, &releasedAt,
		&d.region, &d.bucket, &d.endpoint, &d.accessKeyID, &d.secretEnc,
		&displayPath, &recordingNm, &clipStartAt, &namingProf, &folderName,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		util.WriteError(w, http.StatusNotFound, "clip not found")
		return
	}
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("load clip: %v", err))
		return
	}
	// A released clip is off the org's books (a future feature serves the retained
	// object via a different path); a purged clip's object is gone. Both are 410
	// for the org's download endpoint.
	if purgedAt != nil || releasedAt != nil {
		util.WriteError(w, http.StatusGone, "clip is no longer available")
		return
	}

	client, err := s.buildClipClient(r, d)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("build destination client: %v", err))
		return
	}

	// disposition=attachment (default) forces a real file download with a sensible
	// name; disposition=inline presigns plain so the browser plays the clip in a
	// new tab.
	var url string
	if strings.TrimSpace(r.URL.Query().Get("disposition")) == "inline" {
		url, err = client.PresignGet(r.Context(), d.objectKey, s.cfg.R2SignGetTTL)
	} else {
		filename := clipDownloadFilename(recordingNm, recordingID, clipStartAt, d.objectKey, displayPath, namingProf, folderName)
		if filename == "" {
			util.WriteError(w, http.StatusInternalServerError, "clip download filename is empty")
			return
		}
		url, err = client.PresignGetDownload(r.Context(), d.objectKey, filename, s.cfg.R2SignGetTTL)
	}
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("presign download: %v", err))
		return
	}
	util.WriteJSON(w, http.StatusOK, map[string]any{
		"url":            url,
		"object_key":     d.objectKey,
		"size_bytes":     d.sizeBytes,
		"expires_in_sec": int(s.cfg.R2SignGetTTL.Seconds()),
	})
}

func clipDownloadFilename(recordingName string, recordingID int64, clipStartAt time.Time, objectKey, displayPath, namingProfile, folderName string) string {
	if namingProfile == recordingnaming.ProfileStoaramaV1.String() && strings.TrimSpace(folderName) == "recordings" {
		return buildClipDownloadFilename(recordingName, recordingID, clipStartAt, objectKey)
	}
	return displayPathFilename(displayPath)
}

func displayPathFilename(displayPath string) string {
	displayPath = strings.Trim(strings.TrimSpace(displayPath), "/")
	if displayPath == "" {
		return ""
	}
	if i := strings.LastIndex(displayPath, "/"); i >= 0 {
		return displayPath[i+1:]
	}
	return displayPath
}

// buildClipDownloadFilename derives a stable, safe save-as name for a clip:
// "<slug(recording-name)|recording-<id>>-<UTC ts>.<ext>". The extension is taken
// from the object key (default .mp4). Used for the attachment Content-Disposition.
func buildClipDownloadFilename(recordingName string, recordingID int64, clipStartAt time.Time, objectKey string) string {
	slug := slugifyName(recordingName)
	if slug == "" {
		slug = fmt.Sprintf("recording-%d", recordingID)
	}
	ext := ".mp4"
	if i := strings.LastIndex(objectKey, "."); i >= 0 {
		if e := objectKey[i:]; len(e) <= 6 && !strings.ContainsAny(e, "/ ") {
			ext = e
		}
	}
	return fmt.Sprintf("%s-%s%s", slug, clipStartAt.UTC().Format("20060102T150405Z"), ext)
}

// slugifyName lowercases a name and keeps [a-z0-9-], collapsing other runs to a
// single hyphen, so it is a safe filename component.
func slugifyName(name string) string {
	var b strings.Builder
	prevHyphen := false
	for _, r := range strings.ToLower(strings.TrimSpace(name)) {
		switch {
		case r >= 'a' && r <= 'z', r >= '0' && r <= '9':
			b.WriteRune(r)
			prevHyphen = false
		default:
			if !prevHyphen && b.Len() > 0 {
				b.WriteByte('-')
				prevHyphen = true
			}
		}
	}
	return strings.Trim(b.String(), "-")
}

// releaseClip marks one account-owned clip released_at=now() WITHOUT deleting its
// R2 object: the recording_clips row + object_key + storage_destination_id +
// recording association are all kept. Released is DISTINCT from purged (purged =
// legacy R2-deleted). A released clip stops billing (excluded from the snapshot)
// and disappears from the org clip surfaces, but the bytes are retained for a
// future cross-user download feature. The UPDATE is account-scoped via the
// recordings join. Returns (found, alreadyReleased, err); an already-released or
// already-purged clip is left as-is (idempotent). NO r2 delete is ever issued.
//
// requireNASPull additionally confines the target to a delivery='nas_pull'
// recording (mirroring the accountClipsCursorSQL feed): the pull key hands out ONLY
// nas_pull clips, so its release must never touch a managed recording's clip even
// if a leaked key enumerates the sequential clip id. A managed clip is reported
// not-found (found=false), same as an out-of-scope id.
func (s *Server) releaseClip(ctx context.Context, accountID, recordingID, clipID int64, requireNASPull bool) (found, alreadyReleased bool, err error) {
	deliveryPredicate := ""
	if requireNASPull {
		deliveryPredicate = ` AND r.delivery = '` + string(deliveryNASPull) + `'`
	}
	var purgedAt, releasedAt *time.Time
	err = s.pool.QueryRow(ctx, `
		SELECT c.purged_at, c.released_at
		FROM recording_clips c
		JOIN recordings r ON r.id = c.recording_id
		WHERE c.id=$1 AND c.recording_id=$2 AND r.account_id=$3`+deliveryPredicate, clipID, recordingID, accountID).Scan(&purgedAt, &releasedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, false, nil
	}
	if err != nil {
		return false, false, err
	}
	if purgedAt != nil || releasedAt != nil {
		return true, true, nil
	}
	if _, err := s.pool.Exec(ctx, `
		UPDATE recording_clips SET released_at=now() WHERE id=$1 AND released_at IS NULL
	`, clipID); err != nil {
		return true, false, err
	}
	return true, false, nil
}

// handleAccountRecordingClipDelete RELEASES one clip: it marks the row
// released_at=now() and KEEPS the R2 object + row + associations. No R2 delete is
// issued (DENIZ policy: recorded content is never hard-deleted). The org stops
// being billed for the clip and no longer sees it, but the bytes are retained. An
// already-released or already-purged clip returns 200 idempotently.
func (s *Server) handleAccountRecordingClipDelete(w http.ResponseWriter, r *http.Request) {
	principal, ok := accountPrincipalFromContext(r.Context())
	if !ok {
		util.WriteError(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	recordingID, ok := parseInt64Path(w, r, "id")
	if !ok {
		return
	}
	clipID, ok := parseInt64Path(w, r, "clipId")
	if !ok {
		return
	}

	found, _, err := s.releaseClip(r.Context(), principal.AccountID, recordingID, clipID, false)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("release clip: %v", err))
		return
	}
	if !found {
		util.WriteError(w, http.StatusNotFound, "clip not found")
		return
	}
	util.WriteJSON(w, http.StatusOK, map[string]any{"id": clipID, "released": true})
}

// handleAccountRecordingClipRelease is the pull-key RELEASE endpoint. The NAS pull
// client calls it after it has downloaded + byte-verified a clip: the managed copy
// is DETACHED from the org (released_at=now(), billing stops, org can no longer see
// it) but the R2 object + row + association are KEPT. Same release-semantics as the
// user-facing delete; exposed as an explicit POST .../release so the pull key's
// allowlist grants release without granting hard-delete.
//
// The release is confined to delivery='nas_pull' recordings (requireNASPull=true),
// matching the feed (accountClipsCursorSQL) that only ever hands out nas_pull clips.
// A leaked/malicious pull key can POST .../release on ANY sequential clip id, so
// without this guard it could detach a managed recording's footage that the feed
// never offered. A managed (or out-of-scope) clip is 404 (found=false).
func (s *Server) handleAccountRecordingClipRelease(w http.ResponseWriter, r *http.Request) {
	principal, ok := accountPrincipalFromContext(r.Context())
	if !ok {
		util.WriteError(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	recordingID, ok := parseInt64Path(w, r, "id")
	if !ok {
		return
	}
	clipID, ok := parseInt64Path(w, r, "clipId")
	if !ok {
		return
	}

	found, _, err := s.releaseClip(r.Context(), principal.AccountID, recordingID, clipID, true)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("release clip: %v", err))
		return
	}
	if !found {
		util.WriteError(w, http.StatusNotFound, "clip not found")
		return
	}
	util.WriteJSON(w, http.StatusOK, map[string]any{"id": clipID, "released": true})
}

// handleAccountRecordingClipsDeleteAll RELEASES every still-org-visible clip of
// one recording in a single account-scoped UPDATE: released_at=now() on all rows
// that are neither purged nor already released. No R2 delete is issued (DENIZ
// policy) and the rows + objects + associations are all kept; the org stops being
// billed and no longer sees them. Idempotent: a re-run releases only the remaining
// active clips.
func (s *Server) handleAccountRecordingClipsDeleteAll(w http.ResponseWriter, r *http.Request) {
	principal, ok := accountPrincipalFromContext(r.Context())
	if !ok {
		util.WriteError(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	recordingID, ok := parseInt64Path(w, r, "id")
	if !ok {
		return
	}

	var ownerOK bool
	if err := s.pool.QueryRow(r.Context(), `
		SELECT EXISTS(SELECT 1 FROM recordings WHERE id=$1 AND account_id=$2)
	`, recordingID, principal.AccountID).Scan(&ownerOK); err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("load recording: %v", err))
		return
	}
	if !ownerOK {
		util.WriteError(w, http.StatusNotFound, "recording not found")
		return
	}

	ct, err := s.pool.Exec(r.Context(), `
		UPDATE recording_clips
		SET released_at=now()
		WHERE recording_id=$1 AND purged_at IS NULL AND released_at IS NULL
	`, recordingID)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("release clips: %v", err))
		return
	}
	util.WriteJSON(w, http.StatusOK, map[string]any{"released": ct.RowsAffected()})
}

// clipZipRequest selects the page of clips to archive. It mirrors the clips-list
// pagination so "Download all" zips exactly the page the UI is showing.
type clipZipRequest struct {
	Limit  int `json:"limit"`
	Offset int `json:"offset"`
}

// clipZipRow carries one clip's location + metadata for the zip builder, grouped
// later by storage destination so a single r2 client opens all of a destination's
// objects.
type clipZipRow struct {
	row  dayZipSegmentRow
	dest clipDestination
}

// handleAccountRecordingClipsZip archives one page (<=dayZipMaxItems) of a
// recording's clips into a STORE zip with an in-zip manifest.csv
// (id,start,end,duration,size,object_key), reusing the day-zip job machinery and
// builder. The page is selected by limit/offset matching the clips list. The
// assembled zip is streamed into the operator export bucket (s.r2) and delivered
// via the same handleDayZipGet polling endpoint.
func (s *Server) handleAccountRecordingClipsZip(w http.ResponseWriter, r *http.Request) {
	principal, ok := accountPrincipalFromContext(r.Context())
	if !ok {
		util.WriteError(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	if s.secrets == nil {
		util.WriteError(w, http.StatusServiceUnavailable, "storage credential key is unset")
		return
	}
	if s.r2 == nil {
		util.WriteError(w, http.StatusServiceUnavailable, "export storage is not configured")
		return
	}
	recordingID, ok := parseInt64Path(w, r, "id")
	if !ok {
		return
	}
	var req clipZipRequest
	if err := util.DecodeJSON(r, &req); err != nil {
		util.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	limit := req.Limit
	if limit <= 0 {
		limit = 100
	}
	if limit > 500 {
		limit = 500
	}
	offset := req.Offset
	if offset < 0 {
		offset = 0
	}

	var recordingName string
	err := s.pool.QueryRow(r.Context(), `
		SELECT name FROM recordings WHERE id=$1 AND account_id=$2 AND status <> 'canceled'
	`, recordingID, principal.AccountID).Scan(&recordingName)
	if errors.Is(err, pgx.ErrNoRows) {
		util.WriteError(w, http.StatusNotFound, "recording not found")
		return
	}
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("load recording: %v", err))
		return
	}

	// Load the page of still-org-visible clips with their destination snapshot,
	// newest fire first (same order as the clips list). Released clips (detached to
	// NAS) are excluded alongside purged, so a released clip is never re-zipped
	// (consistent with the individual-download 410 and the view-clips filter).
	rows, err := s.pool.Query(r.Context(), `
		SELECT c.id, c.clip_start_at, c.clip_end_at, c.duration_ms, c.size_bytes, c.object_key,
		       sd.region, sd.bucket, sd.endpoint, sd.access_key_id, sd.secret_access_key_enc
		FROM recording_clips c
		JOIN storage_destinations sd ON sd.id = c.storage_destination_id
		WHERE c.recording_id=$1 AND c.purged_at IS NULL AND c.released_at IS NULL
		ORDER BY c.fire_at DESC
		LIMIT $2 OFFSET $3
	`, recordingID, limit, offset)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("list clips: %v", err))
		return
	}
	defer rows.Close()
	var clips []clipZipRow
	var totalBytes int64
	for rows.Next() {
		var (
			z       clipZipRow
			clipID  int64
			startAt time.Time
			endAt   time.Time
		)
		if err := rows.Scan(&clipID, &startAt, &endAt, &z.row.DurationMs, &z.row.SizeBytes, &z.row.ObjectKey,
			&z.dest.region, &z.dest.bucket, &z.dest.endpoint, &z.dest.accessKeyID, &z.dest.secretEnc); err != nil {
			util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("scan clip: %v", err))
			return
		}
		end := endAt.UTC()
		z.row.ID = clipID
		z.row.SegmentStartAt = startAt.UTC()
		z.row.ClipEndAt = &end
		z.row.MIMEType = "video/mp4"
		totalBytes += z.row.SizeBytes
		clips = append(clips, z)
	}
	if err := rows.Err(); err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("iterate clips: %v", err))
		return
	}
	if len(clips) == 0 {
		util.WriteError(w, http.StatusNotFound, "no clips on this page")
		return
	}
	if totalBytes > dayZipMaxBytes {
		util.WriteError(w, http.StatusBadRequest, "page too large")
		return
	}

	// 1-job lock: non-blocking try-acquire (shared with the public day-zip system).
	select {
	case s.dayZipSlot <- struct{}{}:
	default:
		util.WriteError(w, http.StatusConflict, "busy")
		return
	}

	slug := slugifyName(recordingName)
	if slug == "" {
		slug = fmt.Sprintf("recording-%d", recordingID)
	}
	jobID := uuid.NewString()
	zipKey := fmt.Sprintf("exports/account/%d/recording-%d-%s.zip", principal.AccountID, recordingID, jobID)
	s.setDayZipJob(&dayZipJob{
		ID:        jobID,
		StreamID:  recordingID,
		Status:    "pending",
		ZipKey:    zipKey,
		ItemCount: len(clips),
		SizeBytes: totalBytes,
	})
	go s.runClipsZipJob(jobID, slug, recordingID, clips)

	util.WriteJSON(w, http.StatusAccepted, map[string]any{
		"job_id": jobID,
		"status": "pending",
	})
}

// runClipsZipJob streams the page's clips (read from the user's destination
// buckets) into a zip uploaded to the operator export bucket, reusing buildDayZip
// and the dayZipJob status machinery. Clips are grouped by destination so each
// group opens with its own r2 client; within a group buildDayZip handles the
// streaming + manifest.
func (s *Server) runClipsZipJob(jobID, slug string, recordingID int64, clips []clipZipRow) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()
	defer func() { <-s.dayZipSlot }()

	startedAt := time.Now().UTC()
	s.updateDayZipJob(jobID, func(job *dayZipJob) {
		job.Status = "running"
		job.StartedAt = &startedAt
		job.ErrorText = ""
	})
	job, ok := s.getDayZipJob(jobID)
	if !ok {
		return
	}

	failJob := func(format string, args ...any) {
		finishedAt := time.Now().UTC()
		s.updateDayZipJob(jobID, func(j *dayZipJob) {
			j.Status = "error"
			j.ErrorText = fmt.Sprintf(format, args...)
			j.FinishedAt = &finishedAt
		})
	}

	// Group rows by destination snapshot so one client opens all of a group's
	// objects; build a per-key->client map for the open func.
	type destClient struct {
		client *r2.Client
		dest   clipDestination
	}
	clients := make(map[string]*destClient)
	rows := make([]dayZipSegmentRow, 0, len(clips))
	keyClient := make(map[string]*r2.Client, len(clips))
	for _, c := range clips {
		gk := c.dest.endpoint + "\x00" + c.dest.bucket + "\x00" + c.dest.accessKeyID
		dc, present := clients[gk]
		if !present {
			client, err := s.buildClipClientCtx(ctx, c.dest)
			if err != nil {
				failJob("build destination client: %v", err)
				return
			}
			dc = &destClient{client: client, dest: c.dest}
			clients[gk] = dc
		}
		keyClient[c.row.ObjectKey] = dc.client
		rows = append(rows, c.row)
	}

	open := func(ctx context.Context, key string) (io.ReadCloser, error) {
		client, ok := keyClient[key]
		if !ok {
			return nil, fmt.Errorf("no client for object %s", key)
		}
		return client.Open(ctx, key)
	}

	// Stream zip directly from source reads -> pipe -> operator export bucket, so
	// the whole archive is never materialized (mirrors runDayZipJob).
	pr, pw := io.Pipe()
	buildDone := make(chan error, 1)
	go func() {
		_, buildErr := buildDayZip(ctx, pw, rows, slug, recordingID, open, func(n int) {
			s.updateDayZipJob(jobID, func(j *dayZipJob) { j.Processed = n })
		})
		_ = pw.CloseWithError(buildErr)
		buildDone <- buildErr
	}()

	_, upErr := s.r2.PutMultipart(ctx, job.ZipKey, "application/zip", pr)
	_ = pr.CloseWithError(upErr)
	buildErr := <-buildDone

	if buildErr != nil {
		failJob("%v", buildErr)
		return
	}
	if upErr != nil {
		failJob("upload archive: %v", upErr)
		return
	}

	finishedAt := time.Now().UTC()
	s.updateDayZipJob(jobID, func(j *dayZipJob) {
		j.Status = "complete"
		j.FinishedAt = &finishedAt
		j.ErrorText = ""
	})
}

// buildClipClientCtx is buildClipClient without the *http.Request, for use from a
// background job that owns its own context.
func (s *Server) buildClipClientCtx(ctx context.Context, d clipDestination) (*r2.Client, error) {
	secret, err := s.secrets.Decrypt(d.secretEnc)
	if err != nil {
		return nil, fmt.Errorf("decrypt destination secret: %w", err)
	}
	return r2.New(ctx, r2.Config{
		AccessKey: d.accessKeyID,
		SecretKey: string(secret),
		Region:    d.region,
		Bucket:    d.bucket,
		Endpoint:  d.endpoint,
	})
}

type clipTransferRequest struct {
	TargetStorageDestinationID int64 `json:"target_storage_destination_id"`
}

type clipTransferJobResponse struct {
	ID                         int64      `json:"id"`
	RecordingClipID            int64      `json:"recording_clip_id"`
	TargetStorageDestinationID int64      `json:"target_storage_destination_id"`
	Status                     string     `json:"status"`
	BytesCopied                int64      `json:"bytes_copied"`
	ErrorText                  string     `json:"error_text"`
	CreatedAt                  time.Time  `json:"created_at"`
	CompletedAt                *time.Time `json:"completed_at"`
}

// handleAccountRecordingClipTransfer enqueues an async COPY of one clip into
// another storage destination the same account owns. The copy is performed by
// the clip-transfer worker in recorder-control (streamed GET source -> PUT
// target). It is never a move: the source clip is left intact (purged_at
// untouched). Re-enqueue of an already in-flight/done transfer is a no-op that
// returns the current job (the idempotency_key dedups on clip+target).
func (s *Server) handleAccountRecordingClipTransfer(w http.ResponseWriter, r *http.Request) {
	principal, ok := accountPrincipalFromContext(r.Context())
	if !ok {
		util.WriteError(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	recordingID, ok := parseInt64Path(w, r, "id")
	if !ok {
		return
	}
	clipID, ok := parseInt64Path(w, r, "clipId")
	if !ok {
		return
	}
	var req clipTransferRequest
	if err := util.DecodeJSON(r, &req); err != nil {
		util.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	if req.TargetStorageDestinationID <= 0 {
		util.WriteError(w, http.StatusBadRequest, "target_storage_destination_id is required")
		return
	}

	// Load the clip under the account scope: it must belong to a recording the
	// caller owns, must not be purged, and must not be released (a released clip
	// has been drained to the NAS and its managed copy is gone, so it can never be
	// individually re-transferred). sourceDestID lets us reject a no-op copy onto
	// the clip's own destination.
	var (
		sourceDestID int64
		objectKey    string
		purgedAt     *time.Time
	)
	err := s.pool.QueryRow(r.Context(), `
		SELECT c.storage_destination_id, c.object_key, c.purged_at
		FROM recording_clips c
		JOIN recordings rec ON rec.id = c.recording_id
		WHERE c.id=$1 AND c.recording_id=$2 AND rec.account_id=$3 AND c.released_at IS NULL
	`, clipID, recordingID, principal.AccountID).Scan(&sourceDestID, &objectKey, &purgedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		util.WriteError(w, http.StatusNotFound, "clip not found")
		return
	}
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("load clip: %v", err))
		return
	}
	if purgedAt != nil {
		util.WriteError(w, http.StatusGone, "clip was deleted")
		return
	}
	if req.TargetStorageDestinationID == sourceDestID {
		util.WriteError(w, http.StatusBadRequest, "target is the clip's own storage destination; nothing to copy")
		return
	}

	// The target destination must be owned by the same account, or be a shared
	// destination the account was granted (same owner-or-granted predicate as
	// recording-create selection).
	var targetKeyPrefix string
	err = s.pool.QueryRow(r.Context(), fmt.Sprintf(`
		SELECT sd.key_prefix FROM storage_destinations sd WHERE sd.id=$1 AND %s
	`, fmt.Sprintf(storageDestAccessPredicate, "$2")), req.TargetStorageDestinationID, principal.AccountID).Scan(&targetKeyPrefix)
	if errors.Is(err, pgx.ErrNoRows) {
		util.WriteError(w, http.StatusNotFound, "target storage destination not found")
		return
	}
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("load target destination: %v", err))
		return
	}

	targetObjectKey := buildClipTransferObjectKey(targetKeyPrefix, recordingID, clipID, objectKey)
	idempotencyKey := fmt.Sprintf("xfer:%d:%d", clipID, req.TargetStorageDestinationID)

	// Insert-or-fetch: a new clip+target enqueues; a re-enqueue is a no-op that
	// returns the existing job row (and its current status).
	var resp clipTransferJobResponse
	err = s.pool.QueryRow(r.Context(), `
		WITH ins AS (
			INSERT INTO clip_transfer_jobs
				(account_id, recording_clip_id, target_storage_destination_id, target_object_key, idempotency_key)
			VALUES ($1,$2,$3,$4,$5)
			ON CONFLICT (idempotency_key) DO NOTHING
			RETURNING id, recording_clip_id, target_storage_destination_id, status, bytes_copied, error_text, created_at, completed_at
		)
		SELECT id, recording_clip_id, target_storage_destination_id, status, bytes_copied, error_text, created_at, completed_at FROM ins
		UNION ALL
		SELECT id, recording_clip_id, target_storage_destination_id, status, bytes_copied, error_text, created_at, completed_at
		FROM clip_transfer_jobs WHERE idempotency_key=$5
		LIMIT 1
	`, principal.AccountID, clipID, req.TargetStorageDestinationID, targetObjectKey, idempotencyKey).Scan(
		&resp.ID, &resp.RecordingClipID, &resp.TargetStorageDestinationID, &resp.Status,
		&resp.BytesCopied, &resp.ErrorText, &resp.CreatedAt, &resp.CompletedAt,
	)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("enqueue clip transfer: %v", err))
		return
	}
	util.WriteJSON(w, http.StatusOK, resp)
}

// exportBatchSize bounds one multi-row clip_transfer_jobs insert during a bulk
// export. The clip set is streamed from a server-side cursor and flushed in
// batches of this size so an account with thousands of clips never materializes
// the whole set in memory.
const exportBatchSize = 500

// exportCreateRequest is the bulk-export body. scope selects the clip set
// (account: every clip; recording: one recording's clips; bundle: every member
// recording's clips). One target storage_destination_id receives the copies; an
// optional purge_source deletes each managed source object after a confirmed
// copy so managed storage stops accruing stream_hour_month.
type exportCreateRequest struct {
	Scope                      string `json:"scope"`
	RecordingID                int64  `json:"recording_id"`
	BundleID                   int64  `json:"bundle_id"`
	StorageDestinationID       int64  `json:"storage_destination_id"`
	TargetStorageDestinationID int64  `json:"target_storage_destination_id"`
	PurgeSource                bool   `json:"purge_source"`
}

// exportClipRow is one streamed clip in scope: the minimum needed to build the
// target key + idempotency key for its transfer job.
type exportClipRow struct {
	id          int64
	recordingID int64
	objectKey   string
}

// handleAccountExportCreate enqueues a clip_transfer_job for EVERY not-yet-
// transferred clip in a scope to one storage destination, reusing the exact
// per-clip enqueue contract of handleAccountRecordingClipTransfer (same
// idempotency_key namespace and target_object_key builder) so a bulk-enqueued
// row is indistinguishable from a single-clip transfer and the same worker
// drains it. The clip set is streamed from a server-side cursor and inserted in
// batches with ON CONFLICT (idempotency_key) DO NOTHING, so the operation is
// idempotent and resumable: a clip already enqueued/in-flight/done to the target
// is skipped, and a re-run only inserts the remainder. Clips already living in
// the target destination, or already purged, are excluded by the scope query so
// no self-copy is ever enqueued.
func (s *Server) handleAccountExportCreate(w http.ResponseWriter, r *http.Request) {
	principal, ok := accountPrincipalFromContext(r.Context())
	if !ok {
		util.WriteError(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	var req exportCreateRequest
	if err := util.DecodeJSON(r, &req); err != nil {
		util.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	targetDestID := req.StorageDestinationID
	if targetDestID <= 0 {
		targetDestID = req.TargetStorageDestinationID
	}
	if targetDestID <= 0 {
		util.WriteError(w, http.StatusBadRequest, "storage_destination_id is required")
		return
	}
	scope := strings.TrimSpace(req.Scope)
	if scope == "" {
		scope = "account"
	}

	// The target destination must be owned by the same account, or be a shared
	// destination the account was granted (same predicate the single-clip
	// transfer uses). Its key_prefix is needed to build each target object key.
	var targetKeyPrefix string
	err := s.pool.QueryRow(r.Context(), fmt.Sprintf(`
		SELECT sd.key_prefix FROM storage_destinations sd WHERE sd.id=$1 AND %s
	`, fmt.Sprintf(storageDestAccessPredicate, "$2")), targetDestID, principal.AccountID).Scan(&targetKeyPrefix)
	if errors.Is(err, pgx.ErrNoRows) {
		util.WriteError(w, http.StatusNotFound, "target storage destination not found")
		return
	}
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("load target destination: %v", err))
		return
	}

	// Resolve the scope to a WHERE predicate over the shared
	// recording_clips JOIN recordings pattern, account-scoped. recording and
	// bundle scopes first verify the caller owns the parent (mirroring the
	// per-recording / bundle owner checks).
	var scopeSQL string
	args := []any{principal.AccountID, targetDestID}
	switch scope {
	case "account":
		scopeSQL = ""
	case "recording":
		if req.RecordingID <= 0 {
			util.WriteError(w, http.StatusBadRequest, "recording_id is required for scope=recording")
			return
		}
		var ownerOK bool
		if err := s.pool.QueryRow(r.Context(), `
			SELECT EXISTS(SELECT 1 FROM recordings WHERE id=$1 AND account_id=$2)
		`, req.RecordingID, principal.AccountID).Scan(&ownerOK); err != nil {
			util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("load recording: %v", err))
			return
		}
		if !ownerOK {
			util.WriteError(w, http.StatusNotFound, "recording not found")
			return
		}
		args = append(args, req.RecordingID)
		scopeSQL = fmt.Sprintf("AND c.recording_id=$%d", len(args))
	case "bundle":
		if req.BundleID <= 0 {
			util.WriteError(w, http.StatusBadRequest, "bundle_id is required for scope=bundle")
			return
		}
		var ownerOK bool
		if err := s.pool.QueryRow(r.Context(), `
			SELECT EXISTS(SELECT 1 FROM recording_bundles WHERE id=$1 AND account_id=$2 AND status <> 'canceled')
		`, req.BundleID, principal.AccountID).Scan(&ownerOK); err != nil {
			util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("load bundle: %v", err))
			return
		}
		if !ownerOK {
			util.WriteError(w, http.StatusNotFound, "bundle not found")
			return
		}
		args = append(args, req.BundleID)
		scopeSQL = fmt.Sprintf("AND rec.bundle_id=$%d", len(args))
	default:
		util.WriteError(w, http.StatusBadRequest, "scope must be account, recording, or bundle")
		return
	}

	// Stream the in-scope clip set: not purged, not released (a released clip is
	// detached to NAS and must never be re-transferred), not already living in the
	// target (so a self-copy is never enqueued), account-scoped, ordered by id so a
	// resumed run is deterministic. The cursor is iterated WITHOUT materializing
	// all rows; rows accumulate into a fixed-size batch buffer and flush.
	rows, err := s.pool.Query(r.Context(), fmt.Sprintf(`
		SELECT c.id, c.recording_id, c.object_key
		FROM recording_clips c
		JOIN recordings rec ON rec.id = c.recording_id
		WHERE rec.account_id=$1
		  AND c.purged_at IS NULL
		  AND c.released_at IS NULL
		  AND c.storage_destination_id <> $2
		  %s
		ORDER BY c.id ASC
	`, scopeSQL), args...)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("list clips: %v", err))
		return
	}
	defer rows.Close()

	var (
		total    int64
		enqueued int64
		batch    []exportClipRow
	)
	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		n, err := s.enqueueExportBatch(r.Context(), principal.AccountID, targetDestID, targetKeyPrefix, req.PurgeSource, batch)
		if err != nil {
			return err
		}
		enqueued += n
		batch = batch[:0]
		return nil
	}
	for rows.Next() {
		var row exportClipRow
		if err := rows.Scan(&row.id, &row.recordingID, &row.objectKey); err != nil {
			util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("scan clip: %v", err))
			return
		}
		total++
		batch = append(batch, row)
		if len(batch) >= exportBatchSize {
			if err := flush(); err != nil {
				util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("enqueue clip transfers: %v", err))
				return
			}
		}
	}
	if err := rows.Err(); err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("iterate clips: %v", err))
		return
	}
	if err := flush(); err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("enqueue clip transfers: %v", err))
		return
	}

	util.WriteJSON(w, http.StatusOK, map[string]any{
		"scope":                         scope,
		"recording_id":                  req.RecordingID,
		"bundle_id":                     req.BundleID,
		"target_storage_destination_id": targetDestID,
		"total_clips":                   total,
		"enqueued":                      enqueued,
		"already_present":               total - enqueued,
		"purge_source":                  req.PurgeSource,
	})
}

// enqueueExportBatch inserts one batch of clip_transfer_jobs in a single Exec
// via parallel arrays (unnest), with ON CONFLICT (idempotency_key) DO NOTHING so
// a clip already enqueued/in-flight/done to the target is skipped. The
// target_object_key and idempotency_key are built exactly as the single-clip
// transfer builds them, so bulk and single-clip rows share one job namespace.
// Returns the number of rows actually inserted (newly enqueued).
func (s *Server) enqueueExportBatch(ctx context.Context, accountID, targetDestID int64, targetKeyPrefix string, purge bool, batch []exportClipRow) (int64, error) {
	clipIDs := make([]int64, len(batch))
	targetKeys := make([]string, len(batch))
	idemKeys := make([]string, len(batch))
	for i, row := range batch {
		clipIDs[i] = row.id
		targetKeys[i] = buildClipTransferObjectKey(targetKeyPrefix, row.recordingID, row.id, row.objectKey)
		idemKeys[i] = fmt.Sprintf("xfer:%d:%d", row.id, targetDestID)
	}
	tag, err := s.pool.Exec(ctx, `
		INSERT INTO clip_transfer_jobs
			(account_id, recording_clip_id, target_storage_destination_id, target_object_key, idempotency_key, auto_purge_source)
		SELECT $1, clip_id, $2, target_key, idem_key, $3
		FROM unnest($4::bigint[], $5::text[], $6::text[]) AS t(clip_id, target_key, idem_key)
		ON CONFLICT (idempotency_key) DO NOTHING
	`, accountID, targetDestID, purge, clipIDs, targetKeys, idemKeys)
	if err != nil {
		return 0, err
	}
	return tag.RowsAffected(), nil
}

// handleAccountExportProgress aggregates clip_transfer_jobs by status for a
// scope+target so the UI can show queued / in-progress / done / failed counts
// for a bulk export. Progress is derived from the job rows themselves (no
// separate export table): the same scope predicate + target destination that the
// create endpoint enqueued is re-applied here. pending+leased = in-progress.
func (s *Server) handleAccountExportProgress(w http.ResponseWriter, r *http.Request) {
	principal, ok := accountPrincipalFromContext(r.Context())
	if !ok {
		util.WriteError(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	targetDestID := parseInt64Query(r, "storage_destination_id")
	if targetDestID <= 0 {
		targetDestID = parseInt64Query(r, "target_storage_destination_id")
	}
	if targetDestID <= 0 {
		util.WriteError(w, http.StatusBadRequest, "storage_destination_id is required")
		return
	}
	scope := strings.TrimSpace(r.URL.Query().Get("scope"))
	if scope == "" {
		scope = "account"
	}

	var scopeSQL string
	args := []any{principal.AccountID, targetDestID}
	switch scope {
	case "account":
		scopeSQL = ""
	case "recording":
		rid := parseInt64Query(r, "recording_id")
		if rid <= 0 {
			util.WriteError(w, http.StatusBadRequest, "recording_id is required for scope=recording")
			return
		}
		args = append(args, rid)
		scopeSQL = fmt.Sprintf("AND c.recording_id=$%d", len(args))
	case "bundle":
		bid := parseInt64Query(r, "bundle_id")
		if bid <= 0 {
			util.WriteError(w, http.StatusBadRequest, "bundle_id is required for scope=bundle")
			return
		}
		args = append(args, bid)
		scopeSQL = fmt.Sprintf("AND rec.bundle_id=$%d", len(args))
	default:
		util.WriteError(w, http.StatusBadRequest, "scope must be account, recording, or bundle")
		return
	}

	rows, err := s.pool.Query(r.Context(), fmt.Sprintf(`
		SELECT j.status, count(*)
		FROM clip_transfer_jobs j
		JOIN recording_clips c ON c.id = j.recording_clip_id
		JOIN recordings rec ON rec.id = c.recording_id
		WHERE rec.account_id=$1 AND j.target_storage_destination_id=$2 %s
		GROUP BY j.status
	`, scopeSQL), args...)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("aggregate transfers: %v", err))
		return
	}
	defer rows.Close()
	counts := map[string]int64{}
	var total int64
	for rows.Next() {
		var status string
		var n int64
		if err := rows.Scan(&status, &n); err != nil {
			util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("scan transfer count: %v", err))
			return
		}
		counts[status] = n
		total += n
	}
	if err := rows.Err(); err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("iterate transfer counts: %v", err))
		return
	}
	inProgress := counts["pending"] + counts["leased"]
	util.WriteJSON(w, http.StatusOK, map[string]any{
		"scope":                         scope,
		"target_storage_destination_id": targetDestID,
		"pending":                       counts["pending"],
		"leased":                        counts["leased"],
		"in_progress":                   inProgress,
		"done":                          counts["done"],
		"error":                         counts["error"],
		"canceled":                      counts["canceled"],
		"total":                         total,
	})
}

// handleAccountRecordingTransfers lists the transfer jobs for a recording's
// clips, account-scoped. The target destination name is joined in so the UI can
// label each row.
func (s *Server) handleAccountRecordingTransfers(w http.ResponseWriter, r *http.Request) {
	principal, ok := accountPrincipalFromContext(r.Context())
	if !ok {
		util.WriteError(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	recordingID, ok := parseInt64Path(w, r, "id")
	if !ok {
		return
	}

	var ownerOK bool
	if err := s.pool.QueryRow(r.Context(), `
		SELECT EXISTS(SELECT 1 FROM recordings WHERE id=$1 AND account_id=$2)
	`, recordingID, principal.AccountID).Scan(&ownerOK); err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("load recording: %v", err))
		return
	}
	if !ownerOK {
		util.WriteError(w, http.StatusNotFound, "recording not found")
		return
	}

	rows, err := s.pool.Query(r.Context(), `
		SELECT j.id, j.recording_clip_id, j.target_storage_destination_id, sd.name,
		       j.status, j.bytes_copied, j.error_text, j.created_at, j.completed_at
		FROM clip_transfer_jobs j
		JOIN recording_clips c ON c.id = j.recording_clip_id
		JOIN storage_destinations sd ON sd.id = j.target_storage_destination_id
		WHERE c.recording_id=$1 AND j.account_id=$2
		ORDER BY j.id DESC
	`, recordingID, principal.AccountID)
	if err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("list transfers: %v", err))
		return
	}
	defer rows.Close()
	items := make([]map[string]any, 0, 16)
	for rows.Next() {
		var (
			id          int64
			clipID      int64
			targetID    int64
			targetName  string
			status      string
			bytesCopied int64
			errorText   string
			createdAt   time.Time
			completedAt *time.Time
		)
		if err := rows.Scan(&id, &clipID, &targetID, &targetName, &status, &bytesCopied, &errorText, &createdAt, &completedAt); err != nil {
			util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("scan transfer: %v", err))
			return
		}
		items = append(items, map[string]any{
			"id":                            id,
			"recording_clip_id":             clipID,
			"target_storage_destination_id": targetID,
			"target_name":                   targetName,
			"status":                        status,
			"bytes_copied":                  bytesCopied,
			"error_text":                    errorText,
			"created_at":                    createdAt.UTC(),
			"completed_at":                  completedAt,
		})
	}
	if err := rows.Err(); err != nil {
		util.WriteError(w, http.StatusInternalServerError, fmt.Sprintf("iterate transfers: %v", err))
		return
	}
	util.WriteJSON(w, http.StatusOK, map[string]any{"items": items})
}

// buildClipTransferObjectKey is the deterministic target key for a copied clip:
// the target destination's key_prefix joined to recordings/<recordingID>/
// <clipID>-<basename(sourceObjectKey)>. It is stable across re-enqueues so the
// idempotency_key (clip+target) and the resulting object map one-to-one. Empty,
// "." and ".." segments are dropped so a stored prefix cannot traverse out of
// the recordings namespace (mirrors buildRecordingClipObjectKey).
func buildClipTransferObjectKey(keyPrefix string, recordingID, clipID int64, sourceObjectKey string) string {
	parts := make([]string, 0, 5)
	for _, seg := range strings.Split(strings.Trim(strings.TrimSpace(keyPrefix), "/"), "/") {
		if seg == "" || seg == "." || seg == ".." {
			continue
		}
		parts = append(parts, seg)
	}
	base := sourceObjectKey
	if i := strings.LastIndex(base, "/"); i >= 0 {
		base = base[i+1:]
	}
	if base == "" {
		base = "clip"
	}
	parts = append(parts,
		"recordings",
		fmt.Sprintf("%d", recordingID),
		fmt.Sprintf("%d-%s", clipID, base),
	)
	return strings.Join(parts, "/")
}
