package api

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/daydemir/stoarama/backend/internal/util"
)

const (
	presentationV2ClaimLease       = 60 * time.Second
	presentationV2CompletionMargin = 15 * time.Second
	presentationV2MaxAttempts      = 5
)

type presentationV2AttemptRequest struct {
	AdmissionID      string `json:"admission_id"`
	IdempotencyKey   string `json:"idempotency_key"`
	FFmpegVersion    string `json:"ffmpeg_version"`
	FFprobeVersion   string `json:"ffprobe_version"`
	Libavformat      string `json:"libavformat_version"`
	Libavcodec       string `json:"libavcodec_version"`
	Libavutil        string `json:"libavutil_version"`
	BuildFlagsSHA256 string `json:"build_flags_sha256"`
	Demuxer          string `json:"demuxer_name"`
	VideoDecoder     string `json:"video_decoder_name"`
	AudioDecoder     string `json:"audio_decoder_name,omitempty"`
	ParserSchema     string `json:"parser_schema"`
}

type presentationV2IngestEnvelope struct {
	AttemptID                  string `json:"attempt_id"`
	LocalUploadIdentitySHA256  string `json:"local_upload_identity_sha256"`
	Disposition                string `json:"disposition"`
	StagingIdentitySHA256      string `json:"staging_identity_sha256,omitempty"`
	StagingMethod              string `json:"staging_method,omitempty"`
	StagingDeviceID            string `json:"staging_device_id,omitempty"`
	StagingInodeID             string `json:"staging_inode_id,omitempty"`
	StagingCloneIdentitySHA256 string `json:"staging_clone_identity_sha256,omitempty"`
	UnavailableReason          string `json:"unavailable_reason,omitempty"`
}

type presentationV2IngestReplay struct {
	TaskID             uuid.UUID
	ClipID             int64
	State              string
	RetentionState     string
	RequestSHA256      string
	ResponseSHA256     string
	AbsoluteDeadlineAt time.Time
}

type presentationV2QueryRower interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}

func loadPresentationV2IngestReplay(ctx context.Context, q presentationV2QueryRower, intentID uuid.UUID, p nodePrincipal, lease *uuid.UUID) (presentationV2IngestReplay, error) {
	var out presentationV2IngestReplay
	err := q.QueryRow(ctx, `
		SELECT id,clip_id,state,retention_state,request_sha256,response_sha256,absolute_deadline_at
		FROM recording_presentation_v2_probe_tasks
		WHERE upload_intent_id=$1 AND account_id=$2 AND node_id=$3 AND lease_token=$4
	`, intentID, p.AccountID, p.NodeID, lease).Scan(&out.TaskID, &out.ClipID, &out.State, &out.RetentionState, &out.RequestSHA256, &out.ResponseSHA256, &out.AbsoluteDeadlineAt)
	return out, err
}

func presentationV2ReplayResponse(out presentationV2IngestReplay) map[string]any {
	return map[string]any{"clip_id": out.ClipID, "presentation_probe": map[string]any{
		"task_id": out.TaskID, "state": out.State, "retention_state": out.RetentionState,
		"absolute_deadline_at": out.AbsoluteDeadlineAt, "request_sha256": out.RequestSHA256,
		"creation_response_sha256": out.ResponseSHA256, "replayed": true,
	}}
}

func validatePresentationV2IngestEnvelope(v *presentationV2IngestEnvelope) (uuid.UUID, error) {
	if v == nil {
		return uuid.Nil, nil
	}
	id, err := uuid.Parse(strings.TrimSpace(v.AttemptID))
	if err != nil || !lowerHex64(v.LocalUploadIdentitySHA256) {
		return uuid.Nil, fmt.Errorf("invalid presentation probe identity")
	}
	switch v.Disposition {
	case "retained":
		if !lowerHex64(v.StagingIdentitySHA256) || (v.StagingMethod != "hardlink" && v.StagingMethod != "clone") || v.UnavailableReason != "" {
			return uuid.Nil, fmt.Errorf("retained presentation probe requires staging identity only")
		}
		if v.StagingMethod == "hardlink" && (!canonicalUnsignedDecimal(v.StagingDeviceID, true) || !canonicalUnsignedDecimal(v.StagingInodeID, false) || v.StagingCloneIdentitySHA256 != "") {
			return uuid.Nil, fmt.Errorf("hardlink presentation probe requires exact device and inode")
		}
		if v.StagingMethod == "clone" && (v.StagingDeviceID != "" || v.StagingInodeID != "" || !lowerHex64(v.StagingCloneIdentitySHA256)) {
			return uuid.Nil, fmt.Errorf("clone presentation probe requires exact clone identity")
		}
	case "unavailable":
		if v.StagingIdentitySHA256 != "" || v.StagingMethod != "" || v.StagingDeviceID != "" || v.StagingInodeID != "" || v.StagingCloneIdentitySHA256 != "" || !presentationUnavailableReason(v.UnavailableReason) {
			return uuid.Nil, fmt.Errorf("unavailable presentation probe reason is invalid")
		}
	default:
		return uuid.Nil, fmt.Errorf("presentation probe disposition is invalid")
	}
	return id, nil
}

func canonicalUnsignedDecimal(v string, zeroAllowed bool) bool {
	if v == "" || (len(v) > 1 && v[0] == '0') {
		return false
	}
	n, err := strconv.ParseUint(v, 10, 64)
	return err == nil && (zeroAllowed || n > 0)
}

func presentationUnavailableReason(v string) bool {
	switch v {
	case "retention_unavailable", "state_reserve", "link_unavailable", "retention_deadline":
		return true
	}
	return false
}

func presentationRuntimeUnavailableReason(v string) bool {
	switch v {
	case "probe_timeout", "probe_resource_limit", "probe_unavailable", "retention_lost", "retention_deadline":
		return true
	}
	return false
}

func lowerHex64(v string) bool {
	v = strings.TrimSpace(v)
	if len(v) != 64 {
		return false
	}
	_, err := hex.DecodeString(v)
	return err == nil && v == strings.ToLower(v)
}

func presentationV2IngestRequestSHA(intentID uuid.UUID, req recordingClipIngestRequest) string {
	// Bind recovery to every typed ingest field. JSON whitespace and object-key
	// ordering are deliberately ignored, while any semantic request change is a
	// conflict after an ambiguous successful commit.
	req.IntentID = intentID.String()
	req.SHA256 = strings.ToLower(strings.TrimSpace(req.SHA256))
	raw, _ := json.Marshal(req)
	return presentationSHA(raw)
}

type presentationV2Axis struct {
	Axis                 string                     `json:"axis"`
	Status               string                     `json:"status"`
	Reason               string                     `json:"reason,omitempty"`
	StreamIndex          *int                       `json:"stream_index,omitempty"`
	UnitCount            *int64                     `json:"unit_count,omitempty"`
	CanonicalSHA256      string                     `json:"canonical_sha256,omitempty"`
	TimeBaseNum          *int64                     `json:"time_base_num,omitempty"`
	TimeBaseDen          *int64                     `json:"time_base_den,omitempty"`
	FirstOrdinal         *int64                     `json:"first_ordinal,omitempty"`
	FirstTimestamp       *int64                     `json:"first_timestamp,omitempty"`
	EndOrdinal           *int64                     `json:"end_ordinal,omitempty"`
	EndTimestamp         *int64                     `json:"end_timestamp,omitempty"`
	NonmonotonicCount    *int64                     `json:"nonmonotonic_count,omitempty"`
	DuplicateCount       *int64                     `json:"duplicate_count,omitempty"`
	HoleCount            *int64                     `json:"hole_count,omitempty"`
	OverlapCount         *int64                     `json:"overlap_count,omitempty"`
	SampleRate           *int                       `json:"sample_rate,omitempty"`
	ChannelCount         *int                       `json:"channel_count,omitempty"`
	ChannelLayout        string                     `json:"channel_layout,omitempty"`
	NormalizationProfile string                     `json:"normalization_profile,omitempty"`
	EditListSHA256       string                     `json:"edit_list_sha256,omitempty"`
	EditListKind         string                     `json:"edit_list_kind,omitempty"`
	SkipSamples          *int64                     `json:"skip_samples,omitempty"`
	DiscardPadding       *int64                     `json:"discard_padding,omitempty"`
	CodecDelay           *int64                     `json:"codec_delay,omitempty"`
	InitialPadding       *int64                     `json:"initial_padding,omitempty"`
	TrailingPadding      *int64                     `json:"trailing_padding,omitempty"`
	PacketEdges          []presentationV2PacketEdge `json:"packet_edges,omitempty"`
	RawExtents           []presentationV2RawExtent  `json:"raw_extents,omitempty"`
	VideoFrames          []presentationV2VideoEdge  `json:"video_frames,omitempty"`
	AudioBlocks          []presentationV2AudioEdge  `json:"audio_blocks,omitempty"`
}

type presentationV2PacketEdge struct {
	Side           string `json:"side"`
	Rank           int    `json:"rank"`
	Ordinal        int64  `json:"ordinal"`
	PTS            int64  `json:"pts"`
	DTS            int64  `json:"dts"`
	Duration       int64  `json:"duration"`
	TimeBaseNum    int64  `json:"time_base_num"`
	TimeBaseDen    int64  `json:"time_base_den"`
	Flags          int    `json:"flags"`
	SideDataSHA256 string `json:"side_data_sha256"`
	PayloadSHA256  string `json:"payload_sha256"`
}

type presentationV2RawExtent struct {
	Side      string `json:"side"`
	Rank      int    `json:"rank"`
	Ordinal   int64  `json:"ordinal"`
	Position  int64  `json:"position"`
	SizeBytes int64  `json:"size_bytes"`
	SHA256    string `json:"sha256"`
}

type presentationV2VideoEdge struct {
	Side        string `json:"side"`
	Rank        int    `json:"rank"`
	Ordinal     int64  `json:"ordinal"`
	PTS         int64  `json:"pts"`
	Duration    int64  `json:"duration"`
	TimeBaseNum int64  `json:"time_base_num"`
	TimeBaseDen int64  `json:"time_base_den"`
	PixelSHA256 string `json:"pixel_sha256,omitempty"`
}

type presentationV2AudioEdge struct {
	Side          string `json:"side"`
	Rank          int    `json:"rank"`
	Ordinal       int64  `json:"ordinal"`
	PTS           int64  `json:"pts"`
	SampleCount   int    `json:"sample_count"`
	TimeBaseNum   int64  `json:"time_base_num"`
	TimeBaseDen   int64  `json:"time_base_den"`
	SampleRate    int    `json:"sample_rate"`
	ChannelCount  int    `json:"channel_count"`
	ChannelLayout string `json:"channel_layout"`
	PCMSHA256     string `json:"pcm_sha256,omitempty"`
}

type presentationV2Completion struct {
	ClaimToken     string               `json:"claim_token"`
	RequestSHA256  string               `json:"request_sha256"`
	AuthoredStatus string               `json:"authored_status"`
	TerminalReason string               `json:"terminal_reason"`
	Axes           []presentationV2Axis `json:"axes"`
}

func decodePresentationV2(w http.ResponseWriter, r *http.Request, max int64, dst any) ([]byte, bool) {
	raw, err := io.ReadAll(http.MaxBytesReader(w, r.Body, max))
	if err != nil {
		util.WriteError(w, http.StatusBadRequest, "invalid presentation evidence request")
		return nil, false
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil || dec.Decode(&struct{}{}) != io.EOF {
		util.WriteError(w, http.StatusBadRequest, "invalid presentation evidence request")
		return nil, false
	}
	return raw, true
}

func presentationSHA(raw []byte) string {
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}

func presentationV2ToolIdentity(req presentationV2AttemptRequest) string {
	fields := []string{
		req.FFmpegVersion, req.FFprobeVersion, req.Libavformat, req.Libavcodec, req.Libavutil,
		req.BuildFlagsSHA256, req.Demuxer, req.VideoDecoder, req.AudioDecoder, req.ParserSchema,
	}
	var canonical strings.Builder
	canonical.WriteString("presentation-semantic-tool-v2\n")
	for _, field := range fields {
		fmt.Fprintf(&canonical, "%d:%s\n", len([]byte(field)), field)
	}
	return presentationSHA([]byte(canonical.String()))
}

type presentationV2RetentionIdentityInput struct {
	TaskID      uuid.UUID
	NodeID      int64
	Method      string
	DeviceID    string
	InodeID     string
	CloneSHA256 string
	SizeBytes   int64
	FileSHA256  string
	Deadline    time.Time
}

func presentationV2RetentionIdentity(v presentationV2RetentionIdentityInput) string {
	fields := []string{v.TaskID.String(), strconv.FormatInt(v.NodeID, 10), v.Method, v.DeviceID, v.InodeID,
		v.CloneSHA256, strconv.FormatInt(v.SizeBytes, 10), v.FileSHA256, strconv.FormatInt(v.Deadline.UnixMicro(), 10)}
	var canonical strings.Builder
	canonical.WriteString("presentation-retention-v2\n")
	for _, field := range fields {
		fmt.Fprintf(&canonical, "%d:%s\n", len([]byte(field)), field)
	}
	return presentationSHA([]byte(canonical.String()))
}

func presentationNodeTask(w http.ResponseWriter, r *http.Request) (nodePrincipal, uuid.UUID, bool) {
	p, ok := nodePrincipalFromContext(r.Context())
	if !ok || p.NodeType != nodeTypeRelay {
		util.WriteError(w, http.StatusForbidden, "relay node required")
		return nodePrincipal{}, uuid.Nil, false
	}
	id, err := uuid.Parse(strings.TrimSpace(chi.URLParam(r, "taskId")))
	if err != nil {
		util.WriteError(w, http.StatusBadRequest, "invalid presentation task id")
		return nodePrincipal{}, uuid.Nil, false
	}
	return p, id, true
}

func (s *Server) handleRecordingPresentationV2Attempt(w http.ResponseWriter, r *http.Request) {
	p, ok := nodePrincipalFromContext(r.Context())
	if !ok || p.NodeType != nodeTypeRelay {
		util.WriteError(w, http.StatusForbidden, "relay node required")
		return
	}
	jobID, ok := parseInt64Path(w, r, "id")
	if !ok {
		return
	}
	lease, err := recordingLeaseToken(r)
	if err != nil || lease == nil {
		util.WriteError(w, http.StatusBadRequest, "generation-fenced recording lease required")
		return
	}
	var req presentationV2AttemptRequest
	_, ok = decodePresentationV2(w, r, 8192, &req)
	if !ok {
		return
	}
	admissionID, err := uuid.Parse(req.AdmissionID)
	if err != nil || strings.TrimSpace(req.IdempotencyKey) == "" {
		util.WriteError(w, http.StatusBadRequest, "admission_id and idempotency_key are required")
		return
	}
	tx, err := s.pool.Begin(r.Context())
	if err != nil {
		util.WriteError(w, 500, "begin presentation attempt")
		return
	}
	defer tx.Rollback(r.Context())
	req.AdmissionID = admissionID.String()
	req.IdempotencyKey = strings.TrimSpace(req.IdempotencyKey)
	req.BuildFlagsSHA256 = strings.ToLower(strings.TrimSpace(req.BuildFlagsSHA256))
	if !lowerHex64(req.BuildFlagsSHA256) {
		util.WriteError(w, http.StatusBadRequest, "invalid presentation semantic tool identity")
		return
	}
	toolIdentity := presentationV2ToolIdentity(req)
	canonical, _ := json.Marshal(req)
	requestSHA := presentationSHA(canonical)
	var admittedToolIdentity string
	err = tx.QueryRow(r.Context(), `SELECT capture_tool_identity_sha256 FROM recording_presentation_v2_admissions WHERE id=$1 AND recording_job_id=$2 AND lease_token=$3 AND node_id=$4 AND account_id=$5`, admissionID, jobID, *lease, p.NodeID, p.AccountID).Scan(&admittedToolIdentity)
	if errors.Is(err, pgx.ErrNoRows) {
		util.WriteError(w, 404, "presentation admission unavailable")
		return
	}
	if err != nil {
		util.WriteError(w, 500, "load presentation admission tool identity")
		return
	}
	if admittedToolIdentity != toolIdentity {
		util.WriteError(w, 409, "presentation semantic tool identity differs from admission")
		return
	}
	var priorID uuid.UUID
	var priorRequestSHA, priorResponseSHA string
	priorErr := tx.QueryRow(r.Context(), `SELECT id,request_sha256,response_sha256 FROM recording_presentation_v2_attempts WHERE admission_id=$1 AND idempotency_key=$2 AND account_id=$3 AND node_id=$4`, admissionID, req.IdempotencyKey, p.AccountID, p.NodeID).Scan(&priorID, &priorRequestSHA, &priorResponseSHA)
	if priorErr == nil {
		if priorRequestSHA != requestSHA || priorResponseSHA != presentationV2AttemptResponseSHA(priorID) {
			util.WriteError(w, 409, "presentation attempt differs from committed request")
			return
		}
		if err = tx.Commit(r.Context()); err != nil {
			util.WriteError(w, 500, "commit presentation attempt replay")
			return
		}
		util.WriteJSON(w, 200, map[string]any{"attempt_id": priorID, "replayed": true})
		return
	}
	if !errors.Is(priorErr, pgx.ErrNoRows) {
		util.WriteError(w, 500, "load prior presentation attempt")
		return
	}
	id := uuid.New()
	responseSHA := presentationV2AttemptResponseSHA(id)
	var inserted uuid.UUID
	err = tx.QueryRow(r.Context(), `
		INSERT INTO recording_presentation_v2_attempts(
		 admission_id,account_id,recording_id,stream_id,recording_job_id,lease_token,node_id,idempotency_key,
		 ffmpeg_version,ffprobe_version,libavformat_version,libavcodec_version,libavutil_version,
		 build_flags_sha256,demuxer_name,video_decoder_name,audio_decoder_name,parser_schema,request_sha256,response_sha256,id)
		SELECT a.id,a.account_id,a.recording_id,a.stream_id,a.recording_job_id,a.lease_token,a.node_id,$5,
		 $6,$7,$8,$9,$10,$11,$12,$13,NULLIF($14,''),$15,$17,$18,$19
		FROM recording_presentation_v2_admissions a
		WHERE a.id=$1 AND a.recording_job_id=$2 AND a.lease_token=$3 AND a.node_id=$4 AND a.account_id=$16
		ON CONFLICT(admission_id,idempotency_key) DO NOTHING
		RETURNING id
	`, admissionID, jobID, *lease, p.NodeID, strings.TrimSpace(req.IdempotencyKey), req.FFmpegVersion, req.FFprobeVersion,
		req.Libavformat, req.Libavcodec, req.Libavutil, strings.ToLower(req.BuildFlagsSHA256), req.Demuxer,
		req.VideoDecoder, req.AudioDecoder, req.ParserSchema, p.AccountID, requestSHA, responseSHA, id).Scan(&inserted)
	if errors.Is(err, pgx.ErrNoRows) {
		err = tx.QueryRow(r.Context(), `SELECT id,request_sha256,response_sha256 FROM recording_presentation_v2_attempts WHERE admission_id=$1 AND idempotency_key=$2 AND account_id=$3 AND node_id=$4`, admissionID, req.IdempotencyKey, p.AccountID, p.NodeID).Scan(&id, &priorRequestSHA, &priorResponseSHA)
		if errors.Is(err, pgx.ErrNoRows) {
			util.WriteError(w, 404, "presentation admission unavailable")
			return
		}
		if err != nil || priorRequestSHA != requestSHA || priorResponseSHA != presentationV2AttemptResponseSHA(id) {
			util.WriteError(w, 409, "presentation attempt differs from committed request")
			return
		}
	} else if err == nil {
		id = inserted
	}
	if err != nil {
		util.WriteError(w, 409, "presentation attempt conflicts with frozen admission")
		return
	}
	if err = tx.Commit(r.Context()); err != nil {
		util.WriteError(w, 500, "commit presentation attempt")
		return
	}
	util.WriteJSON(w, 200, map[string]any{"attempt_id": id})
}

func (s *Server) handleRecordingPresentationV2Status(w http.ResponseWriter, r *http.Request) {
	p, id, ok := presentationNodeTask(w, r)
	if !ok {
		return
	}
	var state, retention string
	var revision int64
	var deadline time.Time
	var releaseVersion *int64
	err := s.pool.QueryRow(r.Context(), `SELECT t.state,t.retention_state,t.revision,t.absolute_deadline_at,a.release_version FROM recording_presentation_v2_probe_tasks t LEFT JOIN recording_presentation_v2_release_authorizations a ON a.task_id=t.id WHERE t.id=$1 AND t.account_id=$2 AND t.node_id=$3`, id, p.AccountID, p.NodeID).Scan(&state, &retention, &revision, &deadline, &releaseVersion)
	if errors.Is(err, pgx.ErrNoRows) {
		util.WriteError(w, 404, "presentation task not found")
		return
	}
	if err != nil {
		util.WriteError(w, 500, "load presentation task")
		return
	}
	util.WriteJSON(w, 200, map[string]any{"task_id": id, "state": state, "retention_state": retention, "revision": revision, "absolute_deadline_at": deadline, "release_version": releaseVersion})
}

func (s *Server) handleRecordingPresentationV2Activate(w http.ResponseWriter, r *http.Request) {
	p, id, ok := presentationNodeTask(w, r)
	if !ok {
		return
	}
	var req struct {
		ExpectedRevision        int64  `json:"expected_revision"`
		StagingIdentitySHA256   string `json:"staging_identity_sha256"`
		RetentionIdentitySHA256 string `json:"retention_identity_sha256"`
		Method                  string `json:"method"`
		DeviceID                string `json:"device_id,omitempty"`
		InodeID                 string `json:"inode_id,omitempty"`
		CloneIdentitySHA256     string `json:"clone_identity_sha256,omitempty"`
		FileSizeBytes           int64  `json:"file_size_bytes"`
		FileSHA256              string `json:"file_sha256"`
	}
	if _, ok := decodePresentationV2(w, r, 4096, &req); !ok {
		return
	}
	req.StagingIdentitySHA256 = strings.ToLower(strings.TrimSpace(req.StagingIdentitySHA256))
	req.RetentionIdentitySHA256 = strings.ToLower(strings.TrimSpace(req.RetentionIdentitySHA256))
	req.FileSHA256 = strings.ToLower(strings.TrimSpace(req.FileSHA256))
	req.CloneIdentitySHA256 = strings.ToLower(strings.TrimSpace(req.CloneIdentitySHA256))
	if !lowerHex64(req.StagingIdentitySHA256) || !lowerHex64(req.RetentionIdentitySHA256) || !lowerHex64(req.FileSHA256) || req.FileSizeBytes <= 0 ||
		(req.Method == "hardlink" && (!canonicalUnsignedDecimal(req.DeviceID, true) || !canonicalUnsignedDecimal(req.InodeID, false) || req.CloneIdentitySHA256 != "")) ||
		(req.Method == "clone" && (req.DeviceID != "" || req.InodeID != "" || !lowerHex64(req.CloneIdentitySHA256))) ||
		(req.Method != "hardlink" && req.Method != "clone") {
		util.WriteError(w, 400, "invalid typed presentation retention identity")
		return
	}
	tx, err := s.pool.Begin(r.Context())
	if err != nil {
		util.WriteError(w, 500, "begin presentation retention activation")
		return
	}
	defer tx.Rollback(r.Context())
	var revision int64
	var state, retention, staging, stagingMethod, stagingDevice, stagingInode, stagingClone string
	var retained, retainedMethod, retainedDevice, retainedInode, retainedClone *string
	var clipSize int64
	var clipSHA string
	var deadline, dbNow time.Time
	err = tx.QueryRow(r.Context(), `SELECT revision,state,retention_state,staging_identity_sha256,staging_method,COALESCE(staging_device_id,''),COALESCE(staging_inode_id,''),COALESCE(staging_clone_identity_sha256,''),retention_identity_sha256,retention_method,retention_device_id,retention_inode_id,retention_clone_identity_sha256,clip_size_bytes,clip_sha256,absolute_deadline_at,now() FROM recording_presentation_v2_probe_tasks WHERE id=$1 AND account_id=$2 AND node_id=$3 FOR UPDATE`, id, p.AccountID, p.NodeID).Scan(&revision, &state, &retention, &staging, &stagingMethod, &stagingDevice, &stagingInode, &stagingClone, &retained, &retainedMethod, &retainedDevice, &retainedInode, &retainedClone, &clipSize, &clipSHA, &deadline, &dbNow)
	if err != nil {
		util.WriteError(w, 409, "presentation retention activation conflict")
		return
	}
	identity := presentationV2RetentionIdentity(presentationV2RetentionIdentityInput{TaskID: id, NodeID: p.NodeID, Method: req.Method, DeviceID: req.DeviceID, InodeID: req.InodeID, CloneSHA256: req.CloneIdentitySHA256, SizeBytes: req.FileSizeBytes, FileSHA256: req.FileSHA256, Deadline: deadline})
	exact := staging == req.StagingIdentitySHA256 && stagingMethod == req.Method && stagingDevice == req.DeviceID && stagingInode == req.InodeID && stagingClone == req.CloneIdentitySHA256 && clipSize == req.FileSizeBytes && clipSHA == req.FileSHA256 && identity == req.RetentionIdentitySHA256
	if revision == req.ExpectedRevision+1 && state == "pending" && retention == "active" && exact && retained != nil && *retained == identity && retainedMethod != nil && *retainedMethod == req.Method && valueOrEmpty(retainedDevice) == req.DeviceID && valueOrEmpty(retainedInode) == req.InodeID && valueOrEmpty(retainedClone) == req.CloneIdentitySHA256 {
		if err = tx.Commit(r.Context()); err != nil {
			util.WriteError(w, 409, "presentation retention activation conflict")
			return
		}
		util.WriteJSON(w, 200, map[string]any{"task_id": id, "state": "pending", "retention_state": "active", "revision": revision, "replayed": true})
		return
	}
	if revision != req.ExpectedRevision || state != "awaiting_retention" || retention != "awaiting" || !exact || !deadline.After(dbNow) {
		util.WriteError(w, 409, "presentation retention activation conflict")
		return
	}
	ct, err := tx.Exec(r.Context(), `UPDATE recording_presentation_v2_probe_tasks SET state='pending',retention_state='active',retention_identity_sha256=$2,retention_method=$3,retention_device_id=NULLIF($4,''),retention_inode_id=NULLIF($5,''),retention_clone_identity_sha256=NULLIF($6,''),revision=revision+1 WHERE id=$1`, id, identity, req.Method, req.DeviceID, req.InodeID, req.CloneIdentitySHA256)
	if err != nil || ct.RowsAffected() != 1 {
		util.WriteError(w, 409, "presentation retention activation conflict")
		return
	}
	if err = tx.Commit(r.Context()); err != nil {
		util.WriteError(w, 409, "presentation retention activation conflict")
		return
	}
	util.WriteJSON(w, 200, map[string]any{"task_id": id, "state": "pending", "retention_state": "active", "revision": req.ExpectedRevision + 1})
}

func valueOrEmpty(v *string) string {
	if v == nil {
		return ""
	}
	return *v
}

type presentationV2ClaimedTask struct {
	ID                      uuid.UUID `json:"task_id"`
	ClaimToken              uuid.UUID `json:"claim_token"`
	RetentionIdentitySHA256 string    `json:"retention_identity_sha256"`
	LeaseExpiresAt          time.Time `json:"lease_expires_at"`
	AbsoluteDeadlineAt      time.Time `json:"absolute_deadline_at"`
	ClipID                  int64     `json:"clip_id"`
	ClipSHA256              string    `json:"clip_sha256"`
	ClipSizeBytes           int64     `json:"clip_size_bytes"`
	RequestSHA256           string    `json:"request_sha256"`
}

// claimRecordingPresentationV2 implements the fenced C1 lifecycle for direct
// deterministic tests. No production route calls it in C1; enabling the route
// requires a later reviewed code change, not configuration.
func (s *Server) claimRecordingPresentationV2(ctx context.Context, p nodePrincipal) (*presentationV2ClaimedTask, error) {
	// Row locks and state/claim-token transition triggers are the claim fence.
	// READ COMMITTED plus SKIP LOCKED lets concurrent node polls take distinct
	// eligible rows without a predicate-serialization failure.
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)
	// Cleanup is bounded and never consumes the claim result. Stale rows are
	// prefiltered/locked in small batches; even a much larger backlog cannot
	// starve an eligible task in this same poll.
	type staleTask struct {
		id      uuid.UUID
		binding string
	}
	cleanup := func(query, terminalState, reason string, args ...any) error {
		rows, queryErr := tx.Query(ctx, query, args...)
		if queryErr != nil {
			return queryErr
		}
		stale := make([]staleTask, 0, 16)
		for rows.Next() {
			var item staleTask
			if scanErr := rows.Scan(&item.id, &item.binding); scanErr != nil {
				rows.Close()
				return scanErr
			}
			stale = append(stale, item)
		}
		if rows.Err() != nil {
			rows.Close()
			return rows.Err()
		}
		rows.Close()
		for _, item := range stale {
			if _, updateErr := tx.Exec(ctx, `UPDATE recording_presentation_v2_probe_tasks SET state=$2,retention_state='release_pending',unavailable_reason=NULLIF($3,''),claim_token=NULL,lease_expires_at=NULL,revision=revision+1 WHERE id=$1`, item.id, terminalState, reason); updateErr != nil {
				return updateErr
			}
			if _, insertErr := tx.Exec(ctx, `INSERT INTO recording_presentation_v2_release_authorizations(task_id,release_version,node_id,binding_sha256,terminal_state) VALUES($1,1,$2,$3,$4)`, item.id, p.NodeID, item.binding, terminalState); insertErr != nil {
				return insertErr
			}
		}
		return nil
	}
	if err = cleanup(`SELECT id,COALESCE(retention_identity_sha256,staging_identity_sha256) FROM recording_presentation_v2_probe_tasks WHERE account_id=$1 AND node_id=$2 AND initial_disposition='retained' AND state IN('awaiting_retention','pending','leased') AND absolute_deadline_at<=now() ORDER BY absolute_deadline_at,id LIMIT 16 FOR UPDATE SKIP LOCKED`, "expired", "", p.AccountID, p.NodeID); err != nil {
		return nil, err
	}
	if err = cleanup(`SELECT id,retention_identity_sha256 FROM recording_presentation_v2_probe_tasks WHERE account_id=$1 AND node_id=$2 AND initial_disposition='retained' AND retention_state='active' AND attempt_count>=$3 AND (state='pending' OR (state='leased' AND lease_expires_at<=now())) ORDER BY next_attempt_at,id LIMIT 16 FOR UPDATE SKIP LOCKED`, "unavailable", "probe_unavailable", p.AccountID, p.NodeID, presentationV2MaxAttempts); err != nil {
		return nil, err
	}
	var out presentationV2ClaimedTask
	var priorState string
	err = tx.QueryRow(ctx, `SELECT id,state,retention_identity_sha256,absolute_deadline_at,clip_id,clip_sha256,clip_size_bytes,request_sha256 FROM recording_presentation_v2_probe_tasks WHERE account_id=$1 AND node_id=$2 AND retention_state='active' AND retention_identity_sha256 IS NOT NULL AND next_attempt_at<=now() AND absolute_deadline_at>now()+$3::interval AND attempt_count<$4 AND (state='pending' OR (state='leased' AND lease_expires_at<=now())) ORDER BY next_attempt_at,id LIMIT 1 FOR UPDATE SKIP LOCKED`, p.AccountID, p.NodeID, presentationV2CompletionMargin.String(), presentationV2MaxAttempts).Scan(&out.ID, &priorState, &out.RetentionIdentitySHA256, &out.AbsoluteDeadlineAt, &out.ClipID, &out.ClipSHA256, &out.ClipSizeBytes, &out.RequestSHA256)
	if errors.Is(err, pgx.ErrNoRows) {
		_ = tx.Commit(ctx)
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if priorState == "leased" {
		if _, err = tx.Exec(ctx, `UPDATE recording_presentation_v2_probe_tasks SET state='pending',claim_token=NULL,lease_expires_at=NULL,revision=revision+1 WHERE id=$1`, out.ID); err != nil {
			return nil, err
		}
	}
	out.ClaimToken = uuid.New()
	err = tx.QueryRow(ctx, `UPDATE recording_presentation_v2_probe_tasks SET state='leased',claim_token=$2,lease_expires_at=LEAST(now()+$3::interval,absolute_deadline_at-$4::interval),attempt_count=attempt_count+1,revision=revision+1 WHERE id=$1 RETURNING lease_expires_at`, out.ID, out.ClaimToken, presentationV2ClaimLease.String(), presentationV2CompletionMargin.String()).Scan(&out.LeaseExpiresAt)
	if err != nil {
		return nil, err
	}
	if err = tx.Commit(ctx); err != nil {
		return nil, err
	}
	return &out, nil
}

func (s *Server) handleRecordingPresentationV2Claim(w http.ResponseWriter, r *http.Request) {
	p, ok := nodePrincipalFromContext(r.Context())
	if !ok || p.NodeType != nodeTypeRelay {
		util.WriteError(w, 403, "relay node required")
		return
	}
	// C1 is structurally disabled. There is intentionally no config knob.
	util.WriteJSON(w, 200, map[string]any{"enabled": false, "task": nil})
}

func (s *Server) handleRecordingPresentationV2Retry(w http.ResponseWriter, r *http.Request) {
	p, id, ok := presentationNodeTask(w, r)
	if !ok {
		return
	}
	var req struct {
		ClaimToken string `json:"claim_token"`
	}
	if _, ok := decodePresentationV2(w, r, 1024, &req); !ok {
		return
	}
	token, err := uuid.Parse(req.ClaimToken)
	if err != nil {
		util.WriteError(w, 400, "invalid claim token")
		return
	}
	ct, err := s.pool.Exec(r.Context(), `UPDATE recording_presentation_v2_probe_tasks SET state='pending',claim_token=NULL,lease_expires_at=NULL,next_attempt_at=now()+make_interval(secs=>LEAST(300,5*power(2,LEAST(attempt_count,5))::integer)),revision=revision+1 WHERE id=$1 AND account_id=$2 AND node_id=$3 AND state='leased' AND claim_token=$4 AND lease_expires_at>now() AND absolute_deadline_at>now()+$5::interval`, id, p.AccountID, p.NodeID, token, presentationV2CompletionMargin.String())
	if err != nil || ct.RowsAffected() != 1 {
		var replayed bool
		_ = s.pool.QueryRow(r.Context(), `SELECT EXISTS(SELECT 1 FROM recording_presentation_v2_task_events e JOIN recording_presentation_v2_probe_tasks t ON t.id=e.task_id WHERE e.task_id=$1 AND t.account_id=$2 AND t.node_id=$3 AND e.from_state='leased' AND e.to_state='pending' AND e.claim_token=$4 AND t.state='pending')`, id, p.AccountID, p.NodeID, token).Scan(&replayed)
		if replayed {
			util.WriteJSON(w, 200, map[string]any{"task_id": id, "state": "pending", "replayed": true})
			return
		}
		util.WriteError(w, 409, "presentation retry fence rejected")
		return
	}
	util.WriteJSON(w, 200, map[string]any{"task_id": id, "state": "pending"})
}

func (s *Server) handleRecordingPresentationV2Complete(w http.ResponseWriter, r *http.Request) {
	p, id, ok := presentationNodeTask(w, r)
	if !ok {
		return
	}
	var req presentationV2Completion
	raw, ok := decodePresentationV2(w, r, 4<<20, &req)
	if !ok {
		return
	}
	token, err := uuid.Parse(req.ClaimToken)
	if err != nil {
		util.WriteError(w, 400, "invalid claim token")
		return
	}
	reportSHA := presentationSHA(raw)
	tx, err := s.pool.BeginTx(r.Context(), pgx.TxOptions{IsoLevel: pgx.Serializable})
	if err != nil {
		util.WriteError(w, 500, "begin presentation completion")
		return
	}
	defer tx.Rollback(r.Context())
	var priorID int64
	var priorSHA string
	priorErr := tx.QueryRow(r.Context(), `SELECT id,report_sha256 FROM recording_presentation_v2_authored_facts WHERE task_id=$1 AND account_id=$2 AND node_id=$3 AND claim_token=$4`, id, p.AccountID, p.NodeID, token).Scan(&priorID, &priorSHA)
	if priorErr == nil {
		if priorSHA != reportSHA {
			util.WriteError(w, 409, "presentation completion differs from prior result")
			return
		}
		_ = tx.Commit(r.Context())
		util.WriteJSON(w, 200, map[string]any{"task_id": id, "fact_id": priorID, "replayed": true})
		return
	}
	if !errors.Is(priorErr, pgx.ErrNoRows) {
		util.WriteError(w, 500, "load prior presentation completion")
		return
	}
	var requestSHA, binding string
	var dbNow, lease, deadline time.Time
	err = tx.QueryRow(r.Context(), `SELECT request_sha256,retention_identity_sha256,now(),lease_expires_at,absolute_deadline_at FROM recording_presentation_v2_probe_tasks WHERE id=$1 AND account_id=$2 AND node_id=$3 AND state='leased' AND retention_state='active' AND claim_token=$4 FOR UPDATE`, id, p.AccountID, p.NodeID, token).Scan(&requestSHA, &binding, &dbNow, &lease, &deadline)
	if err != nil || lease.Before(dbNow) || deadline.Before(dbNow.Add(presentationV2CompletionMargin)) {
		util.WriteError(w, 409, "presentation completion lease expired")
		return
	}
	if req.RequestSHA256 != requestSHA || len(req.Axes) != 6 {
		util.WriteError(w, 409, "presentation completion identity or axes mismatch")
		return
	}
	var factID int64
	err = tx.QueryRow(r.Context(), `INSERT INTO recording_presentation_v2_authored_facts(task_id,account_id,node_id,claim_token,request_sha256,report_sha256,authored_status,terminal_reason,completed_at) VALUES($1,$2,$3,$4,$5,$6,$7,$8,now()) RETURNING id`, id, p.AccountID, p.NodeID, token, requestSHA, reportSHA, req.AuthoredStatus, req.TerminalReason).Scan(&factID)
	if err != nil {
		util.WriteError(w, 409, "invalid presentation fact header")
		return
	}
	for _, a := range req.Axes {
		_, err = tx.Exec(r.Context(), `INSERT INTO recording_presentation_v2_fact_axes(fact_id,axis,status,reason,stream_index,unit_count,canonical_sha256,time_base_num,time_base_den,first_ordinal,first_timestamp,end_ordinal,end_timestamp,nonmonotonic_count,duplicate_count,hole_count,overlap_count,sample_rate,channel_count,channel_layout,normalization_profile,edit_list_sha256,edit_list_kind,skip_samples,discard_padding,codec_delay,initial_padding,trailing_padding) VALUES($1,$2,$3,NULLIF($4,''),$5,$6,NULLIF($7,''),$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,NULLIF($20,''),NULLIF($21,''),NULLIF($22,''),NULLIF($23,''),$24,$25,$26,$27,$28)`, factID, a.Axis, a.Status, a.Reason, a.StreamIndex, a.UnitCount, a.CanonicalSHA256, a.TimeBaseNum, a.TimeBaseDen, a.FirstOrdinal, a.FirstTimestamp, a.EndOrdinal, a.EndTimestamp, a.NonmonotonicCount, a.DuplicateCount, a.HoleCount, a.OverlapCount, a.SampleRate, a.ChannelCount, a.ChannelLayout, a.NormalizationProfile, a.EditListSHA256, a.EditListKind, a.SkipSamples, a.DiscardPadding, a.CodecDelay, a.InitialPadding, a.TrailingPadding)
		if err != nil {
			util.WriteError(w, 409, "invalid presentation axis")
			return
		}
		for _, e := range a.PacketEdges {
			_, err = tx.Exec(r.Context(), `INSERT INTO recording_presentation_v2_packet_edges VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)`, factID, a.Axis, e.Side, e.Rank, e.Ordinal, e.PTS, e.DTS, e.Duration, e.TimeBaseNum, e.TimeBaseDen, e.Flags, e.SideDataSHA256, e.PayloadSHA256)
			if err != nil {
				util.WriteError(w, 409, "invalid packet edge")
				return
			}
		}
		for _, e := range a.RawExtents {
			_, err = tx.Exec(r.Context(), `INSERT INTO recording_presentation_v2_raw_extents VALUES($1,$2,$3,$4,$5,$6,$7,$8)`, factID, a.Axis, e.Side, e.Rank, e.Ordinal, e.Position, e.SizeBytes, e.SHA256)
			if err != nil {
				util.WriteError(w, 409, "invalid raw extent")
				return
			}
		}
		for _, e := range a.VideoFrames {
			_, err = tx.Exec(r.Context(), `INSERT INTO recording_presentation_v2_video_frame_edges(fact_id,edge_side,edge_rank,presentation_ordinal,pts,duration,time_base_num,time_base_den,pixel_sha256) VALUES($1,$2,$3,$4,$5,$6,$7,$8,NULLIF($9,''))`, factID, e.Side, e.Rank, e.Ordinal, e.PTS, e.Duration, e.TimeBaseNum, e.TimeBaseDen, e.PixelSHA256)
			if err != nil {
				util.WriteError(w, 409, "invalid video edge")
				return
			}
		}
		for _, e := range a.AudioBlocks {
			_, err = tx.Exec(r.Context(), `INSERT INTO recording_presentation_v2_audio_block_edges(fact_id,edge_side,edge_rank,block_ordinal,pts,sample_count,time_base_num,time_base_den,sample_rate,channel_count,channel_layout,pcm_sha256) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,NULLIF($12,''))`, factID, e.Side, e.Rank, e.Ordinal, e.PTS, e.SampleCount, e.TimeBaseNum, e.TimeBaseDen, e.SampleRate, e.ChannelCount, e.ChannelLayout, e.PCMSHA256)
			if err != nil {
				util.WriteError(w, 409, "invalid audio edge")
				return
			}
		}
	}
	if _, err = tx.Exec(r.Context(), `UPDATE recording_presentation_v2_probe_tasks SET state='completed',retention_state='release_pending',terminal_claim_token=claim_token,claim_token=NULL,lease_expires_at=NULL,revision=revision+1 WHERE id=$1`, id); err != nil {
		util.WriteError(w, 409, "complete presentation task")
		return
	}
	if _, err = tx.Exec(r.Context(), `INSERT INTO recording_presentation_v2_release_authorizations(task_id,release_version,node_id,binding_sha256,terminal_state) VALUES($1,1,$2,$3,'completed')`, id, p.NodeID, binding); err != nil {
		util.WriteError(w, 409, "authorize presentation release")
		return
	}
	if err = tx.Commit(r.Context()); err != nil {
		util.WriteError(w, 409, "presentation completion failed coherence validation")
		return
	}
	util.WriteJSON(w, 200, map[string]any{"task_id": id, "fact_id": factID, "release_version": 1})
}

func (s *Server) handleRecordingPresentationV2Unavailable(w http.ResponseWriter, r *http.Request) {
	p, id, ok := presentationNodeTask(w, r)
	if !ok {
		return
	}
	var req struct {
		ClaimToken string `json:"claim_token,omitempty"`
		Reason     string `json:"reason"`
	}
	if _, ok := decodePresentationV2(w, r, 1024, &req); !ok {
		return
	}
	if !presentationRuntimeUnavailableReason(req.Reason) {
		util.WriteError(w, 400, "presentation unavailable reason is invalid")
		return
	}
	tx, err := s.pool.BeginTx(r.Context(), pgx.TxOptions{IsoLevel: pgx.Serializable})
	if err != nil {
		util.WriteError(w, 500, "begin presentation unavailable")
		return
	}
	defer tx.Rollback(r.Context())
	var state, retention, binding, priorReason string
	var currentToken *uuid.UUID
	err = tx.QueryRow(r.Context(), `SELECT state,retention_state,COALESCE(retention_identity_sha256,staging_identity_sha256,''),COALESCE(unavailable_reason,''),claim_token FROM recording_presentation_v2_probe_tasks WHERE id=$1 AND account_id=$2 AND node_id=$3 FOR UPDATE`, id, p.AccountID, p.NodeID).Scan(&state, &retention, &binding, &priorReason, &currentToken)
	if err != nil {
		util.WriteError(w, 404, "presentation task not found")
		return
	}
	if state == "unavailable" && retention == "none" {
		if priorReason != req.Reason {
			util.WriteError(w, 409, "presentation unavailable differs from committed result")
			return
		}
		_ = tx.Commit(r.Context())
		util.WriteJSON(w, 200, map[string]any{"task_id": id, "state": state})
		return
	}
	if state == "unavailable" && (retention == "release_pending" || retention == "released") {
		if priorReason != req.Reason {
			util.WriteError(w, 409, "presentation unavailable differs from committed result")
			return
		}
		_ = tx.Commit(r.Context())
		util.WriteJSON(w, 200, map[string]any{"task_id": id, "state": state, "release_version": 1, "replayed": true})
		return
	}
	if state == "leased" {
		token, e := uuid.Parse(req.ClaimToken)
		if e != nil || currentToken == nil || *currentToken != token {
			util.WriteError(w, 409, "presentation unavailable claim mismatch")
			return
		}
	}
	if _, err = tx.Exec(r.Context(), `UPDATE recording_presentation_v2_probe_tasks SET state='unavailable',retention_state='release_pending',unavailable_reason=$2,claim_token=NULL,lease_expires_at=NULL,revision=revision+1 WHERE id=$1`, id, req.Reason); err != nil {
		util.WriteError(w, 409, "presentation unavailable transition rejected")
		return
	}
	if _, err = tx.Exec(r.Context(), `INSERT INTO recording_presentation_v2_release_authorizations(task_id,release_version,node_id,binding_sha256,terminal_state) VALUES($1,1,$2,$3,'unavailable')`, id, p.NodeID, binding); err != nil {
		util.WriteError(w, 409, "presentation release authorization rejected")
		return
	}
	if err = tx.Commit(r.Context()); err != nil {
		util.WriteError(w, 409, "presentation unavailable commit rejected")
		return
	}
	util.WriteJSON(w, 200, map[string]any{"task_id": id, "state": "unavailable", "release_version": 1})
}

func (s *Server) handleRecordingPresentationV2ReleaseAck(w http.ResponseWriter, r *http.Request) {
	p, id, ok := presentationNodeTask(w, r)
	if !ok {
		return
	}
	var req struct {
		ReleaseVersion int64  `json:"release_version"`
		BindingSHA256  string `json:"binding_sha256"`
	}
	if _, ok := decodePresentationV2(w, r, 1024, &req); !ok {
		return
	}
	// The release authorization row and deferred task validator provide the
	// fence. READ COMMITTED lets concurrent byte-identical ACKs converge after
	// the first commit instead of turning an idempotent replay into 40001.
	tx, err := s.pool.BeginTx(r.Context(), pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		util.WriteError(w, 500, "begin release ack")
		return
	}
	defer tx.Rollback(r.Context())
	ct, err := tx.Exec(r.Context(), `INSERT INTO recording_presentation_v2_release_acknowledgements(task_id,release_version,node_id,binding_sha256) SELECT task_id,release_version,node_id,binding_sha256 FROM recording_presentation_v2_release_authorizations WHERE task_id=$1 AND release_version=$2 AND node_id=$3 AND binding_sha256=$4 ON CONFLICT(task_id) DO NOTHING`, id, req.ReleaseVersion, p.NodeID, strings.ToLower(req.BindingSHA256))
	if err != nil {
		util.WriteError(w, 409, "release acknowledgement mismatch")
		return
	}
	if ct.RowsAffected() == 0 {
		var same bool
		_ = tx.QueryRow(r.Context(), `SELECT EXISTS(SELECT 1 FROM recording_presentation_v2_release_acknowledgements WHERE task_id=$1 AND release_version=$2 AND node_id=$3 AND binding_sha256=$4)`, id, req.ReleaseVersion, p.NodeID, strings.ToLower(req.BindingSHA256)).Scan(&same)
		if !same {
			util.WriteError(w, 409, "release acknowledgement differs")
			return
		}
	}
	ct, err = tx.Exec(r.Context(), `UPDATE recording_presentation_v2_probe_tasks SET retention_state='released',revision=revision+1 WHERE id=$1 AND account_id=$2 AND node_id=$3 AND retention_state='release_pending'`, id, p.AccountID, p.NodeID)
	if err != nil {
		util.WriteError(w, 409, "release transition rejected")
		return
	}
	if ct.RowsAffected() == 0 {
		var released bool
		_ = tx.QueryRow(r.Context(), `SELECT retention_state='released' FROM recording_presentation_v2_probe_tasks WHERE id=$1 AND account_id=$2 AND node_id=$3`, id, p.AccountID, p.NodeID).Scan(&released)
		if !released {
			util.WriteError(w, 409, "release task unavailable")
			return
		}
	}
	if err = tx.Commit(r.Context()); err != nil {
		util.WriteError(w, 409, "release acknowledgement commit rejected")
		return
	}
	util.WriteJSON(w, 200, map[string]any{"task_id": id, "released": true})
}

func presentationV2ResponseSHA(taskID uuid.UUID, clipID int64, state string) string {
	return presentationSHA([]byte(fmt.Sprintf("task:%s:%d:%s", taskID, clipID, state)))
}

func presentationV2AttemptResponseSHA(attemptID uuid.UUID) string {
	return presentationSHA([]byte("attempt:" + attemptID.String()))
}

func nullableText(s string) any {
	if strings.TrimSpace(s) == "" {
		return nil
	}
	return strings.TrimSpace(s)
}
