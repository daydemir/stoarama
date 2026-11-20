package r2

import (
	"context"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"net/http"
	"regexp"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/jackc/pgx/v5"
)

const StitchStagePartBytes = 16 << 20

type StitchStageLease struct {
	HourID, APIKeyID          int64
	Attempt                   int
	Token, PlanSHA, Toolchain string
}

type StitchStageUpload struct {
	ID               int64  `json:"id"`
	Size             int64  `json:"size_bytes"`
	State            string `json:"state"`
	PartBytes        int64  `json:"part_bytes"`
	key, upload, sha string
}

func (c *Client) stitchStageWriter(ctx context.Context, tx pgx.Tx, lease StitchStageLease, id int64) (StitchStageUpload, time.Duration, error) {
	var o StitchStageUpload
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock_shared(7343226011187)`); err != nil {
		return o, 0, err
	}
	err := tx.QueryRow(ctx, `SELECT o.id,o.size_bytes,o.state,o.object_key,coalesce(o.upload_id,''),o.sha256
 FROM nas_stitch_r2_stage_objects o JOIN nas_stitch_r2_hours h ON h.id=o.hour_id
 JOIN nas_stitch_scopes s ON s.id=h.scope_id JOIN account_api_keys k ON k.id=o.api_key_id
 CROSS JOIN nas_stitch_r2_control control JOIN r2_retirement_writer_fences f
 ON f.bucket=o.bucket AND f.endpoint_sha256=o.endpoint_sha256
 WHERE o.id=$1 AND o.hour_id=$2 AND o.attempt=$3 AND o.api_key_id=$4 AND h.attempt=o.attempt
  AND h.lease_api_key_id=o.api_key_id AND h.lease_token=$5 AND h.plan_sha256=$6
  AND o.bucket=$7 AND o.endpoint_sha256=$8 AND h.lease_toolchain_sha256=$9 AND h.lease_toolchain_attempt=h.attempt
 FOR UPDATE OF o FOR SHARE OF h,s,k,control,f`, id, lease.HourID, lease.Attempt, lease.APIKeyID,
		lease.Token, lease.PlanSHA, c.bucket, c.EndpointSHA256(), lease.Toolchain).Scan(&o.ID, &o.Size, &o.State, &o.key, &o.upload, &o.sha)
	if err != nil {
		return o, 0, err
	}
	var expires, now time.Time
	err = tx.QueryRow(ctx, `SELECT least(h.lease_expires_at,s.approved_until,coalesce(k.expires_at,'infinity')),clock_timestamp()
 FROM nas_stitch_r2_hours h JOIN nas_stitch_scopes s ON s.id=h.scope_id
 JOIN account_api_keys k ON k.id=h.lease_api_key_id CROSS JOIN nas_stitch_r2_control control
 WHERE h.id=$1 AND h.state='leased' AND NOT s.paused AND k.revoked_at IS NULL AND control.enabled
  AND NOT EXISTS(SELECT 1 FROM nas_cleanup_frozen_streams f WHERE f.stream_id=s.stream_id)`, lease.HourID).Scan(&expires, &now)
	ttl := min(expires.Sub(now).Truncate(time.Second), 3*time.Minute)
	if err != nil || ttl < time.Second {
		return o, 0, errors.New("stitch stage lease is inactive")
	}
	o.PartBytes = StitchStagePartBytes
	return o, ttl, nil
}

func (c *Client) StartStitchStage(ctx context.Context, lease StitchStageLease, id int64) (StitchStageUpload, error) {
	var o StitchStageUpload
	if c.writerDB == nil {
		return o, errors.New("stitch staging requires a durable writer ledger")
	}
	tx, err := c.writerDB.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return o, err
	}
	defer tx.Rollback(ctx)
	o, _, err = c.stitchStageWriter(ctx, tx, lease, id)
	if err != nil {
		return o, err
	}
	if o.State == "uploading" || o.State == "completing" || o.State == "staged" {
		return o, tx.Commit(ctx)
	}
	tag, err := tx.Exec(ctx, `UPDATE nas_stitch_r2_stage_objects SET start_issued_at=clock_timestamp()
 WHERE id=$1 AND state='starting' AND start_issued_at IS NULL`, id)
	if err != nil || tag.RowsAffected() != 1 {
		return o, errors.New("stitch multipart start already issued; reconcile before retry")
	}
	if err := tx.Commit(ctx); err != nil {
		return o, err
	}
	created, err := c.s3.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
		Bucket: aws.String(c.bucket), Key: aws.String(o.key), ContentType: aws.String("video/mp4"),
		Metadata: map[string]string{"sha256": o.sha}, ChecksumAlgorithm: types.ChecksumAlgorithmSha256,
	}, noStitchStageRetries)
	if err != nil || aws.ToString(created.UploadId) == "" {
		return o, errors.New("stitch multipart start unconfirmed; reconcile before retry")
	}
	tag, err = c.writerDB.Exec(ctx, `UPDATE nas_stitch_r2_stage_objects SET state='uploading',upload_id=$2
 WHERE id=$1 AND state='starting' AND start_issued_at IS NOT NULL AND upload_id IS NULL`, id, *created.UploadId)
	if err != nil || tag.RowsAffected() != 1 {
		return o, errors.New("stitch multipart upload ID not durable; reconcile before retry")
	}
	o.State = "uploading"
	return o, nil
}

func noStitchStageRetries(o *s3.Options) { o.Retryer = aws.NopRetryer{} }

func (c *Client) PresignStitchStagePart(ctx context.Context, lease StitchStageLease, id int64, number int32, sha string) (PresignedRequest, error) {
	if c.writerDB == nil {
		return PresignedRequest{}, errors.New("stitch staging requires a durable writer ledger")
	}
	digest, err := hex.DecodeString(sha)
	if err != nil || len(digest) != 32 || hex.EncodeToString(digest) != sha {
		return PresignedRequest{}, errors.New("stitch part requires its exact SHA-256")
	}
	tx, err := c.writerDB.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return PresignedRequest{}, err
	}
	defer tx.Rollback(ctx)
	o, ttl, err := c.stitchStageWriter(ctx, tx, lease, id)
	if err != nil {
		return PresignedRequest{}, err
	}
	offset := (int64(number) - 1) * StitchStagePartBytes
	if o.State != "uploading" || offset < 0 || offset >= o.Size {
		return PresignedRequest{}, errors.New("stitch part outside active upload")
	}
	if _, err = tx.Exec(ctx, `INSERT INTO nas_stitch_r2_parts(object_id,part_number,sha256)
 VALUES($1,$2,$3) ON CONFLICT DO NOTHING`, o.ID, number, sha); err != nil {
		return PresignedRequest{}, err
	}
	var frozen string
	if err = tx.QueryRow(ctx, `SELECT sha256 FROM nas_stitch_r2_parts WHERE object_id=$1 AND part_number=$2`,
		o.ID, number).Scan(&frozen); err != nil || frozen != sha {
		return PresignedRequest{}, errors.New("stitch part digest differs")
	}
	checksum := base64.StdEncoding.EncodeToString(digest)
	length := min(StitchStagePartBytes, o.Size-offset)
	cap, err := c.presignStitchPart(ctx, o, number, checksum, length, ttl)
	if err != nil {
		return PresignedRequest{}, err
	}
	if _, err = tx.Exec(ctx, `UPDATE nas_stitch_r2_stage_objects SET grants_drain_at=greatest(grants_drain_at,
 clock_timestamp()+$2::bigint*interval '1 second'+interval '2 minutes') WHERE id=$1`, o.ID, int64(ttl/time.Second)); err != nil {
		return PresignedRequest{}, err
	}
	if err = tx.Commit(ctx); err != nil {
		return PresignedRequest{}, err
	}
	return cap, nil
}

func (c *Client) presignStitchPart(ctx context.Context, o StitchStageUpload, number int32, checksum string, length int64, ttl time.Duration) (PresignedRequest, error) {
	signed, err := c.presigner.PresignUploadPart(ctx, &s3.UploadPartInput{
		Bucket: aws.String(c.bucket), Key: aws.String(o.key), UploadId: aws.String(o.upload),
		PartNumber: &number, ContentLength: &length, ChecksumSHA256: &checksum,
	}, s3.WithPresignExpires(ttl), func(options *s3.PresignOptions) {
		options.Presigner = v4.NewSigner(func(sign *v4.SignerOptions) {
			sign.DisableHeaderHoisting = true
		})
	})
	if err != nil {
		return PresignedRequest{}, err
	}
	if signed.SignedHeader.Get("x-amz-checksum-sha256") != checksum || signed.SignedHeader.Get("Content-Length") != fmt.Sprint(length) {
		return PresignedRequest{}, errors.New("stitch part signature omitted its length or checksum")
	}
	return PresignedRequest{URL: signed.URL, Method: signed.Method, Headers: signed.SignedHeader.Clone()}, nil
}

// FinishStitchStage completes exactly the frozen parts. An uncertain provider
// result leaves the row completing; subsequent calls reconcile by HEAD.
func (c *Client) FinishStitchStage(ctx context.Context, lease StitchStageLease, id int64) error {
	if c.writerDB == nil {
		return errors.New("stitch staging requires a durable writer ledger")
	}
	tx, err := c.writerDB.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	o, _, err := c.stitchStageWriter(ctx, tx, lease, id)
	if err != nil {
		return err
	}
	switch o.State {
	case "staged":
		return tx.Commit(ctx)
	case "completing":
		if err := tx.Commit(ctx); err != nil {
			return err
		}
		return c.recordStitchStage(ctx, o)
	case "uploading":
	default:
		return errors.New("stitch stage is not uploadable")
	}
	parts, err := c.stitchStageCompletedParts(ctx, tx, o)
	if err != nil {
		return err
	}
	if _, err = tx.Exec(ctx, `UPDATE nas_stitch_r2_stage_objects SET state='completing' WHERE id=$1`, id); err != nil {
		return err
	}
	if err = tx.Commit(ctx); err != nil {
		return err
	}
	_, err = c.s3.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
		Bucket: aws.String(c.bucket), Key: aws.String(o.key), UploadId: aws.String(o.upload),
		IfNoneMatch: aws.String("*"), MultipartUpload: &types.CompletedMultipartUpload{Parts: parts},
	}, noStitchStageRetries)
	if err != nil {
		return errors.New("stitch multipart completion unconfirmed; reconcile before retry")
	}
	return c.recordStitchStage(ctx, o)
}

func (c *Client) stitchStageCompletedParts(ctx context.Context, tx pgx.Tx, o StitchStageUpload) ([]types.CompletedPart, error) {
	rows, err := tx.Query(ctx, `SELECT part_number,sha256 FROM nas_stitch_r2_parts
 WHERE object_id=$1 ORDER BY part_number`, o.ID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	expected := make([]string, 0, (o.Size+StitchStagePartBytes-1)/StitchStagePartBytes)
	for rows.Next() {
		var number int
		var sha string
		if err := rows.Scan(&number, &sha); err != nil {
			return nil, err
		}
		if number != len(expected)+1 {
			return nil, errors.New("stitch part ledger has a gap")
		}
		digest, err := hex.DecodeString(sha)
		if err != nil || len(digest) != 32 {
			return nil, errors.New("stitch part ledger digest differs")
		}
		expected = append(expected, base64.StdEncoding.EncodeToString(digest))
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if int64(len(expected)) != (o.Size+StitchStagePartBytes-1)/StitchStagePartBytes {
		return nil, errors.New("stitch part ledger is incomplete")
	}
	actual, err := c.s3.ListParts(ctx, &s3.ListPartsInput{
		Bucket: aws.String(c.bucket), Key: aws.String(o.key), UploadId: aws.String(o.upload),
		MaxParts: aws.Int32(1000),
	})
	if err != nil || actual.IsTruncated == nil || *actual.IsTruncated || len(actual.Parts) != len(expected) {
		return nil, errors.New("stitch provider part inventory incomplete")
	}
	completion := make([]types.CompletedPart, 0, len(expected))
	for i, part := range actual.Parts {
		length := min(StitchStagePartBytes, o.Size-int64(i)*StitchStagePartBytes)
		if aws.ToInt32(part.PartNumber) != int32(i+1) || aws.ToInt64(part.Size) != length ||
			aws.ToString(part.ChecksumSHA256) != expected[i] || aws.ToString(part.ETag) == "" {
			return nil, errors.New("stitch provider part identity differs")
		}
		completion = append(completion, types.CompletedPart{
			ETag: part.ETag, PartNumber: part.PartNumber, ChecksumSHA256: part.ChecksumSHA256,
		})
	}
	return completion, nil
}

func (c *Client) recordStitchStage(ctx context.Context, o StitchStageUpload) error {
	head, err := c.Head(ctx, o.key)
	if err != nil || head.ETag == "" || head.LastModified.IsZero() ||
		head.SizeBytes != o.Size || head.SHA256Metadata != o.sha {
		return errors.New("stitch staged object identity unavailable")
	}
	tag, err := c.writerDB.Exec(ctx, `UPDATE nas_stitch_r2_stage_objects
 SET state='staged',etag=$2,version_id=nullif($3,''),last_modified=$4
 WHERE id=$1 AND state='completing' AND upload_id=$5`, o.ID, head.ETag, head.VersionID, head.LastModified, o.upload)
	if err != nil || tag.RowsAffected() != 1 {
		return errors.New("stitch staged object identity not durable")
	}
	return nil
}

// StitchStageTarget is the exact immutable provider generation a stage
// retirement binds. Every field must equal the certified stage row.
type StitchStageTarget struct {
	Bucket string
	// EndpointSHA256 is the exact provider endpoint this generation lives at,
	// taken from the committed stage row rather than from process config. The
	// wire must re-verify it against its own credential, so evidence can never
	// be bound to one endpoint and deleted at another.
	EndpointSHA256 string
	Key            string
	ETag           string
	SHA256         string
	SizeBytes      int64
	LastModified   time.Time
	VersionID      string
}

// StitchStageExternalWriterRace is the only accepted residual premise. It is a
// fixed identifier, not free text and not an acceptance: it records that a
// holder of the account R2 credential outside every 0187-governed writer can
// still race this key. Proving or accepting that is separate rollout evidence
// for the controller and owner, and this value grants nothing by itself.
const StitchStageExternalWriterRace = "external_writer_race_not_proven_v1"

// StitchStageEvidence is the reviewed, independently pinned per-stage
// retirement authority. It binds one stage object to its certified hour,
// attempt and manifest, and to the live fence activation it was reviewed
// against, so it cannot be reused for another key, attempt or fence.
//
// It is a premise for exactly one stage key. It carries no RAW or keep28
// authority, makes no provider-safety claim, and has no expiry: the historical
// unsupported-conditional-DELETE receipt is a design premise, never an
// affirmative safety result, so nothing about it must be probed again.
type StitchStageEvidence struct {
	Version                int       `json:"version"`
	Bucket                 string    `json:"bucket"`
	EndpointSHA256         string    `json:"endpoint_sha256"`
	ObjectID               int64     `json:"object_id"`
	ObjectKey              string    `json:"object_key"`
	SHA256                 string    `json:"sha256"`
	SizeBytes              int64     `json:"size_bytes"`
	HourID                 int64     `json:"hour_id"`
	Attempt                int       `json:"attempt"`
	PlanSHA256             string    `json:"plan_sha256"`
	ManifestSHA256         string    `json:"manifest_sha256"`
	FenceActivatedAt       time.Time `json:"fence_activated_at"`
	DrainNotBefore         time.Time `json:"drain_not_before"`
	DeployedWriterCoverage string    `json:"deployed_writer_coverage_evidence"`
	ExternalWriterRace     string    `json:"external_writer_race"`
	// PicksSHA256 pins the reviewed owner picks this evidence was derived from.
	// The retirement never trusts any stream set written here: it re-derives the
	// decided and keep-everything sets from those pinned picks and requires the
	// reviewed record below to equal that derivation exactly, so an evidence
	// document cannot name a stream the picks never decided.
	PicksSHA256      string  `json:"picks_sha256"`
	KEStreamIDs      []int64 `json:"ke_stream_ids"`
	DecidedStreamIDs []int64 `json:"decided_stream_ids"`
}

// StitchStageWire is the audited single-attempt transport. The caller supplies
// it so the first-byte guard and the one-DELETE transport are the same code the
// RAW lanes use. recheck runs at the transport before the first request byte,
// while this retirement still holds its locks; the confirming HEAD and the
// strict absence readback stay in this package.
type StitchStageWire interface {
	// EndpointSHA256 is the wire's own provider endpoint. The retirement refuses
	// any wire whose endpoint differs from the committed stage row's endpoint.
	EndpointSHA256() string
	// ExactHead is one direct no-redirect HEAD of this exact generation.
	ExactHead(context.Context, StitchStageTarget, time.Time) (ObjectHead, error)
	// DeleteOnce sends at most one DELETE under deadline. A lost or unknown
	// answer returns a non-nil error, which leaves the intent unresolved.
	DeleteOnce(context.Context, StitchStageTarget, time.Time, func(context.Context) error) error
}

// StitchStagePicks is the exact stream decision set derived in the command layer
// from one independently pinned owner picks file. The retirement never accepts
// these sets from the evidence document: an evidence claim that names a stream
// the pinned picks never decided must not become authority.
type StitchStagePicks struct {
	SHA256 string
	// Decided is every stream the pinned picks decided against the embedded
	// policy window, ascending. KE is the decided `all` subset.
	Decided []int64
	KE      []int64
}

// A stage retirement's database steps and its wire step have separate bounds,
// so lock waits never spend the wire budget and a slow confirm never cancels a
// database step. Nothing retries: both a lost DELETE and a failed step leave
// the one-shot intent held.
const (
	stitchStageDBTimeout   = 2 * time.Minute
	stitchStageWireTimeout = 30 * time.Second
	stitchStageLockTimeout = 5 * time.Second
)

// stitchStageKeyPrefix is the only provider namespace a stage retirement may
// delete in. RAW, keep28, delivery and every other prefix are unreachable here.
const stitchStageKeyPrefix = "stitch-staging/"

var stitchStageKeyShape = regexp.MustCompile(`^stitch-staging/[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}/[0-9a-f]{64}\.mp4$`)

// ValidStitchStageKey reports whether key is exactly one stage generation's
// key for sha: the only namespace a stage retirement may ever delete in.
func ValidStitchStageKey(key, sha string) bool {
	return stitchStageKeyShape.MatchString(key) && strings.HasSuffix(key, "/"+sha+".mp4")
}

// RetireStitchStage is an operator-only one-shot retirement of an isolated
// stage generation. It never reissues, never edits and never releases a hold: a
// previously issued delete can only be reconciled by observed absence.
//
// picks is the exact decision set the command layer derived from the one
// independently pinned owner picks file. It is supplied separately from ev so
// the evidence document's own stream lists are only ever compared against it,
// never treated as the decision.
func (c *Client) RetireStitchStage(ctx context.Context, objectID int64, sha string, ev StitchStageEvidence, picks StitchStagePicks, wire StitchStageWire, reconcile bool) error {
	if c.writerDB == nil || objectID <= 0 {
		return errors.New("stitch stage retirement requires the exact operator target")
	}
	if !isLowerHexSHA256(strings.TrimSpace(sha)) || sha != strings.TrimSpace(sha) {
		return errors.New("stitch stage retirement requires the exact object SHA-256")
	}
	if !reconcile && (wire == nil || ev.Version != 1) {
		return errors.New("stitch stage apply requires reviewed evidence and the audited wire transport")
	}
	// The transaction, including its fence and epoch locks, must outlive the
	// wire attempt. One database session owns it; the durable intent commits on
	// a second connection while this one keeps its locks.
	ctx, cancel := context.WithTimeout(ctx, stitchStageDBTimeout)
	defer cancel()
	conn, err := c.writerDB.Acquire(ctx)
	if err != nil {
		return err
	}
	defer conn.Release()
	// One wire admission per staged object. This session guard spans the
	// separately committed one-shot reservation; other objects proceed.
	const stageLock = `hashtextextended($1::bigint::text,7343226011222)`
	var owned bool
	if err = conn.QueryRow(ctx, `SELECT pg_try_advisory_lock(`+stageLock+`)`, objectID).Scan(&owned); err != nil || !owned {
		return errors.New("stitch stage retirement busy")
	}
	defer conn.Exec(context.Background(), `SELECT pg_advisory_unlock(`+stageLock+`)`, objectID)
	tx, err := conn.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return err
	}
	defer tx.Rollback(context.Background())
	if _, err = tx.Exec(ctx, `SET LOCAL idle_in_transaction_session_timeout='90s'`); err != nil {
		return err
	}
	// Fail closed rather than queue behind a long holder. Every wait below is a
	// bounded lock acquisition, never an unbounded retry.
	if _, err = tx.Exec(ctx, `SELECT set_config('lock_timeout',$1,true)`, stitchStageLockTimeout.String()); err != nil {
		return err
	}
	if reconcile {
		found, err := c.observeAbortedStitchStage(ctx, tx, objectID, sha)
		if err != nil {
			return err
		}
		if found {
			return tx.Commit(ctx)
		}
	}
	// Writer admission first: the same shared lock every 0187-governed R2
	// writer takes, so a fence transition's exclusive attempt cannot pass while
	// this read-only-until-the-intent stage generation is being retired.
	if _, err = tx.Exec(ctx, `SELECT pg_advisory_xact_lock_shared(7343226011187)`); err != nil {
		return errors.New("stitch stage retirement requires quiescent R2 writer admission")
	}
	// Protection epoch next, matching certification and the inverse executor.
	if _, err = tx.Exec(ctx, `SELECT epoch FROM r2_inverse_ke_epoch FOR SHARE`); err != nil {
		return err
	}
	var t StitchStageTarget
	var hourID int64
	var attempt int
	var planSHA, manifestSHA string
	var streamID int64
	var issued, strictAbsent, absencePublished bool
	// FOR SHARE, never FOR UPDATE: the durable intent commits on another
	// connection and its foreign key needs KEY SHARE on this row, which a
	// FOR UPDATE lock would block. The endpoint is selected from the committed
	// row rather than assumed from config, so the value the wire must match is
	// the one certification recorded for this exact generation.
	err = tx.QueryRow(ctx, `SELECT o.object_key,o.etag,coalesce(o.version_id,''),o.size_bytes,o.last_modified,o.sha256,
 o.endpoint_sha256,h.id,h.attempt,h.plan_sha256,h.staged_manifest_sha256,r.stream_id,
 d.issued_at IS NOT NULL,d.strict_absence_observed_at IS NOT NULL,
 d.absence_observed_at IS NOT NULL
 FROM nas_stitch_r2_stage_objects o
 JOIN nas_stitch_r2_hours h ON h.id=o.hour_id AND h.attempt=o.attempt
 JOIN nas_stitch_r2_certificates cert ON cert.hour_id=h.id AND cert.attempt=h.attempt
  AND cert.plan_sha256=h.plan_sha256 AND cert.manifest_sha256=h.staged_manifest_sha256
 JOIN recordings r ON r.id=h.recording_id
 JOIN nas_r2_copy_items q ON q.kind='stitch_output' AND q.source_id=o.id AND q.connection_id=13
  AND q.manifest_sha256=cert.manifest_sha256 AND nas_stitch_r2_exact_copy_receipt(q,o)
 LEFT JOIN nas_stitch_r2_stage_retirements d ON d.object_id=o.id
 WHERE o.id=$1 AND o.sha256=$2 AND o.bucket=$3
  AND o.object_key LIKE '`+stitchStageKeyPrefix+`%'
  AND o.state='staged' AND h.state='certified'
  -- the granting lease and every live copy read grant have drained
  AND q.state='verified' AND q.lease_token IS NULL AND q.lease_expires_at IS NULL
  AND COALESCE(o.grants_drain_at,'infinity')<clock_timestamp()
  AND COALESCE(q.stitch_read_grants_until,'-infinity')<clock_timestamp()
  AND COALESCE(q.range_until,'-infinity')<clock_timestamp()
  -- retained dependency evidence, unchanged from certification
  AND NOT EXISTS(SELECT 1 FROM delivery_keep_holds k WHERE (k.bucket=o.bucket AND k.object_key=o.object_key)
    OR k.relative_path=o.relative_path OR (k.sha256=o.sha256 AND k.size_bytes=o.size_bytes))
  AND NOT EXISTS(SELECT 1 FROM recording_collation_outputs j WHERE j.object_key=o.object_key OR j.nas_relative_path=o.relative_path
    OR (j.sha256=o.sha256 AND j.size_bytes=o.size_bytes))
  AND NOT EXISTS(SELECT 1 FROM recording_clips raw WHERE (raw.bucket=o.bucket AND raw.object_key=o.object_key)
    OR (raw.display_path<>'' AND raw.display_path=o.relative_path))
  -- the staging key is never its own owner exemption
  AND NOT r2_owner_keep_held(o.bucket,o.object_key)
 FOR SHARE OF o,h,cert,q`,
		objectID, sha, c.bucket).
		Scan(&t.Key, &t.ETag, &t.VersionID, &t.SizeBytes, &t.LastModified, &t.SHA256,
			&t.EndpointSHA256, &hourID, &attempt, &planSHA, &manifestSHA, &streamID, &issued, &strictAbsent,
			&absencePublished)
	if err != nil {
		return errors.New("stitch stage lacks exact NAS custody, drain or dependency clearance")
	}
	t.Bucket = c.bucket
	if !ValidStitchStageKey(t.Key, t.SHA256) || t.VersionID != "" {
		return errors.New("stitch stage target is not an exact unversioned stage key")
	}
	// The committed row, this client and the audited wire must all name one
	// provider endpoint. Config never overrides the row, and the wire is checked
	// against the row rather than against the same config it was built from.
	if t.EndpointSHA256 != c.EndpointSHA256() {
		return errors.New("stitch stage committed endpoint differs from the retirement endpoint")
	}
	if !reconcile && wire.EndpointSHA256() != t.EndpointSHA256 {
		return errors.New("stitch stage wire endpoint differs from the committed stage endpoint")
	}
	if strictAbsent {
		return nil
	}
	// A strict absence that a prior invocation proved is terminal for this one:
	// the trigger permits no further update, and the release gate already
	// accepts the row.
	if issued != reconcile {
		return errors.New("stitch stage retirement state differs; issued targets require reconciliation")
	}
	// An issued intent whose strict absence is not published is an unresolved,
	// possibly-ambiguous DELETE. A later read-only observation must never publish
	// it: absence now proves neither that this DELETE caused it nor that an
	// ungoverned writer cannot recreate the key, and this command never edits or
	// re-derives what a prior invocation recorded. Such a target stays held, and
	// the only path that publishes absence is the invocation that saw its own
	// DELETE succeed and then strictly re-confirmed the exact key.
	if reconcile {
		if absencePublished {
			return errors.New("stitch stage delete remains unresolved; a prior absence observation is held and is never confirmed by a later read")
		}
		return errors.New("stitch stage delete remains unresolved; only its own successful DELETE with a strict absence proof can publish absence")
	}
	// The reviewed evidence must name this exact generation and hour, the
	// reviewed picks, and the live fence it was reviewed against. The stream
	// sets are compared against the sets independently derived from the pinned
	// picks; a document that names any other stream is refused.
	if ev.Bucket != t.Bucket || ev.EndpointSHA256 != t.EndpointSHA256 || ev.ObjectID != objectID ||
		ev.ObjectKey != t.Key || ev.SHA256 != t.SHA256 || ev.SizeBytes != t.SizeBytes ||
		ev.HourID != hourID || ev.Attempt != attempt || ev.PlanSHA256 != planSHA || ev.ManifestSHA256 != manifestSHA ||
		ev.ExternalWriterRace != StitchStageExternalWriterRace || strings.TrimSpace(ev.DeployedWriterCoverage) == "" {
		return errors.New("stitch stage evidence does not bind this exact generation, hour or premise")
	}
	if err := picks.matchesEvidence(ev); err != nil {
		return err
	}
	dbNow, fence, err := c.stitchStageFence(ctx, tx)
	if err != nil {
		return err
	}
	// The fence activation and drain deadline are compared exactly, never
	// truncated. Both values come from the same timestamptz row, whose
	// microsecond precision survives the round trip unchanged, and the reviewed
	// evidence must name that row's own stamps. A subsecond difference is a
	// different fence, so it is refused rather than rounded into a match.
	if !fence.ActivatedAt.Equal(ev.FenceActivatedAt) ||
		!fence.DrainNotBefore.Equal(ev.DrainNotBefore) {
		return errors.New("stitch stage evidence names a different writer fence")
	}
	if !dbNow.After(fence.DrainNotBefore) {
		return errors.New("stitch stage evidence was observed before the fence drained")
	}
	// Source provenance is authoritative and fails closed: undecided or
	// keep-everything stream, incomplete committed source set, owner/thesis
	// prefix, a pinned KE folder, or permanent incident evidence refuses the
	// stage whatever its own key says.
	clear, err := c.stitchStageProvenanceClear(ctx, tx, hourID, streamID, picks)
	if err != nil {
		return err
	}
	if !clear {
		return errors.New("stitch stage source provenance is protected, undecided or unproven")
	}
	// One wire deadline covers the exact HEAD and the DELETE's first byte. It is
	// fixed here, before any pre-issuance read, and every pre-issuance step and
	// the independent attempt commit are bounded by it as well, so none of them
	// can consume authority that has already expired. A slower caller deadline
	// never extends it; an earlier one still wins.
	deadline := time.Now().Add(stitchStageWireTimeout)
	if parent, ok := ctx.Deadline(); ok && parent.Before(deadline) {
		deadline = parent
	}
	preIssue, stopPreIssue := context.WithDeadline(ctx, deadline)
	defer stopPreIssue()
	// The exact-key multipart inventory must be empty before any HEAD.
	if err := c.stitchStageNoMultipart(preIssue, t.Key); err != nil {
		return err
	}
	head, err := wire.ExactHead(ctx, t, deadline)
	if err != nil {
		return fmt.Errorf("stitch stage exact HEAD: %w", err)
	}
	if cleanETag(head.ETag) != cleanETag(t.ETag) || head.VersionID != t.VersionID ||
		head.SizeBytes != t.SizeBytes || head.SHA256Metadata != t.SHA256 || !head.LastModified.Equal(t.LastModified) {
		return errors.New("stitch stage generation differs")
	}
	if _, err := c.stitchStageDeleteFence(preIssue, tx); err != nil {
		return err
	}
	// Nothing is minted once the wire bound has passed: authority that could
	// never send must not exist, so this fails before the attempt insert.
	if err := stitchStagePreIssuanceExpired(preIssue); err != nil {
		return fmt.Errorf("stitch stage pre-issuance ended before wire authority could be issued: %w", err)
	}
	// Commit the one-shot reservation on a second connection while the outer
	// session lock and the writer-fence and epoch locks remain held. It is
	// bounded by the same pre-issuance bound.
	if _, err = c.writerDB.Exec(preIssue, `INSERT INTO nas_stitch_r2_stage_retirements(object_id) VALUES($1)`, objectID); err != nil {
		return err
	}
	// The attempt COMMIT is a separate connection and can acknowledge after the
	// bound expired. That durable intent stays held: it is never retried, edited
	// or reissued, and no DELETE may come from authority that outlived its
	// bound. Only an *acknowledged* commit reaches here; an unknown one already
	// failed above and is held the same way.
	if err := stitchStagePreIssuanceExpired(preIssue); err != nil {
		return fmt.Errorf("stitch stage delete intent is held after a late attempt commit and is never reissued: %w", err)
	}
	recheck := func(ctx context.Context) error { return c.stitchStageWireRecheck(ctx, tx, t, ev) }
	if err := wire.DeleteOnce(ctx, t, deadline, recheck); err != nil {
		return fmt.Errorf("stitch stage delete outcome unconfirmed; the intent is held and never reissued: %w", err)
	}
	return c.confirmStitchStageRetirement(ctx, tx, objectID, t.Key)
}

// stitchStagePreIssuanceExpired reports whether the effective pre-issuance
// bound has passed. A context timer can be delivered late, so Err() alone stays
// nil for a while after the deadline; comparing the monotonic wall clock
// against the context's own deadline closes that window instead of re-enabling
// the wire.
func stitchStagePreIssuanceExpired(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	deadline, ok := ctx.Deadline()
	if !ok {
		return nil
	}
	if !time.Now().Before(deadline) {
		return context.DeadlineExceeded
	}
	return nil
}

// matchesEvidence requires the reviewed document to be exactly the derivation
// of the pinned picks it names. The evidence is never the source of the stream
// sets: a document that pins one picks digest and lists streams those picks do
// not decide would otherwise become authority for a stream the owner never
// decided. Both sides are compared as exact sorted sets, so no ordering or
// duplicate in the document can widen or narrow the derivation.
func (p StitchStagePicks) matchesEvidence(ev StitchStageEvidence) error {
	if !isLowerHexSHA256(p.SHA256) || ev.PicksSHA256 != p.SHA256 {
		return errors.New("stitch stage evidence pins different owner picks than the retirement loaded")
	}
	if !sortedUniqueAscending(p.Decided) || !sortedUniqueAscending(p.KE) {
		return errors.New("stitch stage picks derivation is not canonical")
	}
	if !equalIDs(p.Decided, ev.DecidedStreamIDs) {
		return errors.New("stitch stage evidence decided streams differ from the pinned picks")
	}
	if !equalIDs(p.KE, ev.KEStreamIDs) {
		return errors.New("stitch stage evidence keep-everything streams differ from the pinned picks")
	}
	return nil
}

// sortedUniqueAscending reports a strictly ascending, positive, duplicate-free
// id list, so one decision set has exactly one encoding.
func sortedUniqueAscending(ids []int64) bool {
	for i, id := range ids {
		if id <= 0 || (i > 0 && ids[i-1] >= id) {
			return false
		}
	}
	return true
}

func equalIDs(a, b []int64) bool {
	if len(a) != len(b) {
		return false
	}
	if !sortedUniqueAscending(a) || !sortedUniqueAscending(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// keFolders is the same first-path-component KE folder set loadInverseKEFolders
// derives, restricted here to the reviewed keep-everything streams and read in
// the retirement's own transaction. Any lookup, scan or iteration error is
// returned, so the caller refuses the stage instead of silently losing a veto.
func (c *Client) keFolders(ctx context.Context, tx pgx.Tx, streams []int64) ([]string, error) {
	if len(streams) == 0 {
		return nil, nil
	}
	rows, err := tx.Query(ctx, `SELECT DISTINCT folder FROM (
 SELECT split_part(regexp_replace(cc.display_path,'^joined/',''),'/',1) folder
 FROM recording_clips cc JOIN recordings r ON r.id=cc.recording_id
 WHERE cc.bucket=$1 AND r.stream_id=ANY($2::bigint[])
 UNION SELECT split_part(regexp_replace(o.nas_relative_path,'^joined/',''),'/',1)
 FROM recording_collation_outputs o JOIN recording_collation_hours h ON h.id=o.collation_hour_id
 JOIN recordings r ON r.id=h.recording_id WHERE r.stream_id=ANY($2::bigint[])
 UNION SELECT split_part(regexp_replace(cc.display_path,'^joined/',''),'/',1)
 FROM recording_collation_outputs o CROSS JOIN LATERAL unnest(o.source_clip_ids) source_id
 JOIN recording_clips cc ON cc.id=source_id JOIN recordings r ON r.id=cc.recording_id
 WHERE r.stream_id=ANY($2::bigint[])) folders WHERE folder<>''`, c.bucket, streams)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	folders := []string{}
	for rows.Next() {
		var folder string
		if err = rows.Scan(&folder); err != nil {
			return nil, err
		}
		folders = append(folders, folder)
	}
	return folders, rows.Err()
}

// stitchStageProvenanceClear decides whether the certified hour's committed
// sources clear every source-side veto. It reads authoritative catalog rows,
// not filenames: the hour must be decided by the pinned picks, its stream must
// not be keep-everything, every plan clip must have its exact committed hold,
// and no source may be under an owner keep prefix, a pinned KE folder or
// permanent incident evidence. Any missing or mismatched row fails closed.
func (c *Client) stitchStageProvenanceClear(ctx context.Context, tx pgx.Tx, hourID, streamID int64, picks StitchStagePicks) (bool, error) {
	if !containsID(picks.Decided, streamID) || containsID(picks.KE, streamID) {
		return false, nil
	}
	folders, err := c.keFolders(ctx, tx, picks.KE)
	if err != nil {
		return false, err
	}
	var clear bool
	row := tx.QueryRow(ctx, `SELECT
 (SELECT count(*) FROM nas_stitch_r2_input_holds i WHERE i.hour_id=$1)=jsonb_array_length(h.plan->'clips')
 AND NOT EXISTS(
  SELECT 1 FROM nas_stitch_r2_input_holds i
  LEFT JOIN recording_clips cc ON cc.id=i.catalog_clip_id
  LEFT JOIN recordings cr ON cr.id=cc.recording_id
  WHERE i.hour_id=$1 AND (cc.id IS NULL OR cr.id IS NULL OR cr.stream_id IS NULL
   OR cc.recording_id<>h.recording_id OR cc.bucket<>i.bucket OR cc.object_key<>i.object_key
   OR lower(cc.sha256)<>i.sha256 OR cc.size_bytes<>i.size_bytes OR cr.stream_id<>$2))
 AND NOT EXISTS(
  SELECT 1 FROM nas_stitch_r2_input_holds i
  JOIN recording_clips cc ON cc.id=i.catalog_clip_id
  WHERE i.hour_id=$1 AND (
   r2_owner_keep_held(cc.bucket,cc.object_key)
   OR EXISTS(SELECT 1 FROM r2_owner_keep_prefixes p WHERE p.bucket=cc.bucket AND p.prefix='raw/stream/'||$2::text||'/')
   OR EXISTS(SELECT 1 FROM nas_cleanup_incident_sources x WHERE x.clip_id=cc.id)
   OR EXISTS(SELECT 1 FROM nas_cleanup_incident_roster x WHERE x.bucket=cc.bucket
     AND (x.object_key=cc.object_key OR x.raw_source_ids @> ARRAY[cc.id]))))
 AND NOT EXISTS(
  SELECT 1 FROM nas_stitch_r2_input_holds i JOIN recording_clips cc ON cc.id=i.catalog_clip_id
  WHERE i.hour_id=$1 AND cc.display_path<>''
   AND split_part(regexp_replace(cc.display_path,'^joined/',''),'/',1)=ANY($3::text[]))
 FROM nas_stitch_r2_hours h WHERE h.id=$1`, hourID, streamID, folders)
	if err = row.Scan(&clear); err != nil {
		return false, err
	}
	return clear, nil
}

func containsID(ids []int64, id int64) bool {
	for _, v := range ids {
		if v == id {
			return true
		}
	}
	return false
}

// stitchStageWireRecheck is the last check before the first DELETE byte. It
// runs at the transport, so it re-reads the stage row, its drain clocks and the
// live fence under the retirement's still-held locks and refuses any change.
func (c *Client) stitchStageWireRecheck(ctx context.Context, tx pgx.Tx, t StitchStageTarget, ev StitchStageEvidence) error {
	var key, etag, sha string
	var size int64
	var modified time.Time
	var staged, drained, fenced bool
	err := tx.QueryRow(ctx, `SELECT o.object_key,o.etag,o.sha256,o.size_bytes,o.last_modified,
 o.state='staged',COALESCE(o.grants_drain_at,'infinity')<clock_timestamp(),
 r2_retirement_writes_blocked($2,$3)
 FROM nas_stitch_r2_stage_objects o CROSS JOIN r2_retirement_writer_fences f
 WHERE o.bucket=$2 AND o.object_key=$1 AND f.bucket=$2 AND f.endpoint_sha256=$3
 FOR SHARE OF o,f`, t.Key, t.Bucket, t.EndpointSHA256).
		Scan(&key, &etag, &sha, &size, &modified, &staged, &drained, &fenced)
	if err != nil {
		return err
	}
	if key != t.Key || cleanETag(etag) != cleanETag(t.ETag) || sha != t.SHA256 || size != t.SizeBytes ||
		!modified.Equal(t.LastModified) || !staged || !drained || !fenced {
		return errors.New("stitch stage generation, drain or fence changed before the wire")
	}
	now, fence, err := c.stitchStageFence(ctx, tx)
	if err != nil {
		return err
	}
	// Same exact-stamp binding as the pre-wire check: a subsecond difference in
	// the activation is a different fence, not the reviewed one.
	if !fence.ActivatedAt.Equal(ev.FenceActivatedAt) || !now.After(fence.DrainNotBefore) {
		return errors.New("stitch stage fence or drain changed before the wire")
	}
	return nil
}

// confirmStitchStageRetirement publishes a strict absence proof for one exact
// stage key. It is reached only from an invocation that either saw its own
// DELETE answered and then re-read the exact key, or (separately) an aborted
// object's own drained proof. It re-checks the exact-key multipart inventory
// here, so publication can never be recorded while an upload for the same key
// could still complete and recreate the object.
func (c *Client) confirmStitchStageRetirement(ctx context.Context, tx pgx.Tx, objectID int64, key string) error {
	if err := c.stitchStageNoMultipart(ctx, key); err != nil {
		return fmt.Errorf("stitch stage absence unconfirmed: %w", err)
	}
	if err := c.requireStitchStageAbsent(ctx, key); err != nil {
		return fmt.Errorf("stitch stage delete absence unconfirmed; the held intent is never reissued: %w", err)
	}
	tag, err := tx.Exec(ctx, `WITH observed AS MATERIALIZED (SELECT clock_timestamp() AS at)
 UPDATE nas_stitch_r2_stage_retirements d SET absence_observed_at=coalesce(d.absence_observed_at,observed.at),
 strict_absence_observed_at=observed.at FROM observed WHERE d.object_id=$1 AND d.strict_absence_observed_at IS NULL`, objectID)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return errors.New("strict stitch stage absence was not recorded")
	}
	return tx.Commit(ctx)
}

// observeAbortedStitchStage only records R2 absence. An abort after a possible
// multipart completion is ambiguous even when a HEAD currently returns 404.
func (c *Client) observeAbortedStitchStage(ctx context.Context, tx pgx.Tx, objectID int64, sha string) (bool, error) {
	var key string
	var drained, observed bool
	err := tx.QueryRow(ctx, `SELECT o.object_key,
  safe.id IS NOT NULL AND coalesce(safe.grants_drain_at,'-infinity')<clock_timestamp(),a.object_id IS NOT NULL
 FROM nas_stitch_r2_stage_objects o
 LEFT JOIN nas_stitch_r2_aborts_awaiting_absence safe ON safe.id=o.id
 LEFT JOIN nas_stitch_r2_aborted_absences a ON a.object_id=o.id
 WHERE o.id=$1 AND o.sha256=$2 AND o.bucket=$3 AND o.endpoint_sha256=$4 AND o.state='aborted'
 FOR SHARE OF o`, objectID, sha, c.bucket, c.EndpointSHA256()).Scan(&key, &drained, &observed)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if observed {
		return true, nil
	}
	if !drained {
		return false, errors.New("aborted stitch stage writer drain or multipart outcome is unconfirmed")
	}
	if _, err := c.stitchStageDeleteFence(ctx, tx); err != nil {
		return false, err
	}
	if err := c.VerifyAbortedStitchStageAbsence(ctx, c.bucket, c.EndpointSHA256(), key); err != nil {
		return false, err
	}
	_, err = tx.Exec(ctx, `INSERT INTO nas_stitch_r2_aborted_absences(object_id) VALUES($1)`, objectID)
	return err == nil, err
}

// VerifyAbortedStitchStageAbsence is a provider-only check. The caller must
// establish that this exact stage generation is terminal and its grants drained.
func (c *Client) VerifyAbortedStitchStageAbsence(ctx context.Context, bucket, endpointSHA, key string) error {
	if bucket != c.bucket || endpointSHA != c.EndpointSHA256() || !strings.HasPrefix(key, stitchStageKeyPrefix) {
		return errors.New("aborted stitch stage provider identity differs")
	}
	if err := c.stitchStageNoMultipart(ctx, key); err != nil {
		return err
	}
	return c.requireStitchStageAbsent(ctx, key)
}

// stitchStageNoMultipart requires a complete, empty, exact-key multipart
// inventory. Only a first and final page proves the listing is empty, and the
// provider must echo this exact bucket, prefix and no markers.
func (c *Client) stitchStageNoMultipart(ctx context.Context, key string) error {
	uploads, err := c.s3.ListMultipartUploads(ctx, &s3.ListMultipartUploadsInput{
		Bucket: aws.String(c.bucket), Prefix: aws.String(key), MaxUploads: aws.Int32(1),
	}, c.stitchStageObservationOptions)
	if err != nil || aws.ToString(uploads.Bucket) != c.bucket || aws.ToString(uploads.Prefix) != key ||
		aws.ToString(uploads.KeyMarker) != "" || aws.ToString(uploads.UploadIdMarker) != "" ||
		aws.ToString(uploads.NextKeyMarker) != "" || aws.ToString(uploads.NextUploadIdMarker) != "" ||
		aws.ToString(uploads.Delimiter) != "" || (uploads.MaxUploads != nil && *uploads.MaxUploads != 1) ||
		uploads.IsTruncated == nil || *uploads.IsTruncated || len(uploads.Uploads) != 0 || len(uploads.CommonPrefixes) != 0 {
		return errors.New("stitch stage multipart work has not drained")
	}
	return nil
}

func (c *Client) stitchStageObservationOptions(o *s3.Options) {
	noStitchStageRetries(o)
	o.HTTPClient = &http.Client{Transport: c.deleteTransport, CheckRedirect: refuseRetirementRedirect}
}

func (c *Client) requireStitchStageAbsent(ctx context.Context, key string) error {
	_, err := c.s3.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(c.bucket), Key: aws.String(key),
	}, c.stitchStageObservationOptions)
	var response *awshttp.ResponseError
	if !IsNotFound(err) || !errors.As(err, &response) || response.HTTPStatusCode() != http.StatusNotFound ||
		len(response.Response.Header.Values("x-amz-delete-marker")) != 0 {
		return errors.New("stitch stage absence remains unconfirmed")
	}
	codes := response.Response.Header.Values("x-amz-error-code")
	if len(codes) > 1 || (len(codes) == 1 && codes[0] != "NotFound" && codes[0] != "NoSuchKey" && codes[0] != "404") {
		return errors.New("stitch stage HEAD returned a conflicting provider error")
	}
	return nil
}

func (c *Client) stitchStageDeleteFence(ctx context.Context, tx pgx.Tx) (time.Time, error) {
	now, _, err := c.stitchStageFence(ctx, tx)
	return now, err
}

// stitchStageFence reads the exact live fence under the caller's locks and
// returns its database clock, activation and drain deadline, so evidence can be
// bound to the fence it was reviewed against rather than to wall time.
func (c *Client) stitchStageFence(ctx context.Context, tx pgx.Tx) (time.Time, StitchStageFence, error) {
	var now time.Time
	var f StitchStageFence
	var blocked bool
	err := tx.QueryRow(ctx, `SELECT clock_timestamp(),activated_at,drain_not_before,r2_retirement_writes_blocked($1,$2)
 FROM r2_retirement_writer_fences WHERE bucket=$1 AND endpoint_sha256=$2 FOR SHARE`,
		c.bucket, c.EndpointSHA256()).Scan(&now, &f.ActivatedAt, &f.DrainNotBefore, &blocked)
	if err != nil || !blocked {
		return time.Time{}, StitchStageFence{}, errors.New("exact live R2 writer fence and drain required for staging retirement")
	}
	return now, f, nil
}

// StitchStageFence is the exact fence activation a stage's evidence binds.
type StitchStageFence struct {
	ActivatedAt, DrainNotBefore time.Time
}
