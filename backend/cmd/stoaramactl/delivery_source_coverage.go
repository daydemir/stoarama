package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/daydemir/stoarama/backend/internal/collation"
	"github.com/daydemir/stoarama/backend/internal/config"
	"github.com/daydemir/stoarama/backend/internal/r2"
	"github.com/jackc/pgx/v5"
)

const (
	deliveryCoverageVerifierSHA = "17c8033b207a4603fcee7b81321db16c41d02cc385141a8f7a04c05994164589"
	deliveryCoverageSupportSHA  = "a213dfe586530a6f91f0514763bedaeeacd8931d4ff92c70e40cdba03e23d516"
	deliveryCoverageFFmpegSHA   = "eab722b24d599876819f89f503cc142b39a096a971d7d7913a0282b0484b9d5e"
	deliveryCoverageFFprobeSHA  = "95cda0f6de6e2d0c48cc2afe24e8ef9bcb8b4ffc5897af77158b1a59b303a97a"
	deliveryCoverageMaxDocument = 8 << 20
)

type deliveryCoverageTarget struct {
	OutputID, CollationHourID, RecordingID    int64
	ManifestKey, ManifestSHA, HourID, BatchID string
	Bucket, ETag                              string
	SourceIDs                                 []int64
	Output                                    deliveryCoverageOutput
}

type deliveryCoverageOutput struct {
	Part  int    `json:"part"`
	Parts int    `json:"parts"`
	Key   string `json:"object_key"`
	Path  string `json:"nas_relative_path"`
	SHA   string `json:"sha256"`
	Bytes int64  `json:"size_bytes"`
}

type deliveryCoverageSource struct {
	ID          int64     `json:"clip_id"`
	RecordingID int64     `json:"recording_id"`
	Bucket      string    `json:"bucket"`
	Key         string    `json:"object_key"`
	ETag        string    `json:"etag"`
	SHA         string    `json:"sha256"`
	Bytes       int64     `json:"size_bytes"`
	Start       time.Time `json:"start_utc"`
	End         time.Time `json:"end_utc"`
}

type deliveryCoverageVerdict string

const (
	deliveryCoverageProven   deliveryCoverageVerdict = "proven"
	deliveryCoverageUnproven deliveryCoverageVerdict = "not_proven"
)

type deliveryCoverageTrack struct {
	PacketCount int64  `json:"packet_count"`
	PacketChain string `json:"packet_chain_sha256"`
	FrameCount  int64  `json:"frame_count"`
	FrameChain  string `json:"frame_chain_sha256"`
	SampleCount int64  `json:"sample_count"`
	PCMChain    string `json:"pcm_chain_sha256"`
	AudioRule   string `json:"rule"`
	Primers     int    `json:"seam_primer_packets"`
}

type deliveryCoverageObservation struct {
	Source  deliveryCoverageSource           `json:"source"`
	Ordinal int                              `json:"ordinal"`
	Verdict deliveryCoverageVerdict          `json:"verdict"`
	Reason  string                           `json:"reason"`
	Tracks  map[string]deliveryCoverageTrack `json:"tracks"`
}

type deliveryCoverageReport struct {
	SchemaVersion int                           `json:"schema_version"`
	ManifestSHA   string                        `json:"manifest_sha256"`
	HourID        string                        `json:"hour_id"`
	BatchID       string                        `json:"batch_id"`
	Output        deliveryCoverageOutput        `json:"output"`
	VerifierSHA   string                        `json:"verifier_sha256"`
	SupportSHA    string                        `json:"verifier_support_sha256"`
	FFmpegSHA     string                        `json:"ffmpeg_sha256"`
	FFprobeSHA    string                        `json:"ffprobe_sha256"`
	Sources       []deliveryCoverageObservation `json:"sources"`
}

func readDeliveryCoverageTarget(ctx context.Context, q r2RetireQuerier, id int64) (deliveryCoverageTarget, error) {
	t := deliveryCoverageTarget{OutputID: id}
	err := q.QueryRow(ctx, `
 SELECT h.id,h.recording_id,h.manifest_object_key,h.manifest_sha256,h.hour_id,h.batch_id,
        o.source_clip_ids,o.part,o.parts,o.object_key,o.nas_relative_path,o.sha256,o.size_bytes,p.bucket,p.etag
 FROM delivery_nas_handoff_outputs p
 JOIN recording_collation_outputs o ON o.id=p.output_id AND o.object_key=p.object_key
  AND o.nas_relative_path=p.nas_relative_path AND o.sha256=p.sha256 AND o.size_bytes=p.size_bytes
 JOIN recording_collation_hours h ON h.id=o.collation_hour_id AND h.recording_id=p.recording_id
 JOIN recordings rec ON rec.id=h.recording_id AND rec.account_id=47
 WHERE o.id=$1 AND h.generation=2 AND h.status='collated'`, id).Scan(
		&t.CollationHourID, &t.RecordingID, &t.ManifestKey, &t.ManifestSHA, &t.HourID, &t.BatchID, &t.SourceIDs,
		&t.Output.Part, &t.Output.Parts, &t.Output.Key, &t.Output.Path, &t.Output.SHA, &t.Output.Bytes, &t.Bucket, &t.ETag)
	if errors.Is(err, pgx.ErrNoRows) {
		err = errors.New("no approved source-coverage output matches the current catalog")
	}
	return t, err
}

func readCoveragePinnedDocument(path, digest string) ([]byte, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() || info.Size() <= 0 || info.Size() > deliveryCoverageMaxDocument {
		return nil, errors.New("coverage input must be a bounded regular file")
	}
	raw, err := io.ReadAll(io.LimitReader(f, deliveryCoverageMaxDocument+1))
	if err != nil {
		return nil, err
	}
	if int64(len(raw)) != info.Size() || fmt.Sprintf("%x", sha256.Sum256(raw)) != digest {
		return nil, errors.New("coverage input differs from its pinned SHA-256")
	}
	return raw, nil
}

func coverageTool(path, want string) (string, error) {
	path, err := filepath.Abs(path)
	if err != nil {
		return "", err
	}
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return "", err
	}
	if !info.Mode().IsRegular() || info.Mode().Perm()&0111 == 0 || info.Size() > 512<<20 {
		return "", errors.New("coverage media tool is not a regular executable")
	}
	sum := sha256.New()
	if _, err := io.Copy(sum, f); err != nil {
		return "", err
	}
	if hex.EncodeToString(sum.Sum(nil)) != want {
		return "", errors.New("coverage media tool differs from the reviewed executable")
	}
	return path, nil
}

func loadCoverageManifest(target deliveryCoverageTarget, manifestPath string) (collation.HourManifest, []byte, error) {
	var manifest collation.HourManifest
	manifestRaw, err := readCoveragePinnedDocument(manifestPath, target.ManifestSHA)
	if err != nil {
		return manifest, nil, err
	}
	if json.Unmarshal(manifestRaw, &manifest) != nil || manifest.Validate() != nil || manifest.RecordingID != target.RecordingID ||
		manifest.HourID != target.HourID || manifest.BatchID != target.BatchID || target.Output.Part < 1 || target.Output.Part > len(manifest.Outputs) {
		return manifest, nil, errors.New("the frozen collation manifest differs from the generation-2 catalog")
	}
	output := manifest.Outputs[target.Output.Part-1]
	if output.ObjectKey != target.Output.Key || output.SHA256 != target.Output.SHA || output.SizeBytes != target.Output.Bytes ||
		output.NASRelativePath != target.Output.Path || output.Parts != target.Output.Parts || !slices.Equal(output.SourceClipIDs, target.SourceIDs) {
		return manifest, nil, errors.New("approved output or ordered sources differ from the frozen manifest")
	}
	return manifest, manifestRaw, nil
}

func verifyDeliveryCoverage(ctx context.Context, target deliveryCoverageTarget, manifestPath, mediaDir, verifierDir, ffmpeg, ffprobe string) ([]byte, error) {
	manifest, manifestRaw, err := loadCoverageManifest(target, manifestPath)
	if err != nil {
		return nil, err
	}
	output := manifest.Outputs[target.Output.Part-1]
	ffmpeg, err = coverageTool(ffmpeg, deliveryCoverageFFmpegSHA)
	if err != nil {
		return nil, err
	}
	ffprobe, err = coverageTool(ffprobe, deliveryCoverageFFprobeSHA)
	if err != nil {
		return nil, err
	}
	mediaDir, err = filepath.Abs(mediaDir)
	if err != nil {
		return nil, err
	}
	scratch, err := os.MkdirTemp("", "stoarama-delivery-coverage-")
	if err != nil {
		return nil, err
	}
	defer os.RemoveAll(scratch)
	for _, file := range []struct{ name, sha string }{
		{"delivery_coverage.py", deliveryCoverageVerifierSHA},
		{"stoarama_pull.py", deliveryCoverageSupportSHA},
	} {
		raw, err := readCoveragePinnedDocument(filepath.Join(verifierDir, file.name), file.sha)
		if err != nil {
			return nil, err
		}
		if err := os.WriteFile(filepath.Join(scratch, file.name), raw, 0400); err != nil {
			return nil, err
		}
	}
	frozenManifest := filepath.Join(scratch, "manifest.json")
	if err := os.WriteFile(frozenManifest, manifestRaw, 0400); err != nil {
		return nil, err
	}
	args := []string{"-B", filepath.Join(scratch, "delivery_coverage.py"), "--manifest", frozenManifest,
		"--manifest-sha256", target.ManifestSHA, "--part", strconv.Itoa(output.Part), "--ffmpeg", ffmpeg, "--ffprobe", ffprobe,
		filepath.Join(mediaDir, "output.mp4")}
	for _, id := range output.SourceClipIDs {
		args = append(args, filepath.Join(mediaDir, fmt.Sprintf("raw-%d.mp4", id)))
	}
	cmd := exec.CommandContext(ctx, "python3", args...)
	cmd.Dir = scratch
	cmd.Env = []string{"PATH=/usr/bin:/bin", "TMPDIR=" + scratch, "LANG=C"}
	cmd.Stderr = os.Stderr
	return cmd.Output()
}

func fetchCoverageObject(ctx context.Context, store *r2.Client, path, object, etag, digest string, expectedBytes int64) error {
	var head r2.ObjectHead
	var err error
	if etag == "" {
		head, err = store.Head(ctx, object)
	} else {
		head, err = store.HeadExact(ctx, object, etag, "")
	}
	if err != nil {
		return err
	}
	if head.SizeBytes <= 0 || head.ETag == "" ||
		(etag != "" && strings.Trim(head.ETag, `"`) != strings.Trim(etag, `"`)) ||
		(expectedBytes > 0 && expectedBytes != head.SizeBytes) ||
		(expectedBytes == 0 && head.SizeBytes > deliveryCoverageMaxDocument) {
		return errors.New("R2 HEAD differs from the approved delivery coverage identity")
	}
	body, err := store.OpenExact(ctx, object, head.ETag, head.VersionID)
	if err != nil {
		return err
	}
	defer body.Close()
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return err
	}
	defer file.Close()
	sum := sha256.New()
	size, err := io.Copy(io.MultiWriter(file, sum), io.LimitReader(body, head.SizeBytes+1))
	if err != nil {
		return err
	}
	if size != head.SizeBytes || hex.EncodeToString(sum.Sum(nil)) != digest {
		return errors.New("downloaded R2 bytes differ from the approved delivery coverage identity")
	}
	return file.Close()
}

func fetchCoverageInputs(ctx context.Context, store *r2.Client, target deliveryCoverageTarget, dir string) (string, error) {
	if target.Bucket != store.Bucket() {
		return "", errors.New("delivery coverage output is outside the archive bucket")
	}
	manifestPath := filepath.Join(dir, "manifest.json")
	if err := fetchCoverageObject(ctx, store, manifestPath, target.ManifestKey, "", target.ManifestSHA, 0); err != nil {
		return "", err
	}
	manifest, _, err := loadCoverageManifest(target, manifestPath)
	if err != nil {
		return "", err
	}
	if err := fetchCoverageObject(ctx, store, filepath.Join(dir, "output.mp4"), target.Output.Key, target.ETag, target.Output.SHA, target.Output.Bytes); err != nil {
		return "", err
	}
	selected := make(map[int64]collation.Clip)
	for _, row := range manifest.Clips {
		if row.Disposition == "included" && row.Part == target.Output.Part {
			selected[row.Clip.ClipID] = row.Clip
		}
	}
	for _, id := range target.SourceIDs {
		raw, ok := selected[id]
		if !ok || raw.Bucket != store.Bucket() || raw.ETag == "" || raw.SizeBytes <= 0 || !r2RetireLowerHex64(raw.SHA256) || raw.Purged {
			return "", fmt.Errorf("raw clip %d lacks exact archived identity", id)
		}
		if err := fetchCoverageObject(ctx, store, filepath.Join(dir, fmt.Sprintf("raw-%d.mp4", id)),
			raw.ObjectKey, raw.ETag, raw.SHA256, raw.SizeBytes); err != nil {
			return "", fmt.Errorf("raw clip %d: %w", id, err)
		}
	}
	return manifestPath, nil
}

func validDeliveryTrack(typ string, t deliveryCoverageTrack) bool {
	if t.PacketCount <= 0 || len(t.PacketChain) != 64 || !isLowerHex(t.PacketChain) {
		return false
	}
	switch typ {
	case "video":
		return t.FrameCount > 0 && len(t.FrameChain) == 64 && isLowerHex(t.FrameChain)
	case "audio":
		return t.SampleCount > 0 && len(t.PCMChain) == 64 && isLowerHex(t.PCMChain) &&
			((t.AudioRule == "seam_primed" && t.Primers == 1) || (t.AudioRule == "standalone_exact" && t.Primers == 0))
	}
	return false
}

func parseDeliveryCoverageReport(raw []byte, target deliveryCoverageTarget) (deliveryCoverageReport, error) {
	var report deliveryCoverageReport
	if len(raw) > deliveryCoverageMaxDocument || json.Unmarshal(raw, &report) != nil ||
		report.SchemaVersion != 1 || report.ManifestSHA != target.ManifestSHA || report.HourID != target.HourID ||
		report.BatchID != target.BatchID || report.Output != target.Output || report.VerifierSHA != deliveryCoverageVerifierSHA ||
		report.SupportSHA != deliveryCoverageSupportSHA || report.FFmpegSHA != deliveryCoverageFFmpegSHA ||
		report.FFprobeSHA != deliveryCoverageFFprobeSHA || len(report.Sources) != len(target.SourceIDs) {
		return report, errors.New("delivery verifier output differs from its exact inputs or reviewed tools")
	}
	for index, row := range report.Sources {
		if row.Ordinal != index+1 || row.Source.ID != target.SourceIDs[index] || row.Source.RecordingID != target.RecordingID ||
			row.Source.Bytes <= 0 || row.Source.Key == "" || row.Source.ETag == "" || row.Source.Bucket == "" ||
			len(row.Source.SHA) != 64 || !isLowerHex(row.Source.SHA) || !row.Source.End.After(row.Source.Start) {
			return report, errors.New("delivery verifier source identity differs")
		}
		switch row.Verdict {
		case deliveryCoverageProven:
			if !validDeliveryTrack("video", row.Tracks["video"]) || row.Reason != "" {
				return report, errors.New("proven source has no video evidence")
			}
			for kind, track := range row.Tracks {
				if !validDeliveryTrack(kind, track) || (kind == "audio" && (track.Primers == 1) != (index > 0)) {
					return report, errors.New("proven source has unsupported track evidence")
				}
			}
		case deliveryCoverageUnproven:
			if len(row.Tracks) > 0 || row.Reason == "" {
				return report, errors.New("unproven source carries ambiguous evidence")
			}
		default:
			return report, errors.New("unrecognized delivery media verdict")
		}
	}
	return report, nil
}

func storeDeliveryCoverage(ctx context.Context, tx pgx.Tx, target deliveryCoverageTarget, report deliveryCoverageReport, raw []byte) error {
	current, err := readDeliveryCoverageTarget(ctx, tx, target.OutputID)
	if err != nil {
		return err
	}
	if current.CollationHourID != target.CollationHourID || current.RecordingID != target.RecordingID ||
		current.ManifestKey != target.ManifestKey || current.ManifestSHA != target.ManifestSHA || current.Output != target.Output ||
		current.Bucket != target.Bucket || current.ETag != target.ETag ||
		!slices.Equal(current.SourceIDs, target.SourceIDs) {
		return errors.New("delivery catalog changed while proving coverage")
	}
	var runID int64
	err = tx.QueryRow(ctx, `
 INSERT INTO delivery_source_coverage_runs(output_id,collation_hour_id,source_clip_ids,manifest_object_key,manifest_sha256,evidence_sha256,media_evidence)
 VALUES ($1,$2,$3,$4,$5,$6,$7) ON CONFLICT(output_id,evidence_sha256) DO NOTHING RETURNING id`,
		target.OutputID, target.CollationHourID, target.SourceIDs, target.ManifestKey, target.ManifestSHA,
		fmt.Sprintf("%x", sha256.Sum256(raw)), raw).Scan(&runID)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil
	}
	if err != nil {
		return err
	}
	for _, row := range report.Sources {
		c := row.Source
		tag, err := tx.Exec(ctx, `
 INSERT INTO delivery_source_coverage_clips(run_id,clip_id,ordinal,bucket,object_key,etag,sha256,size_bytes,display_path,start_at,end_at,proven)
 SELECT $1,c.id,$3,c.bucket,c.object_key,c.etag,c.sha256,c.size_bytes,c.display_path,c.clip_start_at,c.clip_end_at,$12
 FROM recording_clips c WHERE c.id=$2 AND c.recording_id=$4 AND c.bucket=$5 AND c.object_key=$6
  AND c.etag=$7 AND c.sha256=$8 AND c.size_bytes=$9 AND c.clip_start_at=$10 AND c.clip_end_at=$11
  AND c.display_path<>''`, runID, c.ID, row.Ordinal, c.RecordingID, c.Bucket, c.Key, c.ETag, c.SHA, c.Bytes, c.Start, c.End,
			row.Verdict == deliveryCoverageProven)
		if err != nil {
			return err
		}
		if tag.RowsAffected() != 1 {
			return fmt.Errorf("raw source %d changed or lacks an exact catalog identity", c.ID)
		}
	}
	return nil
}

func runDeliveryCoverage(ctx context.Context, cfg config.Config, args []string) {
	fs := flag.NewFlagSet("delivery-source-coverage", flag.ExitOnError)
	output := fs.Int64("output-id", 0, "approved collation output ID")
	manifest := fs.String("manifest", "", "local exact catalog-pinned collation manifest")
	mediaDir := fs.String("media-dir", "", "directory with output.mp4 and raw-ID.mp4 files")
	verifier := fs.String("verifier-dir", "", "directory containing the exact reviewed verifier and support scripts")
	ffmpeg := fs.String("ffmpeg", "", "pinned FFmpeg executable")
	ffprobe := fs.String("ffprobe", "", "pinned FFprobe executable")
	record := fs.Bool("record", false, "append evidence after a new exact verification")
	fetch := fs.Bool("fetch-r2", false, "read exact held media and its manifest into private scratch")
	if err := fs.Parse(args); err != nil || fs.NArg() != 0 || *output <= 0 || (*fetch && (*manifest != "" || *mediaDir != "")) ||
		(!*fetch && (*manifest == "" || *mediaDir == "")) || *verifier == "" ||
		*ffmpeg == "" || *ffprobe == "" {
		log.Fatal("delivery-source-coverage --output-id ID (--fetch-r2 | --manifest FILE --media-dir DIR) --verifier-dir DIR --ffmpeg FILE --ffprobe FILE [--record]")
	}
	if err := deliveryCoverageCommand(ctx, cfg, *output, *manifest, *mediaDir, *verifier, *ffmpeg, *ffprobe, *record, *fetch); err != nil {
		log.Fatal(err)
	}
}

func deliveryCoverageCommand(ctx context.Context, cfg config.Config, output int64, manifest, mediaDir, verifier, ffmpeg, ffprobe string, record, fetch bool) error {
	ctx, cancel := context.WithTimeout(ctx, 3*time.Hour)
	defer cancel()
	pool, err := r2RetireOpenPool(ctx, cfg, !record)
	if err != nil {
		return err
	}
	defer pool.Close()
	target, err := readDeliveryCoverageTarget(ctx, pool, output)
	if err != nil {
		return err
	}
	if fetch {
		store, err := r2.New(ctx, archiveR2Config(cfg, pool))
		if err != nil {
			return err
		}
		scratch, err := os.MkdirTemp("", "stoarama-delivery-r2-")
		if err != nil {
			return err
		}
		defer os.RemoveAll(scratch)
		mediaDir = scratch
		manifest, err = fetchCoverageInputs(ctx, store, target, scratch)
		if err != nil {
			return err
		}
	}
	raw, err := verifyDeliveryCoverage(ctx, target, manifest, mediaDir, verifier, ffmpeg, ffprobe)
	if err != nil {
		return err
	}
	report, err := parseDeliveryCoverageReport(raw, target)
	if err != nil {
		return err
	}
	if record {
		tx, err := pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead})
		if err != nil {
			return err
		}
		defer tx.Rollback(ctx)
		if err := storeDeliveryCoverage(ctx, tx, target, report, raw); err != nil {
			return err
		}
		if err := tx.Commit(ctx); err != nil {
			return err
		}
	}
	_, err = os.Stdout.Write(raw)
	return err
}
