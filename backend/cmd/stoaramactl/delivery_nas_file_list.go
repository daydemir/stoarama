package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"

	"github.com/daydemir/stoarama/backend/internal/api"
)

type deliveryNASFileLink struct {
	FormerURL                     string `json:"former_url"`
	FormerPinnedURL               string `json:"former_pinned_url"`
	FormerTicketURL               string `json:"former_ticket_url"`
	NASRelativePath               string `json:"nas_relative_path"`
	SizeBytes                     int64  `json:"size_bytes"`
	SHA256                        string `json:"sha256"`
	FormerDownloadURL             string `json:"former_download_url"`
	FormerPinnedDownloadURL       string `json:"former_pinned_download_url"`
	FormerDirectDownloadURL       string `json:"former_direct_download_url"`
	FormerPinnedDirectDownloadURL string `json:"former_pinned_direct_download_url"`
}

type deliveryNASFileList struct {
	SchemaVersion int                   `json:"schema_version"`
	Generation    string                `json:"delivery_generation"`
	Files         []deliveryNASFileLink `json:"files"`
}

func deliveryNASCollectFileList(ctx context.Context, db r2RetireQuerier, summary deliveryNASManifestSummary,
	keep api.DeliveryKeepSnapshot) (deliveryNASFileList, error) {
	list := deliveryNASFileList{SchemaVersion: 1, Generation: keep.PublicGeneration}
	if keep.PublicGeneration == "" {
		return list, errors.New("current public delivery generation is unavailable")
	}
	rows, err := db.Query(ctx, `
 SELECT output_id,recording_id,object_key,etag,sha256,size_bytes,nas_relative_path,device,root_inode,check_id
 FROM delivery_nas_handoff_certified
 WHERE roster_sha256=$1 AND picks_sha256=$2
 ORDER BY object_key`, summary.RosterSHA, summary.PicksSHA)
	if err != nil {
		return list, err
	}
	defer rows.Close()
	count, total := 0, int64(0)
	seen := make(map[string]bool)
	var mount [2]int64
	var observedCheck int64
	for rows.Next() {
		var outputID, recording, bytes int64
		var key, etag, sha, path string
		var current [2]int64
		var checkID int64
		if err := rows.Scan(&outputID, &recording, &key, &etag, &sha, &bytes, &path, &current[0], &current[1], &checkID); err != nil {
			return list, err
		}
		if count == 0 {
			mount, observedCheck = current, checkID
		} else if current != mount || checkID != observedCheck {
			return list, errors.New("the certified outputs span different mounted roots or NAS path checks")
		}
		files := keep.PublicFiles[key]
		if len(files) == 0 || !api.ValidCollatedNASPath(path) {
			return list, fmt.Errorf("output %d has no exact public file link or canonical NAS path", outputID)
		}
		for _, file := range files {
			o := file.Identity
			if o.ETag != etag || o.SHA256 != sha || o.Size != bytes || o.RecordingID != recording || seen[file.FileURL] {
				return list, fmt.Errorf("output %d public file identity differs or repeats", outputID)
			}
			seen[file.FileURL] = true
			list.Files = append(list.Files, deliveryNASFileLink{
				file.FileURL, file.PinnedFileURL, file.TicketURL, "joined/" + path, bytes, sha,
				file.DownloadURL, file.PinnedDownloadURL, file.DirectDownloadURL, file.PinnedDirectDownloadURL,
			})
		}
		count++
		total += bytes
	}
	if err := rows.Err(); err != nil {
		return list, err
	}
	if count != summary.Rows || total != summary.Bytes {
		return list, fmt.Errorf("current NAS readbacks and completed path check cover %d of %d outputs", count, summary.Rows)
	}
	sort.Slice(list.Files, func(i, j int) bool { return list.Files[i].FormerURL < list.Files[j].FormerURL })
	return list, nil
}

func writeDeliveryNASFileList(path string, list deliveryNASFileList) error {
	file, err := os.CreateTemp(filepath.Dir(path), ".delivery-file-list-*")
	if err != nil {
		return err
	}
	defer os.Remove(file.Name())
	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(list); err != nil {
		file.Close()
		return err
	}
	if err := file.Sync(); err != nil {
		file.Close()
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	return os.Link(file.Name(), path)
}
