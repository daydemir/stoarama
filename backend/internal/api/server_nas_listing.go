package api

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"path"
	"sort"
	"strings"

	"github.com/daydemir/stoarama/backend/internal/util"
	"github.com/jackc/pgx/v5"
)

type nasListingAction string

const (
	nasListingStart          nasListingAction = "start"
	nasListingPage           nasListingAction = "page"
	nasListingFolderComplete nasListingAction = "folder_complete"
	nasListingComplete       nasListingAction = "complete"
)

type nasListingFile struct {
	Path    string `json:"path"`
	Size    int64  `json:"size"`
	CtimeNS int64  `json:"ctime_ns"`
	MtimeNS int64  `json:"mtime_ns"`
	Inode   int64  `json:"inode"`
	Device  int64  `json:"device"`
}
type nasListingRequest struct {
	Action     nasListingAction `json:"action"`
	Generation string           `json:"generation"`
	Folders    []string         `json:"folders"`
	Folder     string           `json:"folder"`
	Page       int              `json:"page"`
	Files      []nasListingFile `json:"files"`
	Count      int64            `json:"count"`
	Bytes      int64            `json:"bytes"`
}

// NASListingPathValid is the canonical namespace shared by proof producer and consumer.
func NASListingPathValid(p string) bool {
	return p != "" && p != "." && len(p) <= 4096 && path.Clean(p) == p && !strings.HasPrefix(p, "/") && p != ".." && !strings.HasPrefix(p, "../") && !strings.ContainsAny(p, "\\\x00")
}
func validateNASListing(req nasListingRequest) error {
	if len(req.Generation) != 32 || !lowerHex(req.Generation) || req.Page < 0 || req.Count < 0 || req.Bytes < 0 || len(req.Files) > 200 {
		return errors.New("invalid bounded listing")
	}
	switch req.Action {
	case nasListingStart:
		if len(req.Folders) < 1 || len(req.Folders) > 4096 || !sort.StringsAreSorted(req.Folders) {
			return errors.New("sorted top-level folders required")
		}
		root := false
		for i, f := range req.Folders {
			if f == "." {
				root = true
			} else if !NASListingPathValid(f) || strings.Contains(f, "/") {
				return errors.New("invalid top-level folder")
			}
			if i > 0 && f == req.Folders[i-1] {
				return errors.New("duplicate folder")
			}
		}
		if !root {
			return errors.New("root files folder required")
		}
	case nasListingPage, nasListingFolderComplete:
		if req.Folder != "." && (!NASListingPathValid(req.Folder) || strings.Contains(req.Folder, "/")) {
			return errors.New("invalid folder")
		}
		seen := map[string]bool{}
		for _, f := range req.Files {
			if !NASListingPathValid(f.Path) || f.Size < 0 || f.Inode < 0 || f.Device < 0 || seen[f.Path] || (req.Folder == "." && strings.Contains(f.Path, "/")) || (req.Folder != "." && !strings.HasPrefix(f.Path, req.Folder+"/")) {
				return errors.New("invalid listing file")
			}
			seen[f.Path] = true
		}
		if req.Action == nasListingFolderComplete && len(req.Files) != 0 {
			return errors.New("completion contains files")
		}
	case nasListingComplete:
	default:
		return errors.New("invalid listing action")
	}
	return nil
}

// All transitions lock the generation. Replayed pages must have identical
// content; duplicate paths, sequence gaps and inconsistent completions fail.
func storeNASListing(r *http.Request, tx pgx.Tx, cid int64, req nasListingRequest) error {
	ctx := r.Context()
	if req.Action == nasListingStart {
		if _, err := tx.Exec(ctx, `INSERT INTO nas_listing_generations(connection_id,generation,folders) VALUES($1,$2,$3) ON CONFLICT DO NOTHING`, cid, req.Generation, req.Folders); err != nil {
			return err
		}
	}
	var folders []string
	var complete bool
	if err := tx.QueryRow(ctx, `SELECT folders,completed_at IS NOT NULL FROM nas_listing_generations WHERE connection_id=$1 AND generation=$2 FOR UPDATE`, cid, req.Generation).Scan(&folders, &complete); err != nil {
		return err
	}
	if req.Action == nasListingStart {
		if strings.Join(folders, "\x00") != strings.Join(req.Folders, "\x00") {
			return errors.New("generation folder set changed")
		}
		_, err := tx.Exec(ctx, `INSERT INTO nas_listing_folders(connection_id,generation,folder) SELECT $1,$2,unnest($3::text[]) ON CONFLICT DO NOTHING`, cid, req.Generation, folders)
		return err
	}
	if complete {
		if req.Action == nasListingComplete {
			return nil
		}
		return errors.New("generation is already complete")
	}
	if req.Action == nasListingComplete {
		var valid bool
		if err := tx.QueryRow(ctx, `SELECT count(*)=cardinality($3::text[]) AND bool_and(f.completed_at IS NOT NULL AND f.file_count=(SELECT count(*) FROM nas_listing_files x WHERE x.connection_id=f.connection_id AND x.generation=f.generation AND x.folder=f.folder) AND f.total_bytes=(SELECT coalesce(sum(size_bytes),0) FROM nas_listing_files x WHERE x.connection_id=f.connection_id AND x.generation=f.generation AND x.folder=f.folder)) FROM nas_listing_folders f WHERE connection_id=$1 AND generation=$2`, cid, req.Generation, folders).Scan(&valid); err != nil {
			return err
		}
		if !valid {
			return errors.New("not every folder completed with consistent counts")
		}
		_, err := tx.Exec(ctx, `UPDATE nas_listing_generations SET completed_at=clock_timestamp() WHERE connection_id=$1 AND generation=$2`, cid, req.Generation)
		return err
	}
	var next int
	var count, bytes int64
	var done bool
	if err := tx.QueryRow(ctx, `SELECT next_page,file_count,total_bytes,completed_at IS NOT NULL FROM nas_listing_folders WHERE connection_id=$1 AND generation=$2 AND folder=$3`, cid, req.Generation, req.Folder).Scan(&next, &count, &bytes, &done); err != nil {
		return err
	}
	if req.Action == nasListingFolderComplete {
		if req.Page != next || req.Count != count || req.Bytes != bytes {
			return errors.New("folder counts/sequence differ")
		}
		_, err := tx.Exec(ctx, `UPDATE nas_listing_folders SET completed_at=coalesce(completed_at,clock_timestamp()) WHERE connection_id=$1 AND generation=$2 AND folder=$3`, cid, req.Generation, req.Folder)
		return err
	}
	raw, _ := json.Marshal(req.Files)
	sum := sha256.Sum256(raw)
	digest := hex.EncodeToString(sum[:])
	if req.Page < next {
		var old string
		if err := tx.QueryRow(ctx, `SELECT digest FROM nas_listing_pages WHERE connection_id=$1 AND generation=$2 AND folder=$3 AND page=$4`, cid, req.Generation, req.Folder, req.Page).Scan(&old); err != nil {
			return err
		}
		if digest != old {
			return errors.New("replayed page differs")
		}
		return nil
	}
	if done || req.Page != next || len(req.Files) == 0 {
		return errors.New("folder is complete, empty page or page gap")
	}
	for _, f := range req.Files {
		if _, err := tx.Exec(ctx, `INSERT INTO nas_listing_files(connection_id,generation,folder,relative_path,size_bytes,mtime_ns,inode,device,ctime_ns) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9)`, cid, req.Generation, req.Folder, f.Path, f.Size, f.MtimeNS, f.Inode, f.Device, f.CtimeNS); err != nil {
			return err
		}
		bytes += f.Size
	}
	if _, err := tx.Exec(ctx, `INSERT INTO nas_listing_pages VALUES($1,$2,$3,$4,$5)`, cid, req.Generation, req.Folder, req.Page, digest); err != nil {
		return err
	}
	_, err := tx.Exec(ctx, `UPDATE nas_listing_folders SET next_page=next_page+1,file_count=file_count+$4,total_bytes=$5 WHERE connection_id=$1 AND generation=$2 AND folder=$3`, cid, req.Generation, req.Folder, len(req.Files), bytes)
	return err
}
func (s *Server) handleAccountConnectionListing(w http.ResponseWriter, r *http.Request) {
	cid, ok := s.nasRestoreConnection(r.Context(), w, r)
	if !ok {
		return
	}
	var req nasListingRequest
	if util.DecodeJSON(r, &req) != nil || validateNASListing(req) != nil {
		util.WriteError(w, 400, "invalid listing request")
		return
	}
	var enabled bool
	// Completed starts/markers remain replayable after this one-shot job
	// disarms, including a lost completion response across client restarts.
	if err := s.pool.QueryRow(r.Context(), `SELECT nas_listing_enabled OR ($3::text IN ('start','complete') AND EXISTS(SELECT 1 FROM nas_listing_generations g WHERE g.connection_id=$1 AND g.generation=$2 AND g.completed_at IS NOT NULL)) FROM connections WHERE id=$1`, cid, req.Generation, string(req.Action)).Scan(&enabled); err != nil {
		util.WriteError(w, 503, "listing policy unavailable")
		return
	}
	if cid != 13 || !enabled {
		util.WriteError(w, 403, "listing is disabled")
		return
	}
	tx, err := s.pool.Begin(r.Context())
	if err != nil {
		util.WriteError(w, 503, "listing database unavailable")
		return
	}
	defer tx.Rollback(r.Context())
	if err = storeNASListing(r, tx, cid, req); err != nil {
		util.WriteError(w, 409, "listing transition rejected: "+err.Error())
		return
	}
	var complete bool
	if err = tx.QueryRow(r.Context(), `SELECT completed_at IS NOT NULL FROM nas_listing_generations WHERE connection_id=$1 AND generation=$2`, cid, req.Generation).Scan(&complete); err != nil {
		util.WriteError(w, 503, "listing state unavailable")
		return
	}
	if err = tx.Commit(r.Context()); err != nil {
		util.WriteError(w, 503, "listing commit failed")
		return
	}
	if req.Action == nasListingComplete {
		// After commit: do not invert the heartbeat connection/public-fence lock
		// order. The immutable completed proof remains even if the response is lost.
		if _, err := s.pool.Exec(r.Context(), `UPDATE connections SET nas_listing_enabled=false WHERE id=$1`, cid); err != nil {
			util.WriteError(w, 503, "listing disarm failed; retry completion")
			return
		}
	}
	util.WriteJSON(w, 200, map[string]any{"ok": true, "complete": complete})
}
