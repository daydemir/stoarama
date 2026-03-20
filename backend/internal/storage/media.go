package storage

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
)

type queryRower interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

type MediaObjectInput struct {
	StorageProvider string
	Bucket          string
	ObjectKey       string
	MIMEType        string
	SizeBytes       int64
	ETag            string
	SHA256          string
	Width           int
	Height          int
}

func UpsertMediaObject(ctx context.Context, db queryRower, in MediaObjectInput) (int64, error) {
	var id int64
	err := db.QueryRow(ctx, `
		INSERT INTO media_objects (storage_provider, bucket, object_key, mime_type, size_bytes, etag, sha256, width, height)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
		ON CONFLICT (bucket, object_key)
		DO UPDATE SET
			mime_type=EXCLUDED.mime_type,
			size_bytes=EXCLUDED.size_bytes,
			etag=EXCLUDED.etag,
			sha256=COALESCE(EXCLUDED.sha256, media_objects.sha256),
			width=COALESCE(EXCLUDED.width, media_objects.width),
			height=COALESCE(EXCLUDED.height, media_objects.height)
		RETURNING id
	`, in.StorageProvider, in.Bucket, in.ObjectKey, in.MIMEType, in.SizeBytes, in.ETag, in.SHA256, zeroToNil(in.Width), zeroToNil(in.Height)).Scan(&id)
	if err != nil {
		return 0, fmt.Errorf("upsert media object %s: %w", in.ObjectKey, err)
	}
	return id, nil
}

func zeroToNil(v int) any {
	if v <= 0 {
		return nil
	}
	return v
}
