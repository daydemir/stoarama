package main

import (
	"context"
	"io"

	"github.com/daydemir/stoarama/backend/internal/r2"
	"github.com/daydemir/stoarama/backend/internal/webdav"
)

// objectStore is the single seam over the clip-transfer TARGET. It is the only
// place a destination's transport varies: the transfer SOURCE is always managed
// R2, while the target is either an S3/R2 bucket or a WebDAV destination. r2.Client
// satisfies this verbatim (Open/PutMultipart/Head/DeleteObjects already exist);
// webdavStore adapts the webdav.Client to the same shape. The capture/presign path
// is S3-only and never touches this seam.
type objectStore interface {
	// PutMultipart streams body to key. For S3/R2 it is a real multipart upload; for
	// WebDAV it is a single streamed PUT. The returned ETag is ignored by copyClip.
	PutMultipart(ctx context.Context, key, contentType string, body io.Reader) (string, error)
	// Head returns the object's real byte size after the write.
	Head(ctx context.Context, key string) (r2.ObjectHead, error)
}

// webdavStore adapts webdav.Client to objectStore. webdav.Head returns its own
// ObjectHead; this converts it to r2.ObjectHead so copyClip's size handling is
// identical for both targets.
type webdavStore struct {
	c *webdav.Client
}

func (w webdavStore) PutMultipart(ctx context.Context, key, contentType string, body io.Reader) (string, error) {
	return w.c.PutMultipart(ctx, key, contentType, body)
}

func (w webdavStore) Head(ctx context.Context, key string) (r2.ObjectHead, error) {
	h, err := w.c.Head(ctx, key)
	if err != nil {
		return r2.ObjectHead{}, err
	}
	return r2.ObjectHead{SizeBytes: h.SizeBytes}, nil
}
