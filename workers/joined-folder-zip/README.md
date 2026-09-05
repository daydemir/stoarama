# Joined-folder ZIP engine

`createJoinedZip(bucket, manifest)` preflights canonical R2 identities, then returns a backpressure-aware ZIP64 stream. It uses STORE entries and data descriptors, so media bytes never collect in Worker memory.

The caller supplies canonical manifest entries with `batch_id`, `sha256`, `etag`, `version_id`, `size_bytes`, `relative_path`, and `content_type`. The engine reads each object from `joined/<batch_id>/objects/<sha256>.mp4` with an exact ETag condition. It checks the version, ETag, key, and size before emitting that file's local header.

The archive contains the requested portable paths plus `joined-files.json`. Its public entries contain only `artifact_id`, `relative_path`, `content_type`, `size_bytes`, and `sha256`. Unsafe, Unicode-nonnormalized, and case-insensitive-colliding paths fail before any R2 read.

The bucket interface exposes only `head` and conditional `get`. This package cannot put, delete, or list objects.

The Worker signs manifest requests with an Ed25519 private key stored as the `BACKEND_SIGNING_PRIVATE_KEY` Worker secret; the backend receives only the public key. A Durable Object makes each capability single-use and limits each account to two concurrent downloads and ten starts per hour.

Run:

```sh
npm test
npm run typecheck
```
