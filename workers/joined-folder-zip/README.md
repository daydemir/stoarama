# Joined folder ZIP Worker

This Worker streams stored joined MP4s into a ZIP64 archive. It has no R2 binding or storage credential, so it cannot upload, replace, or delete an object. Stoarama authorizes public and signed-in users through the same backend route, then redirects the browser with a five-minute, folder-scoped capability.

The Worker asks the backend for a bounded manifest using `BACKEND_WORKER_TOKEN`. The backend supplies method-bound, version-bound, `If-Match`-bound presigned HEAD and GET URLs for exact objects. The Worker checks every immutable R2 identity before returning headers and streams each object without buffering media. These read capabilities stay inside the Worker and never appear in the generated `joined-files.json`, which also omits R2 keys, batch IDs, ETags, versions, and credentials. CRC32 uses a statically imported, precompiled 1,231-byte WebAssembly module; the runtime never calls a forbidden dynamic WebAssembly compiler.

Deployment stays disabled until an operator:

1. Confirms the paid Workers plan supports the configured 10,000 subrequests and 300,000 ms CPU limit.
2. Sets the Worker secret `BACKEND_WORKER_TOKEN` to a new credential that matches the backend's `JOINED_ARCHIVE_WORKER_TOKEN`.
3. Confirms `wrangler deploy --dry-run` reports no R2 binding and only the archive limiter Durable Object plus non-secret backend URL.
4. Deploys the Worker to an HTTPS custom domain, then deploys the backend code ship-dark with all three archive variables empty.
5. Suspends backend deploys and sets `JOINED_ARCHIVE_WORKER_URL`, `JOINED_ARCHIVE_CAPABILITY_KEY`, and `JOINED_ARCHIVE_WORKER_TOKEN` through separate `PUT .../env-vars/KEY` calls. After every PUT, it prints keys only and confirms the archive keys written so far plus `STORAGE_CRED_KEY`, `R2_ACCESS_KEY_ID`, `R2_SECRET_ACCESS_KEY`, `R2_BUCKET`, `R2_ACCOUNT_ID`, and `DATABASE_URL` remain present. After the third PUT it explicitly confirms all three archive keys and all six protected keys before deploying.
6. Selects the Render deployment whose status is `live`, verifies `origin/main` matches that deployment's commit and no migration is unapplied, then triggers one backend deploy with `curl -f` and that exact `commitId`, following the repository `AGENTS.md` protocol.
7. Runs a small read-only canary and verifies the ZIP with an independent unzip tool before adding download links.

Local checks:

```sh
npm ci
npm run types
npm test
npm run typecheck
npm run dry-run
```
