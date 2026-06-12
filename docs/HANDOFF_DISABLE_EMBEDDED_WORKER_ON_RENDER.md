# Disable Embedded Worker On Render

## What This Was Trying To Solve

This was trying to reduce Render web-service CPU usage and cost by stopping
`stoarama-api` from also running the embedded boxed-inference worker.

Today the Render web service does two things:

1. serves the API and web routes
2. runs the embedded `inferencebox` background worker when
   `BOX_WORKER_EMBEDDED=true`

That worker polls `inference_box_jobs`, renders boxed detection images, uploads
them to R2, and advances `queued_boxed` inference results toward `success`.

The proposed config/docs change was meant to keep the Render web process focused
on API traffic instead of also spending CPU on background boxed-artifact work.

## What It Was Not Trying To Solve

- capture ingest
- recording reliability
- routes or handlers
- GT import / CVAT flow
- autoresearch / HOTA tuning

## Key Code Facts

- [backend/cmd/stoarama-api/main.go](backend/cmd/stoarama-api/main.go):71
  starts the embedded inference-box worker only when
  `cfg.BoxWorkerEmbedded` is true.
- [backend/internal/config/config.go](backend/internal/config/config.go):100
  loads `BOX_WORKER_*` settings and defaults `BOX_WORKER_EMBEDDED` to `false`.
- [backend/internal/inferencebox/manager.go](backend/internal/inferencebox/manager.go):71
  runs the background worker loop that claims `inference_box_jobs`.
- [backend/internal/inferencebox/manager.go](backend/internal/inferencebox/manager.go):219
  renders boxed images, uploads them, and marks results complete.
- [backend/cmd/inference-box-worker/main.go](backend/cmd/inference-box-worker/main.go):21
  is the standalone worker path if boxed inference should run outside the web
  API process.

## Proposed Change

The stash hunk reviewed earlier was narrow and matched this scope:

- remove the `BOX_WORKER_*` env block from
  [render.yaml](render.yaml)
- change `BOX_WORKER_EMBEDDED=true` to `BOX_WORKER_EMBEDDED=false` in
  [docs/DEPLOY_RENDER.md](docs/DEPLOY_RENDER.md)

No Go code changes were part of that proposal.

## Recommendation

Do not apply this change blindly.

It is likely low-risk for raw capture and recording because those paths are
separate from the embedded box worker. But if the Render web service is
currently the only process consuming `inference_box_jobs`, then disabling the
embedded worker will leave boxed jobs stuck in `queued_boxed` and boxed
artifacts will stop being produced.

Recommended decision rule:

- If boxed images are optional right now and cost reduction matters, this
  change is reasonable.
- If boxed artifacts are still part of the current Stoarama UX or ops flow, do
  not disable the embedded worker unless a separate `inference-box-worker`
  service is already running.

## Bottom Line

This was trying to separate API serving from background boxed-inference work so
the Render web service would use less CPU.
