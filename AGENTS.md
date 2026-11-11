# Stoarama map

Stoarama is the CLI-first product/control plane and a child of the Jellicomb
home (`daydemir/jellicomb`). `README.md` and `docs/ROADMAP.md` describe the
product model; private thesis research lives in `social-isolation`.

- `backend/cmd/stoarama-api/`: API; `backend/cmd/stoaramactl/`: operator CLI;
  `backend/cmd/stoarama/`: account/node CLI. Run either CLI with `--help` from
  `backend/`. Go packages and tests live under `backend/`.
- `infra/sql/migrations/`: schema; `render.yaml`: deployed services.
- `ops/jelly/README.md`: Jelly-only jobs and their service controls.
- `local/*.env`: gitignored runtime credentials; `.example` files show names.
- `docs/operations.md`: product workflow, credential locations, Render env/deploy and
  DigitalOcean spend controls. Read it before those operations.

Never initdb your own cluster; use `scripts/test-db run -- <tests>` from the repo
root (`../scripts/test-db` from `backend/`). For local Python/Go suite commands,
manual databases and cleanup, see README.md's “Local tests”. CI keeps its service
container and explicit `STOARAMA_TEST_DATABASE_URL`.

Keep credentials out of output and Git. Production env edits are single-key;
preserve the existing `STORAGE_CRED_KEY` and encrypted destination data.
