# Security

## Secrets must never be committed

Real credentials (API keys, tokens, connection strings, private keys) never
belong in git. This repo is public.

- Local secret files (e.g. `local/render.env`, `local/do-capture.env`,
  `local/recording-supervisor.env`, `local/youtube-relay-source.env`) live only
  on your machine and are covered by `.gitignore`.
- Each has a committed `*.env.example` next to it listing the required keys with
  placeholder values (`__set_me__`). Copy the example, drop the `.example`
  suffix, and fill in real values locally.

### Layered protection

1. `.gitignore` blocks the known secret file patterns from ever being staged.
2. A repo-local `pre-push` hook runs [gitleaks](https://github.com/gitleaks/gitleaks)
   across the exact first-parent branch history before running the same
   PostgreSQL-backed and ordinary test commands as CI.
3. CI (`.github/workflows/gitleaks.yml`) runs gitleaks on every push and pull
   request and **fails the build** on any finding. This is the authoritative
   gate and cannot be bypassed by local configuration.

### One-time local setup

```bash
bin/install-dev-hooks
```

The verifier requires Docker Desktop to be installed through the supported host
bootstrap/Brewfile workflow and already running. It does not install, open, or
restart Docker. A complete pre-push run is intentionally about as expensive as
backend CI and may fetch `origin/main` and pull the immutable gitleaks and
PostgreSQL images. Run it manually at any time:

```bash
bin/verify-pr
```

The hook accepts deletion-only pushes. For content pushes it deliberately
requires one exact commit shared by every pushed ref, equal to a clean current
`HEAD`; check out and push other branches separately. It fails closed when the
Docker daemon, pinned images, origin history, or local tests are unavailable.

CI enforces the same scan regardless of whether you installed the hook, so a
missed local setup still cannot land a secret on the default branch.

### If a secret is ever exposed

Rotate it immediately at the provider (the value must be treated as public once
committed), then remove the file from tracking with
`git rm --cached <file>` and add its pattern to `.gitignore`.
