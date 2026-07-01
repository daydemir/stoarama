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
2. A local `pre-commit` hook runs [gitleaks](https://github.com/gitleaks/gitleaks)
   before each commit and rejects any commit containing a secret.
3. CI (`.github/workflows/gitleaks.yml`) runs gitleaks on every push and pull
   request and **fails the build** on any finding. This is the authoritative
   gate and cannot be bypassed by local configuration.

### One-time local setup

```bash
pip install pre-commit   # or: brew install pre-commit
pre-commit install       # installs the git hook in this repo
```

After that, gitleaks runs automatically on every `git commit`. You can run it
manually at any time:

```bash
pre-commit run gitleaks --all-files
```

CI enforces the same scan regardless of whether you installed the hook, so a
missed local setup still cannot land a secret on the default branch.

### If a secret is ever exposed

Rotate it immediately at the provider (the value must be treated as public once
committed), then remove the file from tracking with
`git rm --cached <file>` and add its pattern to `.gitignore`.
