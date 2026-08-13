#!/usr/bin/env bash
set -euo pipefail

source_root="$(git rev-parse --show-toplevel)"
test_root="$(mktemp -d "${TMPDIR:-/tmp}/stoarama-verify-pr-test.XXXXXX")"
cleanup() {
  local status=$?
  trap - EXIT INT TERM HUP
  rm -rf "$test_root"
  exit "$status"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
trap 'exit 129' HUP

fail() {
  printf 'test-verify-pr: %s\n' "$*" >&2
  exit 1
}

bare="$test_root/origin.git"
repo="$test_root/repo"
fake_bin="$test_root/fake-bin"
fake_log="$test_root/fake.log"
mkdir -p "$fake_bin"
git init --bare -q "$bare"
git init -q -b main "$repo"

mkdir -p "$repo/bin" "$repo/.githooks" "$repo/.github/workflows" "$repo/backend"
cp "$source_root/bin/verify-pr" "$repo/bin/verify-pr"
cp "$source_root/bin/verify_pr_workflow.py" "$repo/bin/verify_pr_workflow.py"
cp "$source_root/bin/install-dev-hooks" "$repo/bin/install-dev-hooks"
cp "$source_root/.githooks/pre-push" "$repo/.githooks/pre-push"
chmod +x "$repo/bin/"* "$repo/.githooks/pre-push"

cat >"$repo/.gitleaks.toml" <<'EOF'
title = "test"
EOF
cat >"$repo/.github/workflows/test.yml" <<'EOF'
name: test
jobs:
  backend:
    defaults:
      run:
        working-directory: backend
    steps:
      - name: DB proof
        env:
          STOARAMA_TEST_DATABASE_URL: postgres://stoarama@localhost:5432/stoarama_test?sslmode=disable
        run: go test ./... -run TestDB -count=1 -v
      - name: Test
        run: go test ./...
      - name: Vet
        run: go vet ./...
EOF
printf 'package fixture\n' >"$repo/backend/fixture.go"

cat >"$fake_bin/docker" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
printf 'docker %q ' "$@" >>"$FAKE_LOG"
printf '\n' >>"$FAKE_LOG"
case "${1:-}" in
  info)
    [[ "${FAKE_DOCKER_INFO_FAIL:-0}" != 1 ]]
    ;;
  image)
    exit 0
    ;;
  pull)
    exit 0
    ;;
  run)
    if [[ " $* " == *" --detach "* ]]; then
      printf 'fixture-container\n'
      exit 0
    fi
    range="${*: -1}"
    [[ " $* " == *" --log-opts=--no-merges --first-parent "* ]] || exit 91
    if [[ "$(git log -p "$range")" == *SECRET_HISTORY* ]]; then
      exit 2
    fi
    ;;
  port)
    printf '127.0.0.1:49123\n'
    ;;
  exec)
    exit 0
    ;;
  inspect)
    name="${*: -1}"
    printf '%s\n' "${name#stoarama-verify-pr-}"
    ;;
  rm)
    exit 0
    ;;
  *)
    exit 92
    ;;
esac
EOF
cat >"$fake_bin/go" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
[[ -z "${DATABASE_URL:-}" ]] || exit 81
if [[ "${1:-}" == test && " $* " == *" -v "* ]]; then
  [[ "${STOARAMA_TEST_DATABASE_URL:-}" == postgres://stoarama@127.0.0.1:49123/stoarama_test\?sslmode=disable ]] || exit 82
  printf '%s\n' '=== RUN   TestDB' '--- PASS: TestDB (0.00s)' 'PASS'
elif [[ "${1:-}" == test ]]; then
  [[ -z "${STOARAMA_TEST_DATABASE_URL:-}" ]] || exit 83
  [[ ! -e "${TMPDIR:-/tmp}/fail-go" ]] || exit 84
  printf 'ok fixture\n'
elif [[ "${1:-}" == vet ]]; then
  [[ -z "${STOARAMA_TEST_DATABASE_URL:-}" ]] || exit 85
else
  exit 86
fi
EOF
cat >"$fake_bin/gofmt" <<'EOF'
#!/usr/bin/env bash
exit 0
EOF
chmod +x "$fake_bin/"*

(
  cd "$repo"
  git config user.name verifier-test
  git config user.email verifier@example.invalid
  git add .
  git commit -qm base
  git remote add origin "$bare"
  git push -q -u origin main
  git switch -qc feature
  printf 'SECRET_HISTORY\n' > history.txt
  git add history.txt
  git commit -qm secret-like-history
  rm history.txt
  git add -u
  git commit -qm remove-from-tip
)

run_verify() {
  (
    cd "$repo"
    env -u DATABASE_URL -u STOARAMA_TEST_DATABASE_URL \
      PATH="$fake_bin:$PATH" FAKE_LOG="$fake_log" "$@"
  )
}

if run_verify bin/verify-pr >"$test_root/history.out" 2>&1; then
  fail "branch-history finding was not caught after removal from the tip"
fi
grep -q -- '--log-opts=--no-merges' "$fake_log" || fail "gitleaks first-parent flags were not passed"

(
  cd "$repo"
  git reset -q --hard origin/main
  printf 'safe\n' > safe.txt
  git add safe.txt
  git commit -qm safe-feature
)
: >"$fake_log"
run_verify bin/verify-pr >"$test_root/pass.out" 2>&1 || fail "clean verifier fixture failed"
grep -Eq 'docker rm .* -f .*stoarama-verify-pr-' "$fake_log" || {
  cat "$fake_log" >&2
  fail "PostgreSQL cleanup was not attempted"
}

if run_verify env DATABASE_URL=postgres://production.invalid bin/verify-pr >"$test_root/prod.out" 2>&1; then
  fail "inherited production database URL was accepted"
fi
if run_verify env FAKE_DOCKER_INFO_FAIL=1 bin/verify-pr >"$test_root/daemon.out" 2>&1; then
  fail "missing Docker daemon was accepted"
fi

: >"$fake_log"
mkdir -p "$test_root/fail-tmp"
touch "$test_root/fail-tmp/fail-go"
if run_verify env TMPDIR="$test_root/fail-tmp" bin/verify-pr >"$test_root/failure.out" 2>&1; then
  fail "failing generic Go suite was accepted"
fi
grep -Eq 'docker rm .* -f .*stoarama-verify-pr-' "$fake_log" || fail "failure path did not clean PostgreSQL"

(
  cd "$repo"
  bin/install-dev-hooks >/dev/null
  [[ "$(git config --local --get core.hooksPath)" == .githooks ]]
)

head_oid="$(git -C "$repo" rev-parse HEAD)"
zero=0000000000000000000000000000000000000000
printf 'refs/heads/feature %s refs/heads/feature %s\n' "$head_oid" "$zero" |
  run_verify .githooks/pre-push origin fixture-url >"$test_root/hook.out" 2>&1 || {
    cat "$test_root/hook.out" >&2
    fail "single clean HEAD push was rejected"
  }
printf '(delete) %s refs/heads/old %s\n' "$zero" "$head_oid" |
  run_verify .githooks/pre-push origin fixture-url >"$test_root/delete.out" 2>&1 || fail "deletion-only push was rejected"

(
  cd "$repo"
  git branch prior HEAD^
)
prior_oid="$(git -C "$repo" rev-parse prior)"
if printf 'refs/heads/feature %s refs/heads/feature %s\nrefs/heads/prior %s refs/heads/prior %s\n' \
    "$head_oid" "$zero" "$prior_oid" "$zero" |
    run_verify .githooks/pre-push origin fixture-url >"$test_root/multi.out" 2>&1; then
  fail "multi-OID push was accepted"
fi

bad_workflow="$test_root/bad.yml"
sed 's/go test \.\/\.\.\. -run TestDB -count=1 -v/go test .\/.\/ -run TestDB -count=1/' \
  "$repo/.github/workflows/test.yml" >"$bad_workflow"
if python3 "$repo/bin/verify_pr_workflow.py" --root "$repo" --workflow "$bad_workflow" --check >/dev/null 2>&1; then
  fail "database command without -v was accepted"
fi
sed 's/run: go vet \.\/\.\.\./run: echo bypass/' "$repo/.github/workflows/test.yml" >"$bad_workflow"
if python3 "$repo/bin/verify_pr_workflow.py" --root "$repo" --workflow "$bad_workflow" --check >/dev/null 2>&1; then
  fail "unsupported workflow command was accepted"
fi

printf 'test-verify-pr: PASS\n'
