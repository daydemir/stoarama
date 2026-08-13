#!/usr/bin/env bash
set -euo pipefail

source_root="$(git rev-parse --show-toplevel)"
test_root="$(mktemp -d "${TMPDIR:-/tmp}/stoarama-verify-pr-test.XXXXXX")"
real_home="$(mktemp -d /tmp/stoarama-vh.XXXXXX)"
cleanup() {
  local status=$?
  trap - EXIT INT TERM HUP
  [[ -z "${socket_pid:-}" ]] || kill "$socket_pid" 2>/dev/null || true
  [[ -z "${socket_pid:-}" ]] || wait "$socket_pid" 2>/dev/null || true
  find "$test_root" -depth -mindepth 1 -delete 2>/dev/null || true
  rmdir "$test_root" 2>/dev/null || true
  find "$real_home" -depth -mindepth 1 -delete 2>/dev/null || true
  rmdir "$real_home" 2>/dev/null || true
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
cp "$source_root/bin/run_clean.py" "$repo/bin/run_clean.py"
cp "$source_root/.githooks/pre-push" "$repo/.githooks/pre-push"
cp "$source_root/.github/workflows/test.yml" "$repo/.github/workflows/test.yml"
chmod +x "$repo/bin/"* "$repo/.githooks/pre-push"

cat >"$repo/.gitleaks.toml" <<'EOF'
title = "test"
EOF
printf 'package fixture\n' >"$repo/backend/fixture.go"

mkdir -p "$real_home/.docker/run"
socket_path="$real_home/.docker/run/docker.sock"
/usr/bin/python3 - "$socket_path" <<'PY' &
import socket,sys,time
s=socket.socket(socket.AF_UNIX)
s.bind(sys.argv[1])
s.listen()
time.sleep(300)
PY
socket_pid=$!
for _ in {1..50}; do [[ -S "$socket_path" ]] && break; sleep 0.02; done
[[ -S "$socket_path" ]] || fail "test Unix socket did not start"

cat >"$fake_bin/docker" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
printf 'docker %q ' "$@" >>"$FAKE_LOG"
printf '\n' >>"$FAKE_LOG"
case "${1:-}" in
  context)
    if [[ "${2:-}" == show ]]; then
      printf 'desktop-linux\n'
    elif [[ "${2:-}" == inspect ]]; then
      printf 'unix://%s\n' "$FAKE_DOCKER_SOCKET"
    else
      exit 93
    fi
    ;;
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
    if [[ "${*: -1}" == version ]]; then
      printf '8.24.3\n'
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
    if [[ "${FAKE_BLOCK_READY:-0}" == 1 ]]; then
      sleep 300
    fi
    exit 0
    ;;
  inspect)
    name="${*: -1}"
    if [[ " $* " != *" --format "* && -e "$FAKE_STATE_DIR/removed" ]]; then
      exit 1
    fi
    printf '%s\n' "${name#stoarama-verify-pr-}"
    ;;
  rm)
    [[ "${FAKE_CLEANUP_FAIL:-0}" != 1 ]] || exit 99
    touch "$FAKE_STATE_DIR/removed"
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
[[ "${GOENV:-}" == off && "${GOPROXY:-}" == off && "${GOSUMDB:-}" == off ]] || {
  [[ "${1:-}" == env ]] || exit 79
}
if [[ "${1:-}" == env ]]; then
  case "${2:-}" in
    GOCACHE) printf '%s\n' "${TMPDIR:-/tmp}/go-cache" ;;
    GOMODCACHE) printf '%s\n' "${TMPDIR:-/tmp}/go-mod-cache" ;;
    GOPATH) printf '%s\n' "${TMPDIR:-/tmp}/go-path" ;;
    *) exit 80 ;;
  esac
elif [[ "${1:-}" == test && " $* " == *" -json "* ]]; then
  [[ "${STOARAMA_TEST_DATABASE_URL:-}" == postgres://stoarama@127.0.0.1:49123/stoarama_test\?sslmode=disable ]] || exit 82
  [[ ! -e "$HOME/real-home-sentinel" ]] || exit 87
  gate_dir="${GOCACHE%/go-cache}"
  if [[ -e "$gate_dir/block-go" ]]; then
    printf '%s\n' $$ >"$gate_dir/blocked-go-pid"
    sleep 300
  fi
  packages=()
  for arg in "$@"; do [[ "$arg" == ./* ]] && packages+=("$arg"); done
  for index in "${!packages[@]}"; do
    arg="${packages[$index]}"
    if [[ -e "${TMPDIR:-/tmp}/omit-last" && "$index" -eq $((${#packages[@]} - 1)) ]]; then
      continue
    fi
    package="github.com/daydemir/stoarama/backend/${arg#./}"
    printf '{"Action":"start","Package":"%s"}\n' "$package"
    printf '{"Action":"run","Package":"%s","Test":"TestDB"}\n' "$package"
    printf '{"Action":"pass","Package":"%s","Test":"TestDB"}\n' "$package"
    printf '{"Action":"pass","Package":"%s"}\n' "$package"
  done
elif [[ "${1:-}" == test ]]; then
  [[ -z "${STOARAMA_TEST_DATABASE_URL:-}" ]] || exit 83
  [[ ! -e "${GOCACHE%/go-cache}/fail-go" ]] || exit 84
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
cat >"$fake_bin/python3" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
if [[ "${1:-}" == *verify_pr_workflow.py ]]; then
  exec /usr/bin/python3 "$@"
fi
[[ "${1:-}" == -m && "${2:-}" == unittest ]] || exit 88
[[ -z "${DATABASE_URL:-}${STOARAMA_TEST_DATABASE_URL:-}" ]] || exit 89
printf 'python tests ok\n'
EOF
chmod +x "$fake_bin/python3"

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
      PATH="$fake_bin:$PATH" HOME="$real_home" FAKE_LOG="$fake_log" FAKE_DOCKER_SOCKET="$socket_path" \
      FAKE_STATE_DIR="$test_root/fake-state" "$@"
  )
}
touch "$real_home/real-home-sentinel"

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
mkdir -p "$test_root/fake-state"
run_verify bin/verify-pr >"$test_root/pass.out" 2>&1 || {
  cat "$test_root/pass.out" >&2
  fail "clean verifier fixture failed"
}
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
if run_verify env DOCKER_HOST=tcp://remote.invalid bin/verify-pr >"$test_root/remote.out" 2>&1; then
  fail "remote Docker endpoint override was accepted"
fi

: >"$fake_log"
mkdir -p "$test_root/fail-tmp"
touch "$test_root/fail-tmp/fail-go"
if run_verify env TMPDIR="$test_root/fail-tmp" bin/verify-pr >"$test_root/failure.out" 2>&1; then
  fail "failing generic Go suite was accepted"
fi
grep -Eq 'docker rm .* -f .*stoarama-verify-pr-' "$fake_log" || fail "failure path did not clean PostgreSQL"

: >"$fake_log"
mkdir -p "$test_root/fake-state"
if run_verify env FAKE_CLEANUP_FAIL=1 bin/verify-pr >"$test_root/cleanup-fail.out" 2>&1; then
  fail "cleanup failure did not fail a successful run"
fi

env REPO="$repo" FAKE_BIN="$fake_bin" REAL_HOME="$real_home" FAKE_LOG="$fake_log" \
  FAKE_SOCKET="$socket_path" STATE_DIR="$test_root/fake-state" OUT_DIR="$test_root" \
  /usr/bin/python3 <<'PY'
import os,pathlib,signal,subprocess,time
repo=pathlib.Path(os.environ["REPO"])
log=pathlib.Path(os.environ["FAKE_LOG"])
state=pathlib.Path(os.environ["STATE_DIR"])
for signame in ("SIGINT", "SIGTERM", "SIGHUP"):
    log.write_text("")
    state.mkdir(exist_ok=True)
    for child in state.iterdir(): child.unlink()
    env={
        "PATH": os.environ["FAKE_BIN"]+":"+os.environ["PATH"],
        "HOME": os.environ["REAL_HOME"],
        "FAKE_LOG": str(log),
        "FAKE_DOCKER_SOCKET": os.environ["FAKE_SOCKET"],
        "FAKE_STATE_DIR": str(state),
        "FAKE_BLOCK_READY": "1",
    }
    out=open(pathlib.Path(os.environ["OUT_DIR"])/f"signal-{signame}.out","w")
    proc=subprocess.Popen(["bin/verify-pr"],cwd=repo,env=env,stdout=out,stderr=subprocess.STDOUT)
    for _ in range(200):
        if "pg_isready" in log.read_text(): break
        if proc.poll() is not None: raise SystemExit(f"{signame} fixture exited before readiness")
        time.sleep(.02)
    else: raise SystemExit(f"{signame} fixture never reached readiness")
    os.kill(proc.pid,getattr(signal,signame))
    if proc.wait(timeout=10)==0: raise SystemExit(f"{signame} unexpectedly succeeded")
    out.close()
    if " rm " not in log.read_text() or "stoarama-verify-pr-" not in log.read_text():
        raise SystemExit(f"{signame} did not clean PostgreSQL")
PY

(
  cd "$repo"
  bin/install-dev-hooks >/dev/null
  [[ "$(git config --local --get core.hooksPath)" == .githooks ]]
)

if (
  cd "$repo"
  GIT_CONFIG_GLOBAL="$test_root/global.gitconfig" git config --global core.hooksPath global-hooks
  GIT_CONFIG_GLOBAL="$test_root/global.gitconfig" git config --local --unset core.hooksPath
  GIT_CONFIG_GLOBAL="$test_root/global.gitconfig" bin/install-dev-hooks >/dev/null 2>&1
); then
  fail "conflicting global hooksPath was overwritten"
fi
(
  cd "$repo"
  git config --local --unset-all core.hooksPath 2>/dev/null || true
  mkdir -p .git/hooks
  printf '#!/bin/sh\nexit 0\n' >.git/hooks/pre-push
  chmod +x .git/hooks/pre-push
  if bin/install-dev-hooks >/dev/null 2>&1; then
    fail "existing legacy pre-push hook was disabled"
  fi
  unlink .git/hooks/pre-push
  bin/install-dev-hooks >/dev/null
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
perl -0pe 's/-count=1 -v/-count=1/' "$repo/.github/workflows/test.yml" >"$bad_workflow"
if python3 "$repo/bin/verify_pr_workflow.py" --root "$repo" --workflow "$bad_workflow" --check >/dev/null 2>&1; then
  fail "database command without -v was accepted"
fi
sed 's/run: go vet \.\/\.\.\./run: echo bypass/' "$repo/.github/workflows/test.yml" >"$bad_workflow"
if python3 "$repo/bin/verify_pr_workflow.py" --root "$repo" --workflow "$bad_workflow" --check >/dev/null 2>&1; then
  fail "unsupported workflow command was accepted"
fi

awk 'BEGIN{done=0} /jobs:/ && !done {print "backend:\n  steps:\n    - name: fake\n      run: go test ./..."; done=1} {print}' \
  "$repo/.github/workflows/test.yml" >"$bad_workflow"
if /usr/bin/python3 "$repo/bin/verify_pr_workflow.py" --root "$repo" --workflow "$bad_workflow" --check >/dev/null 2>&1; then
  fail "spoofed top-level backend ancestry was accepted"
fi
for mutation in \
  's/    runs-on: ubuntu-latest/    if: false\n    runs-on: ubuntu-latest/' \
  's/    runs-on: ubuntu-latest/    continue-on-error: true\n    runs-on: ubuntu-latest/' \
  's#image: postgres:17@sha256:[a-f0-9]*#image: postgres:17#'; do
  sed -E "$mutation" "$repo/.github/workflows/test.yml" >"$bad_workflow"
  if /usr/bin/python3 "$repo/bin/verify_pr_workflow.py" --root "$repo" --workflow "$bad_workflow" --check >/dev/null 2>&1; then
    fail "mutated backend job envelope was accepted"
  fi
done
perl -0pe 's/TestLockOrganizationPlazaIDsAcceptsIntegerAccountID/TestRecordingQualificationMigrationFreezesCompleteCohort/' \
  "$repo/.github/workflows/test.yml" >"$bad_workflow"
if /usr/bin/python3 "$repo/bin/verify_pr_workflow.py" --root "$repo" --workflow "$bad_workflow" --check >/dev/null 2>&1; then
  fail "mutated frozen -run command was accepted"
fi

mkdir -p "$test_root/omit-tmp"
touch "$test_root/omit-tmp/omit-last"
if env PATH="$fake_bin:$PATH" HOME="$real_home" TMPDIR="$test_root/omit-tmp" \
    STOARAMA_VERIFY_ACTIVE_PGID_FILE="$test_root/omit-tmp/active-pgid" \
    /usr/bin/python3 "$repo/bin/verify_pr_workflow.py" --root "$repo" \
    --workflow "$repo/.github/workflows/test.yml" \
    --database-url 'postgres://stoarama@127.0.0.1:49123/stoarama_test?sslmode=disable' \
    >"$test_root/zero-match.out" 2>&1; then
  fail "multi-package zero-match proof was accepted"
fi

gate_tmp="$test_root/gate-signal"
mkdir -p "$gate_tmp"
touch "$gate_tmp/block-go"
: >"$fake_log"
(
  cd "$repo"
  exec env -u DATABASE_URL -u STOARAMA_TEST_DATABASE_URL \
    PATH="$fake_bin:$PATH" HOME="$real_home" TMPDIR="$gate_tmp" FAKE_LOG="$fake_log" \
    FAKE_DOCKER_SOCKET="$socket_path" FAKE_STATE_DIR="$test_root/fake-state" bin/verify-pr
) >"$test_root/gate-signal.out" 2>&1 &
gate_pid=$!
for _ in {1..200}; do [[ -s "$gate_tmp/blocked-go-pid" ]] && break; sleep 0.02; done
[[ -s "$gate_tmp/blocked-go-pid" ]] || fail "blocked Go gate never started"
child_pid="$(cat "$gate_tmp/blocked-go-pid")"
kill -TERM "$gate_pid"
if wait "$gate_pid"; then fail "TERM during Go gate succeeded"; fi
for _ in {1..100}; do
  child_state="$(ps -o stat= -p "$child_pid" 2>/dev/null | tr -d ' ' || true)"
  [[ -z "$child_state" || "$child_state" == Z* ]] && break
  sleep 0.02
done
child_state="$(ps -o stat= -p "$child_pid" 2>/dev/null | tr -d ' ' || true)"
if [[ -n "$child_state" && "$child_state" != Z* ]]; then
  ps -o pid,ppid,pgid,sid,stat,command -p "$child_pid" >&2 || true
  fail "TERM left Go child running"
fi
grep -Eq 'docker rm .* -f .*stoarama-verify-pr-' "$fake_log" || fail "TERM during Go gate did not clean PostgreSQL"
sed '/      - name: Test PostgreSQL type boundaries/,/ -v$/d' "$repo/.github/workflows/test.yml" >"$bad_workflow"
if /usr/bin/python3 "$repo/bin/verify_pr_workflow.py" --root "$repo" --workflow "$bad_workflow" --check >/dev/null 2>&1; then
  fail "removed frozen workflow step was accepted"
fi

(
  cd "$repo"
  git tag -a annotated -m annotated HEAD
)
tag_oid="$(git -C "$repo" rev-parse annotated)"
if printf 'refs/tags/annotated %s refs/tags/annotated %s\n' "$tag_oid" "$zero" |
    run_verify .githooks/pre-push origin fixture-url >"$test_root/tag.out" 2>&1; then
  fail "annotated tag object was accepted"
fi

kill "$socket_pid" 2>/dev/null || true
wait "$socket_pid" 2>/dev/null || true
socket_pid=""

printf 'test-verify-pr: PASS\n'
