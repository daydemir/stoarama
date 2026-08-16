#!/usr/bin/env bash
set -euo pipefail

# Deploy one exact merged commit in the only safe order for the split database
# credentials: exact migrator build -> successful migration run -> every
# unprivileged runtime -> API (which alone receives ADMISSION_DATABASE_URL).
# This script never reads or prints database values.

die() { printf 'deploy-with-migration: %s\n' "$*" >&2; exit 1; }
need() { command -v "$1" >/dev/null 2>&1 || die "missing required tool: $1"; }
need curl
need python3
need git

: "${RENDER_API_KEY:?RENDER_API_KEY is required}"
: "${RENDER_MIGRATOR_ID:?RENDER_MIGRATOR_ID is required}"
: "${RENDER_API_SERVICE_ID:?RENDER_API_SERVICE_ID is required}"
: "${RENDER_RUNTIME_SERVICE_IDS:?space-separated RENDER_RUNTIME_SERVICE_IDS is required}"
: "${COMMIT_SHA:?COMMIT_SHA is required}"
[[ "$COMMIT_SHA" =~ ^[0-9a-f]{40}$ ]] || die "COMMIT_SHA must be a full lowercase commit"
[[ "$RENDER_MIGRATOR_ID" == crn-* ]] || die "migration service must be a Render cron id"
[[ "$RENDER_API_SERVICE_ID" == srv-* ]] || die "API service must be a Render service id"

git fetch --quiet origin main
[[ "$(git rev-parse origin/main)" == "$COMMIT_SHA" ]] || die "commit is not exact current origin/main"

scratch="$(mktemp -d "${TMPDIR:-/tmp}/stoarama-render-migrate.XXXXXX")"
trap 'rm -rf "$scratch"' EXIT INT TERM HUP
auth=( -H "Authorization: Bearer ${RENDER_API_KEY}" -H 'Accept: application/json' )

api() {
  local method="$1" path="$2" output="$3" body="${4:-}"
  local args=( -fsS --show-error -X "$method" "${auth[@]}" -o "$output" )
  if [[ -n "$body" ]]; then
    args+=( -H 'Content-Type: application/json' --data "$body" )
  fi
  curl "${args[@]}" "https://api.render.com/v1${path}"
}

service_name() {
	local service="$1" out
	out="$scratch/service-${service}.json"
	api GET "/services/${service}" "$out"
	python3 - "$out" <<'PY'
import json,sys
d=json.load(open(sys.argv[1])); d=d.get("service",d)
name=d.get("name","")
if not name: raise SystemExit("Render service has no name")
print(name)
PY
}

check_env_keys() {
	local service="$1" kind="$2" out
	out="$scratch/env-${service}.json"
  api GET "/services/${service}/env-vars?limit=100" "$out"
  python3 - "$out" "$kind" <<'PY'
import json,sys
rows=json.load(open(sys.argv[1]))
keys={r.get("envVar",r).get("key") for r in rows}
kind=sys.argv[2]
common={"DATABASE_URL","STOARAMA_DATABASE_RUNTIME_ROLE","STOARAMA_DATABASE_ROLE_KIND","STOARAMA_ADMISSION_AUTHORITY_ROLE"}
if kind=="migrator":
    required={"MIGRATION_DATABASE_URL","RUNTIME_DATABASE_URL","ADMISSION_DATABASE_URL"}
    forbidden={"DATABASE_URL"}
elif kind=="api":
    required=common|{"ADMISSION_DATABASE_URL","STOARAMA_ADMISSION_EXECUTOR_ROLE"}
    forbidden={"MIGRATION_DATABASE_URL"}
else:
    required=common
    forbidden={"MIGRATION_DATABASE_URL","ADMISSION_DATABASE_URL"}
missing=required-keys
present=forbidden&keys
if missing or present:
    raise SystemExit("invalid %s env-key manifest; missing=%s forbidden_present=%s" %
                     (kind,sorted(missing),sorted(present)))
PY
}

deploy_exact() {
	local service="$1" response
	response="$scratch/deploy-${service}.json"
  api POST "/services/${service}/deploys" "$response" \
    "{\"clearCache\":\"do_not_clear\",\"commitId\":\"${COMMIT_SHA}\"}"
  local deploy_id
  deploy_id="$(python3 - "$response" "$COMMIT_SHA" <<'PY'
import json,sys
d=json.load(open(sys.argv[1]))
assert d.get("commit",{}).get("id")==sys.argv[2], "Render accepted a different commit"
assert d.get("id"), "Render returned no deploy id"
print(d["id"])
PY
)"
  local deadline=$((SECONDS+1800)) status=""
  while (( SECONDS < deadline )); do
    api GET "/services/${service}/deploys/${deploy_id}" "$response"
    status="$(python3 - "$response" "$COMMIT_SHA" <<'PY'
import json,sys
d=json.load(open(sys.argv[1])); d=d.get("deploy",d)
assert d.get("commit",{}).get("id")==sys.argv[2], "deploy commit changed"
print(d.get("status",""))
PY
)"
    [[ "$status" == live ]] && { printf '%s live %s\n' "$service" "${COMMIT_SHA:0:12}"; return; }
    [[ "$status" =~ ^(build_failed|update_failed|canceled|deactivated)$ ]] && die "$service deploy failed: $status"
    sleep 5
  done
  die "$service deploy timed out at status $status"
}

run_migration() {
  local started response="$scratch/migration-run.json" jobs="$scratch/migration-jobs.json"
  started="$(python3 -c 'import time; print(time.time())')"
  api POST "/cron-jobs/${RENDER_MIGRATOR_ID}/runs" "$response"
  local deadline=$((SECONDS+1800)) status=""
  while (( SECONDS < deadline )); do
    api GET "/services/${RENDER_MIGRATOR_ID}/jobs?limit=20" "$jobs"
    status="$(python3 - "$jobs" "$started" <<'PY'
import datetime,json,sys
rows=json.load(open(sys.argv[1])); start=float(sys.argv[2])
def epoch(v):
    if not v:return 0
    return datetime.datetime.fromisoformat(v.replace("Z","+00:00")).timestamp()
items=[r.get("job",r) for r in rows]
items=[r for r in items if epoch(r.get("createdAt"))>=start-2]
items.sort(key=lambda r:r.get("createdAt",""),reverse=True)
print(items[0].get("status","") if items else "")
PY
)"
    [[ "$status" == succeeded ]] && { printf 'migration succeeded %s\n' "${COMMIT_SHA:0:12}"; return; }
    [[ "$status" =~ ^(failed|canceled)$ ]] && die "migration run failed: $status"
    sleep 5
  done
  die "migration run timed out at status $status"
}

[[ "$(service_name "$RENDER_MIGRATOR_ID")" == "stoarama-db-migrate" ]] || die "migration id is not stoarama-db-migrate"
[[ "$(service_name "$RENDER_API_SERVICE_ID")" == "stoarama-api" ]] || die "API id is not stoarama-api"
runtime_names="$scratch/runtime-names"
: > "$runtime_names"
for service in $RENDER_RUNTIME_SERVICE_IDS; do
	[[ "$service" == srv-* || "$service" == crn-* ]] || die "invalid runtime service id: $service"
	[[ "$service" != "$RENDER_API_SERVICE_ID" && "$service" != "$RENDER_MIGRATOR_ID" ]] || die "duplicate service role"
	service_name "$service" >> "$runtime_names"
	done
python3 - "$runtime_names" <<'PY'
import sys
got=[line.strip() for line in open(sys.argv[1]) if line.strip()]
want=sorted([
  "stoarama-recorder-control", "stoarama-recording-health", "stoarama-recording-live-health",
  "stoarama-recording-health-summary", "stoarama-recording-preopen",
  "stoarama-recording-media-health", "stoarama-relay-connectivity",
])
if len(got)!=len(set(got)) or sorted(got)!=want:
    raise SystemExit("runtime service manifest is not exact; got=%s" % sorted(got))
PY

check_env_keys "$RENDER_MIGRATOR_ID" migrator
check_env_keys "$RENDER_API_SERVICE_ID" api
for service in $RENDER_RUNTIME_SERVICE_IDS; do
	check_env_keys "$service" runtime
done

deploy_exact "$RENDER_MIGRATOR_ID"
run_migration
for service in $RENDER_RUNTIME_SERVICE_IDS; do deploy_exact "$service"; done
deploy_exact "$RENDER_API_SERVICE_ID"
printf 'exact migration-first rollout complete %s\n' "$COMMIT_SHA"
