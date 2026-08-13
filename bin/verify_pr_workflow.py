#!/usr/bin/env python3
"""Strictly parse and run the frozen backend commands in the GitHub workflow."""

from __future__ import annotations

import argparse
import json
import os
import pathlib
import re
import shlex
import subprocess
import sys


DB_ENV = "STOARAMA_TEST_DATABASE_URL"
ALLOWED_ENV = {"PATH", "HOME", "TMPDIR", "LANG", "LC_ALL", "GOCACHE", "GOMODCACHE", "GOPATH"}
FROZEN_COMMANDS = (
    ("Test PostgreSQL type boundaries", "db"),
    ("Test NAS backlog eligibility", "db"),
    ("Test batch Plaza naming", "db"),
    ("Test recording qualification freeze transaction", "db"),
    ("Test recording clip authoritative frame provenance", "db"),
    ("Test recording health digest production query", "db"),
    ("Test native stitch migration, claim fencing, and planner races", "db"),
    ("Test presentation v2 C1 is structurally disabled and evidence is fenced", "db"),
    ("Test dynamic streak priority PostgreSQL schedule and plan", "db"),
    ("Test recording upload intent replay", "db"),
    ("Test continuous timestamp contract and canary are fail closed", "db"),
    ("Test migration lock timeout", "db"),
    ("Test frozen qualification cohort migration", "db"),
    ("Test continuous window revive rules", "db"),
    ("Test recorder lease gating, relay balancing, and source propagation", "db"),
    ("Test provisioning teardown lease safety", "db"),
    ("Test clip timestamp drift signal", "db"),
    ("Test recording health alert delivery", "db"),
    ("Test pre-open target selection SQL", "db"),
    ("Test reservation-backed relay pre-open evidence", "db"),
    ("Test transient ledger retention boundaries", "db"),
    ("Test billing meter report ledger", "db"),
    ("Test", "go"),
    ("Test NAS pull client", "python"),
    ("Vet", "vet"),
)
FROZEN_COMMAND_SHA256 = "066c7696d36a862c178df1fa9b3a5188c987f539f990c12cdb90b3a50aa0b6e8"
FROZEN_BACKEND_ENVELOPE_SHA256 = "ed8e14df9e4e62d7b796b99b480db16068baf9909a09b23e8807e9aabefedca3"


class WorkflowError(RuntimeError):
    pass


def scalar(value: str, context: str) -> str:
    value = value.strip()
    if not value or value[0] in "|>" or "${{" in value:
        raise WorkflowError(f"unsupported {context}")
    try:
        words = shlex.split(value) if value[0] in "'\"" else [value]
    except ValueError as exc:
        raise WorkflowError(f"invalid {context}: {exc}") from exc
    if len(words) != 1:
        raise WorkflowError(f"unsupported {context}")
    return words[0]


def put_once(target: dict[str, object], key: str, value: object, line_no: int) -> None:
    if key in target:
        raise WorkflowError(f"duplicate {key!r} at line {line_no}")
    target[key] = value


def parse_workflow(path: pathlib.Path) -> list[dict[str, object]]:
    lines = path.read_text(encoding="utf-8").splitlines()
    import hashlib
    try:
        envelope_start = lines.index("  backend:")
        envelope_end = lines.index("      - name: Test PostgreSQL type boundaries")
    except ValueError as exc:
        raise WorkflowError("workflow lacks the frozen backend job envelope") from exc
    envelope = ("\n".join(lines[envelope_start:envelope_end]) + "\n").encode()
    if hashlib.sha256(envelope).hexdigest() != FROZEN_BACKEND_ENVELOPE_SHA256:
        raise WorkflowError("jobs.backend runner/services/defaults/actions differ from the frozen envelope")
    seen_jobs = False
    seen_backend = False
    seen_defaults = False
    seen_run_defaults = False
    seen_steps = False
    default_workdir: str | None = None
    section = "root"
    steps: list[dict[str, object]] = []
    current: dict[str, object] | None = None
    in_step_env = False

    for line_no, raw in enumerate(lines, 1):
        if not raw.strip() or raw.lstrip().startswith("#"):
            continue
        indent = len(raw) - len(raw.lstrip(" "))
        if "\t" in raw[:indent]:
            raise WorkflowError(f"tabs are unsupported at line {line_no}")
        text = raw.strip()

        if indent == 0:
            if text == "backend:":
                raise WorkflowError(f"top-level backend key is forbidden at line {line_no}")
            section = "jobs" if text == "jobs:" else "root"
            current = None
            if text == "jobs:":
                if seen_jobs:
                    raise WorkflowError(f"duplicate jobs at line {line_no}")
                seen_jobs = True
            continue
        if section == "root":
            continue
        if indent == 2:
            section = "backend" if text == "backend:" else "other-job"
            current = None
            if text == "backend:":
                if seen_backend:
                    raise WorkflowError(f"duplicate jobs.backend at line {line_no}")
                seen_backend = True
            continue
        if not section.startswith("backend"):
            continue
        if indent == 4:
            current = None
            in_step_env = False
            if text == "defaults:":
                if seen_defaults:
                    raise WorkflowError(f"duplicate jobs.backend.defaults at line {line_no}")
                seen_defaults = True
                section = "backend-defaults"
            elif text == "steps:":
                if seen_steps:
                    raise WorkflowError(f"duplicate jobs.backend.steps at line {line_no}")
                seen_steps = True
                section = "backend-steps"
            elif text.startswith("env:"):
                raise WorkflowError("jobs.backend.env is forbidden")
            else:
                section = "backend"
            continue
        if section == "backend-defaults":
            if indent == 6 and text == "run:":
                if seen_run_defaults:
                    raise WorkflowError(f"duplicate jobs.backend.defaults.run at line {line_no}")
                seen_run_defaults = True
                section = "backend-run-defaults"
                continue
            if indent >= 6:
                raise WorkflowError(f"unsupported jobs.backend.defaults at line {line_no}")
        if section == "backend-run-defaults":
            if indent == 8 and text.startswith("working-directory:"):
                if default_workdir is not None:
                    raise WorkflowError(f"duplicate default working-directory at line {line_no}")
                default_workdir = scalar(text.split(":", 1)[1], f"working-directory at line {line_no}")
                continue
            if indent >= 8:
                raise WorkflowError(f"default shell/env is forbidden at line {line_no}")
        if section != "backend-steps":
            continue
        if indent == 6 and text.startswith("- "):
            current = {"env": {}, "uses": False}
            steps.append(current)
            in_step_env = False
            declaration = text[2:]
            if declaration.startswith("uses:"):
                current["uses"] = True
            elif declaration.startswith("name:"):
                put_once(current, "name", scalar(declaration.split(":", 1)[1], "step name"), line_no)
            else:
                raise WorkflowError(f"unsupported step declaration at line {line_no}")
            continue
        if current is None:
            raise WorkflowError(f"step field without step at line {line_no}")
        if indent == 8 and text.startswith("name:"):
            put_once(current, "name", scalar(text.split(":", 1)[1], "step name"), line_no)
            in_step_env = False
        elif indent == 8 and text.startswith("run:"):
            if current["uses"]:
                raise WorkflowError(f"step mixes uses and run at line {line_no}")
            put_once(current, "run", scalar(text.split(":", 1)[1], "run command"), line_no)
            in_step_env = False
        elif indent == 8 and text.startswith("working-directory:"):
            put_once(current, "working_directory", scalar(text.split(":", 1)[1], "working-directory"), line_no)
            in_step_env = False
        elif indent == 8 and text == "env:":
            if current["env"]:
                raise WorkflowError(f"duplicate step env at line {line_no}")
            in_step_env = True
        elif indent == 10 and in_step_env and ":" in text:
            key, value = text.split(":", 1)
            if key != DB_ENV:
                raise WorkflowError(f"unsupported run-step env {key!r} at line {line_no}")
            env = current["env"]
            assert isinstance(env, dict)
            put_once(env, key, scalar(value, "environment value"), line_no)
        elif indent >= 8 and current["uses"]:
            continue
        else:
            raise WorkflowError(f"unsupported backend step syntax at line {line_no}: {text}")

    if not (seen_jobs and seen_backend and seen_defaults and seen_run_defaults and seen_steps):
        raise WorkflowError("workflow lacks unique jobs.backend.defaults.run/steps ancestry")
    if default_workdir != "backend":
        raise WorkflowError("backend default working-directory must be exactly 'backend'")
    run_steps = [step for step in steps if "run" in step]
    observed = []
    for step in run_steps:
        name = str(step.get("name", ""))
        command = str(step["run"])
        try:
            argv = shlex.split(command)
        except ValueError as exc:
            raise WorkflowError(f"invalid command in {name!r}: {exc}") from exc
        if not argv or any(token in command for token in (";", "&&", "||", "`", "$(", "\n")):
            raise WorkflowError(f"unsupported command in {name!r}")
        env = step["env"]
        assert isinstance(env, dict)
        if env:
            kind = "db"
            if set(env) != {DB_ENV} or argv[:2] != ["go", "test"] or "-v" not in argv:
                raise WorkflowError(f"unsupported database step {name!r}")
        elif argv[:2] == ["go", "test"]:
            kind = "go"
        elif argv[:2] == ["go", "vet"]:
            kind = "vet"
        elif argv[:3] == ["python3", "-m", "unittest"]:
            kind = "python"
        else:
            raise WorkflowError(f"unsupported command in {name!r}")
        observed.append((name, kind))
        step["argv"] = argv
        step["kind"] = kind
    if tuple(observed) != FROZEN_COMMANDS:
        raise WorkflowError("backend run-step names/order/classes differ from the frozen 25-step gate")
    digest_payload = json.dumps(
        [(step["name"], step["kind"], step["argv"], step.get("working_directory") or "backend", bool(step["env"])) for step in run_steps],
        ensure_ascii=True,
        separators=(",", ":"),
    ).encode()
    if hashlib.sha256(digest_payload).hexdigest() != FROZEN_COMMAND_SHA256:
        raise WorkflowError("backend run-step argv differ from the frozen command digest")
    return run_steps


def clean_environment() -> dict[str, str]:
    env = {key: value for key, value in os.environ.items() if key in ALLOWED_ENV and value}
    for required in ("PATH", "HOME", "TMPDIR"):
        if not env.get(required):
            raise WorkflowError(f"sanitized environment lacks {required}")
    env.setdefault("LANG", "C.UTF-8")
    env.setdefault("LC_ALL", "C.UTF-8")
    env["GOENV"] = "off"
    env["GOTOOLCHAIN"] = "local"
    env["GOPROXY"] = "off"
    env["GOSUMDB"] = "off"
    env["PYTHONDONTWRITEBYTECODE"] = "1"
    return env


def run_steps(root: pathlib.Path, steps: list[dict[str, object]], database_url: str) -> None:
    if not re.fullmatch(r"postgres://stoarama@127\.0\.0\.1:[0-9]+/stoarama_test\?sslmode=disable", database_url):
        raise WorkflowError("test database URL is not the generated loopback form")
    base_env = clean_environment()
    for step in steps:
        name, kind = str(step["name"]), str(step["kind"])
        argv = list(step["argv"])
        workdir = str(step.get("working_directory") or "backend")
        if pathlib.PurePosixPath(workdir).is_absolute() or ".." in pathlib.PurePosixPath(workdir).parts:
            raise WorkflowError(f"unsafe working-directory in {name!r}")
        cwd = (root / workdir).resolve()
        if not cwd.is_relative_to(root.resolve()):
            raise WorkflowError(f"working-directory escapes repository in {name!r}")
        env = dict(base_env)
        if kind == "db":
            env[DB_ENV] = database_url
            argv.append("-json")
        print(f"verify-pr: {name}", flush=True)
        runner_env = {
            "PATH": env["PATH"],
            "STOARAMA_VERIFY_ARGV_JSON": json.dumps(argv),
            "STOARAMA_VERIFY_ENV_JSON": json.dumps(env),
            "STOARAMA_VERIFY_PGID_FILE": str(root / ".git" / "verify-pr-forbidden"),
        }
        configured_pgid_file = os.environ.get("STOARAMA_VERIFY_ACTIVE_PGID_FILE", "")
        if not configured_pgid_file:
            raise WorkflowError("runner PGID file is not configured")
        pgid_file = pathlib.Path(configured_pgid_file)
        try:
            pgid_file.unlink()
        except FileNotFoundError:
            pass
        runner_env["STOARAMA_VERIFY_PGID_FILE"] = str(pgid_file)
        completed = subprocess.run(
            [sys.executable, str(root / "bin" / "run_clean.py")], cwd=cwd, env=runner_env,
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
        )
        try:
            pgid_file.unlink()
        except FileNotFoundError:
            pass
        sys.stdout.write(completed.stdout)
        if completed.returncode:
            raise WorkflowError(f"workflow step failed: {name}")
        if kind == "db":
            events = []
            for line in completed.stdout.splitlines():
                try:
                    event = json.loads(line)
                except json.JSONDecodeError as exc:
                    raise WorkflowError(f"non-JSON go-test output in {name!r}") from exc
                if isinstance(event, dict):
                    events.append(event)
            packages = {event.get("Package") for event in events if event.get("Action") == "start" and event.get("Package")}
            requested_packages = {str(arg) for arg in step["argv"][2:] if str(arg).startswith("./")}
            matched_packages = {
                requested: {package for package in packages if str(package).endswith(requested[1:])}
                for requested in requested_packages
            }
            if any(len(matches) != 1 for matches in matched_packages.values()) or len(packages) != len(requested_packages):
                raise WorkflowError(f"database step package proof mismatch: {name}")
            for package in packages:
                runs = {event.get("Test") for event in events if event.get("Package") == package and event.get("Action") == "run" and event.get("Test") and "/" not in event.get("Test", "")}
                passed = {event.get("Test") for event in events if event.get("Package") == package and event.get("Action") == "pass" and event.get("Test")}
                skipped = {event.get("Test") for event in events if event.get("Package") == package and event.get("Action") == "skip" and event.get("Test")}
                warnings = [event.get("Output", "") for event in events if event.get("Package") == package]
                if not runs or not runs.issubset(passed) or skipped or any("no tests to run" in output for output in warnings):
                    raise WorkflowError(f"database step lacks per-package RUN/PASS proof: {name} {package}")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", required=True, type=pathlib.Path)
    parser.add_argument("--workflow", required=True, type=pathlib.Path)
    parser.add_argument("--database-url")
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()
    try:
        steps = parse_workflow(args.workflow)
        if args.check:
            print(f"verify-pr: parsed frozen {len(steps)} backend run steps")
        else:
            if not args.database_url:
                raise WorkflowError("--database-url is required")
            run_steps(args.root.resolve(), steps, args.database_url)
    except (OSError, WorkflowError) as exc:
        print(f"verify-pr: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
