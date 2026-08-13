#!/usr/bin/env python3
"""Strictly parse and run the backend commands in the GitHub test workflow."""

from __future__ import annotations

import argparse
import os
import pathlib
import re
import shlex
import subprocess
import sys


DB_ENV = "STOARAMA_TEST_DATABASE_URL"
ALLOWED_ENV = {"HOME", "PATH", "TMPDIR", "LANG", "LC_ALL", "GOCACHE", "GOMODCACHE", "GOPATH"}


class WorkflowError(RuntimeError):
    pass


def scalar(value: str, context: str) -> str:
    value = value.strip()
    if not value or value[0] in "|>" or "${{" in value:
        raise WorkflowError(f"unsupported {context}")
    if value[0] in "'\"":
        try:
            words = shlex.split(value)
        except ValueError as exc:
            raise WorkflowError(f"invalid {context}: {exc}") from exc
        if len(words) != 1:
            raise WorkflowError(f"unsupported {context}")
        return words[0]
    return value


def parse_workflow(path: pathlib.Path) -> tuple[str, list[dict[str, object]]]:
    lines = path.read_text(encoding="utf-8").splitlines()
    in_backend = False
    in_steps = False
    default_workdir: str | None = None
    steps: list[dict[str, object]] = []
    current: dict[str, object] | None = None
    in_env = False

    for line_no, raw in enumerate(lines, 1):
        if not raw.strip() or raw.lstrip().startswith("#"):
            continue
        indent = len(raw) - len(raw.lstrip(" "))
        if "\t" in raw[:indent]:
            raise WorkflowError(f"tabs are unsupported at line {line_no}")
        text = raw.strip()

        if indent == 2 and text.endswith(":"):
            in_backend = text == "backend:"
            in_steps = False
            current = None
            continue
        if not in_backend:
            continue
        if indent == 8 and text.startswith("working-directory:") and not in_steps:
            default_workdir = scalar(text.split(":", 1)[1], f"working-directory at line {line_no}")
            continue
        if indent == 4 and text == "steps:":
            in_steps = True
            current = None
            continue
        if not in_steps:
            continue
        if indent == 6 and text.startswith("- "):
            current = {"name": "", "run": None, "env": {}, "working_directory": None, "uses": False}
            steps.append(current)
            in_env = False
            text = text[2:]
            if text.startswith("uses:"):
                current["uses"] = True
            elif text.startswith("name:"):
                current["name"] = scalar(text.split(":", 1)[1], f"step name at line {line_no}")
            else:
                raise WorkflowError(f"unsupported step declaration at line {line_no}")
            continue
        if current is None:
            raise WorkflowError(f"step field without step at line {line_no}")
        if indent == 8 and text.startswith("name:"):
            current["name"] = scalar(text.split(":", 1)[1], f"step name at line {line_no}")
            in_env = False
        elif indent == 8 and text.startswith("run:"):
            if current["uses"]:
                raise WorkflowError(f"step mixes uses and run at line {line_no}")
            current["run"] = scalar(text.split(":", 1)[1], f"run command at line {line_no}")
            in_env = False
        elif indent == 8 and text.startswith("working-directory:"):
            current["working_directory"] = scalar(
                text.split(":", 1)[1], f"working-directory at line {line_no}"
            )
            in_env = False
        elif indent == 8 and text == "env:":
            in_env = True
        elif indent == 10 and in_env and ":" in text:
            key, value = text.split(":", 1)
            if key != DB_ENV:
                raise WorkflowError(f"unsupported run-step env {key!r} at line {line_no}")
            current_env = current["env"]
            assert isinstance(current_env, dict)
            current_env[key] = scalar(value, f"environment value at line {line_no}")
        elif indent >= 8 and current["uses"]:
            # Action configuration is not executed locally.
            continue
        else:
            raise WorkflowError(f"unsupported backend step syntax at line {line_no}: {text}")

    if default_workdir != "backend":
        raise WorkflowError("backend default working-directory must be exactly 'backend'")

    run_steps = [step for step in steps if step["run"] is not None]
    if not run_steps:
        raise WorkflowError("backend workflow has no run steps")
    for step in run_steps:
        command = str(step["run"])
        try:
            argv = shlex.split(command)
        except ValueError as exc:
            raise WorkflowError(f"invalid command in {step['name']!r}: {exc}") from exc
        if not argv:
            raise WorkflowError(f"empty command in {step['name']!r}")
        allowed = argv[:2] in (["go", "test"], ["go", "vet"]) or argv[:3] == ["python3", "-m", "unittest"]
        if not allowed or any(token in command for token in (";", "&&", "||", "`", "$(", "\n")):
            raise WorkflowError(f"unsupported command in {step['name']!r}")
        env = step["env"]
        assert isinstance(env, dict)
        if env:
            if set(env) != {DB_ENV} or argv[:2] != ["go", "test"]:
                raise WorkflowError(f"unsupported database step {step['name']!r}")
            if "-v" not in argv:
                raise WorkflowError(f"database step {step['name']!r} must use go test -v")
        step["argv"] = argv
    return default_workdir, run_steps


def clean_environment() -> dict[str, str]:
    env = {key: value for key, value in os.environ.items() if key in ALLOWED_ENV and value}
    env.setdefault("PATH", "/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin")
    env.setdefault("HOME", str(pathlib.Path.home()))
    env.setdefault("TMPDIR", "/tmp")
    env.setdefault("LANG", "C.UTF-8")
    env.setdefault("LC_ALL", "C.UTF-8")
    return env


def run_steps(root: pathlib.Path, steps: list[dict[str, object]], database_url: str) -> None:
    if not re.fullmatch(r"postgres://stoarama@127\.0\.0\.1:[0-9]+/stoarama_test\?sslmode=disable", database_url):
        raise WorkflowError("test database URL is not the generated loopback form")
    base_env = clean_environment()
    for step in steps:
        name = str(step["name"])
        argv = list(step["argv"])
        workdir = str(step["working_directory"] or "backend")
        if pathlib.PurePosixPath(workdir).is_absolute() or ".." in pathlib.PurePosixPath(workdir).parts:
            raise WorkflowError(f"unsafe working-directory in {name!r}")
        cwd = (root / workdir).resolve()
        if not cwd.is_relative_to(root.resolve()):
            raise WorkflowError(f"working-directory escapes repository in {name!r}")
        env = dict(base_env)
        is_database = bool(step["env"])
        if is_database:
            env[DB_ENV] = database_url
        print(f"verify-pr: {name}", flush=True)
        completed = subprocess.run(
            argv,
            cwd=cwd,
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            check=False,
        )
        output = completed.stdout
        sys.stdout.write(output)
        if completed.returncode:
            raise WorkflowError(f"workflow step failed: {name}")
        if is_database:
            if "--- SKIP:" in output:
                raise WorkflowError(f"database step skipped a test: {name}")
            runs = re.findall(r"^=== RUN\s+(.+)$", output, flags=re.MULTILINE)
            passes = set(re.findall(r"^--- PASS:\s+([^ ]+)", output, flags=re.MULTILINE))
            if not runs or not all(run.split("/", 1)[0] in passes for run in runs if "/" not in run):
                raise WorkflowError(f"database step lacks RUN/PASS proof: {name}")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", required=True, type=pathlib.Path)
    parser.add_argument("--workflow", required=True, type=pathlib.Path)
    parser.add_argument("--database-url")
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()
    try:
        _, steps = parse_workflow(args.workflow)
        if args.check:
            print(f"verify-pr: parsed {len(steps)} backend run steps")
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
