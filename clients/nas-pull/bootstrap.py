import hashlib
import json
import os
import sys
import urllib.request


STATE = "/state"
CURRENT = STATE + "/stoarama_pull.py"
PREVIOUS = STATE + "/stoarama_pull.previous.py"
RUNTIME = STATE + "/runtime.json"
CANDIDATE = STATE + "/stoarama_pull.candidate.py"
RELEASE_BASE = "https://stoarama.com/nas/download/"


def atomic(path, data):
    temporary = path + ".new"
    with open(temporary, "wb") as output:
        output.write(data)
        output.flush()
        os.fsync(output.fileno())
    os.replace(temporary, path)


def valid_source(path):
    with open(path, "rb") as source_file:
        source = source_file.read()
    compile(source, path, "exec")
    return source


def fetch_latest():
    with urllib.request.urlopen(RELEASE_BASE + "latest.json", timeout=30) as response:
        manifest = json.load(response)
    artifact = str(manifest.get("artifact", ""))
    expected = str(manifest.get("sha256", "")).lower()
    if not artifact or "/" in artifact or "\\" in artifact or len(expected) != 64:
        raise RuntimeError("invalid NAS release manifest")
    with urllib.request.urlopen(RELEASE_BASE + artifact, timeout=30) as response:
        source = response.read()
    if hashlib.sha256(source).hexdigest() != expected:
        raise RuntimeError("NAS client checksum mismatch")
    compile(source, artifact, "exec")
    return source


def recover_previous():
    try:
        with open(RUNTIME, encoding="utf-8") as status_file:
            status = json.load(status_file)
    except (FileNotFoundError, ValueError, TypeError):
        return False
    if not isinstance(status, dict):
        return False
    if not os.path.exists(PREVIOUS) or status.get("exit") not in ("running", "self_update"):
        return False
    atomic(CURRENT, valid_source(PREVIOUS))
    try:
        os.unlink(CANDIDATE)
    except FileNotFoundError:
        pass
    print("NAS bootstrap restored previous client after unclean run", file=sys.stderr, flush=True)
    return True


def install_latest():
    try:
        source = fetch_latest()
    except Exception as exc:
        if not os.path.exists(CURRENT):
            raise
        valid_source(CURRENT)
        print("NAS bootstrap update skipped: %s" % exc, file=sys.stderr, flush=True)
        return
    try:
        previous = valid_source(CURRENT) if os.path.exists(CURRENT) else None
    except Exception:
        previous = None
    if previous == source:
        return
    if previous is not None:
        atomic(PREVIOUS, previous)
    atomic(CURRENT, source)


def main():
    os.makedirs(STATE, exist_ok=True)
    if not recover_previous():
        install_latest()
    os.execv(sys.executable, [sys.executable, CURRENT, "run"])


if __name__ == "__main__":
    main()
