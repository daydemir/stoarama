"""Local-only control transport harness for Go HTTP/TLS/PostgreSQL fixtures."""
import argparse
import concurrent.futures
import contextlib
import importlib.util
import io
import json
import signal
import sys
import threading
import urllib.error
import urllib.parse
from pathlib import Path
from types import SimpleNamespace


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", required=True)
    parser.add_argument("--ca-file")
    parser.add_argument("--timeout", type=float, default=2)
    args = parser.parse_args()
    url = urllib.parse.urlsplit(args.url)
    if url.scheme not in ("http", "https") or url.hostname not in ("127.0.0.1", "localhost", "::1") or url.username or url.query or url.fragment:
        raise ValueError("fixture URL must be loopback")
    requests = json.load(sys.stdin)
    if not isinstance(requests, list) or not 1 <= len(requests) <= 4:
        raise ValueError("fixture supports one to four requests")
    if any(not request["path"].startswith("/") or request["path"].startswith("//") for request in requests):
        raise ValueError("fixture requires relative API paths")
    spec = importlib.util.spec_from_file_location("copy_fixture_pull", Path(__file__).with_name("stoarama_pull.py"))
    pull = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(pull)
    pull.CLIENT_VERSION = "r2-copy-v3"
    if args.ca_file:
        # Public, ephemeral fixture trust, ONLY in this test harness. Production
        # has no trust override or fallback; its child source is unchanged.
        needle = "context=ssl.create_default_context()"
        if pull.R2_COPY_CONTROL_CHILD.count(needle) != 1:
            raise ValueError("fixture trust injection source changed")
        pull.R2_COPY_CONTROL_CHILD = pull.R2_COPY_CONTROL_CHILD.replace(
            needle, "context=ssl.create_default_context(cadata=%r)" % Path(args.ca_file).read_text())
    stop = threading.Event()
    signal.signal(signal.SIGTERM, lambda *_: stop.set())
    signal.signal(signal.SIGINT, lambda *_: stop.set())
    children = []
    popen = pull.subprocess.Popen
    def spawn(*a, **kw):
        process = popen(*a, **kw)
        children.append(process)
        return process
    pull.subprocess.Popen = spawn
    barrier = threading.Barrier(len(requests))
    cfg = SimpleNamespace(api_base=args.url, api_key="fixture-key")
    def run(index, request):
        with pull.R2CopyControl(cfg, stop, index) as control:
            control.start()
            barrier.wait(timeout=5)
            try:
                data = control.request_json(pull.R2CopyControlOperation(request["operation"]),
                                            request["path"], body=request.get("body"), timeout=args.timeout)
                task = data.get("task")
                return {"ok": True, "status": 200, "task_id": task["id"] if task else None, "active": data.get("active")}
            except urllib.error.HTTPError as error:
                return {"ok": False, "status": error.code, "error": type(error).__name__}
            except pull.R2CopyHold as error:
                return {"ok": False, "status": -1, "error": type(error).__name__}
    with contextlib.redirect_stdout(io.StringIO()):
        with concurrent.futures.ThreadPoolExecutor(max_workers=len(requests)) as pool:
            futures = [pool.submit(run, i, request) for i, request in enumerate(requests)]
            results = [future.result() for future in futures]
    lines, dropped = pull.CLIENT_LOG_RING.since(0)
    print(json.dumps({"results": results, "timings": [entry["message"] for entry in lines],
                      "dropped": dropped, "children": len(children),
                      "reaped": all(process.poll() is not None for process in children)}))


if __name__ == "__main__":
    main()
