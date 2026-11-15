#!/usr/bin/env python3
"""Bounded historical invoice evidence validation; never writer-death authority."""
import argparse
import datetime as dt
import hashlib
import json
import math
import os
from pathlib import Path
import re
import stat
import sys
from enum import Enum
import signal
import select
import threading
import time
from urllib.parse import parse_qsl, urlsplit


TEAM = "b6bd57e6-cee4-42fa-98f0-5527fd4fde4a"
PINS_SHA = "29e1cb516623ad635a48dc32101e61f1f0b7a1501a117c6770b353cca37231ab"
BODY_LIMIT = 2 * 1024 * 1024
PAGES = (
    ("02", "de98b6d9-2845-4b98-99f0-1aabd6e07137", 1, 1, 1),
    ("03", "fd89df7a-9f61-4889-bd1d-8206147b9b2c", 1, 12, 12),
    ("04", "aa776d26-9dc2-41df-ad86-52bab734b089", 1, 36, 36),
    ("08", "c6761b91-0242-4d5e-8a88-30666e4a0569", 2, 78, 278),
    ("09", "2616d657-8cdc-4f23-8132-15019df2c424", 2, 55, 255),
)
COMPLETE_MONTHS = {"05": 5, "06": 47, "07": 57, "10": 102, "11": 38}
BASE_FIELDS = frozenset(("product", "category", "description", "amount", "duration", "duration_unit"))
OPTIONAL_FIELDS = frozenset(("start_time", "end_time", "project_name", "resource_id", "resource_uuid", "group_description"))
BASE_FILES = frozenset(("account.json", "capture-lifetimes.json")) | frozenset(
    "invoice-full-2026-%s.json" % p[0] for p in PAGES
) | frozenset("invoice-2026-%s.json" % m for m in ("05", "06", "07", "08", "09", "10", "11"))
REMOTE_BASE = Path("/Users/jelly/stoarama-evidence/do-capture-provenance-20261115/ay-team")
REMOTE_PINS = Path("/Users/jelly/stoarama-evidence/capture-invoice-gaps-20261115/baseline-pins.json")
REMOTE_OUT = Path("/Users/jelly/stoarama-evidence/do-capture-invoice-gaps-20261115T1000Z")
RUN_SECONDS = 55


class Outcome(str, Enum):
    COMPLETE = "complete"
    BASELINE = "baseline_refused"
    REQUEST = "request_failed"
    RESPONSE = "response_refused"
    COVERAGE = "coverage_refused"
    TIMEOUT = "timeout"
    INTERRUPTED = "interrupted"


class Stage(str, Enum):
    BASELINE = "baseline"
    REQUEST = "request"
    RESPONSE = "response"
    VALIDATION = "offline_validation"


class Cancelled(BaseException):
    pass


class EvidenceError(ValueError):
    pass


def require(condition, message):
    if not condition:
        raise EvidenceError(message)


def strict_json(raw):
    def unique(pairs):
        result = {}
        for key, value in pairs:
            require(key not in result, "duplicate JSON field")
            result[key] = value
        return result

    def nonfinite(_):
        raise EvidenceError("nonfinite JSON")

    def number(value):
        result = float(value)
        require(math.isfinite(result), "nonfinite JSON number")
        return result

    return json.loads(raw, object_pairs_hook=unique, parse_constant=nonfinite, parse_float=number)


class Deadline:
    """One absolute monotonic budget; preparation and exec cannot renew it."""
    SIGNALS = (signal.SIGALRM, signal.SIGTERM, signal.SIGINT, signal.SIGHUP)

    def __init__(self, start, end):
        require(type(start) in (int, float) and type(end) in (int, float)
                and math.isfinite(start) and math.isfinite(end)
                and start < end <= start + RUN_SECONDS, "attempt deadline fields")
        self.start, self.end = start, end
        # Reserve cleanup INSIDE the existing hard cap, never after it. Tests
        # use shorter synthetic budgets; production keeps 50s work + <=5s
        # cleanup within the same immutable 55s deadline.
        self.work_end = end - min(5, (end - start) / 3)
        self.cleaning = False
        self.outcome = Outcome.INTERRUPTED

    def remaining(self):
        return max(0, self.end - time.monotonic())

    def check_hard(self):
        if not self.start <= time.monotonic() < self.end:
            self.outcome = Outcome.TIMEOUT
            raise Cancelled()

    def begin_cleanup(self):
        self.cleaning = True
        self.check_hard()
        signal.setitimer(signal.ITIMER_REAL, self.remaining())

    def check(self):
        self.check_hard()
        if not self.cleaning and time.monotonic() >= self.work_end:
            self.outcome = Outcome.TIMEOUT
            self.begin_cleanup()
            raise Cancelled()

    def stop(self, sig, _):
        self.outcome = Outcome.TIMEOUT if sig == signal.SIGALRM else Outcome.INTERRUPTED
        self.begin_cleanup()  # The second alarm is still the original hard end.
        raise Cancelled()

    def __enter__(self):
        self.previous = {sig: signal.getsignal(sig) for sig in self.SIGNALS}
        for sig in self.SIGNALS:
            signal.signal(sig, self.stop)
        try:
            self.check()
            signal.setitimer(signal.ITIMER_REAL, self.work_end - time.monotonic())
            self.check()
            return self
        except BaseException:
            self.__exit__(None, None, None)
            raise

    def __exit__(self, *_):
        signal.setitimer(signal.ITIMER_REAL, 0)
        for sig, handler in self.previous.items():
            signal.signal(sig, handler)


class ChannelWatch:
    """EOF, unexpected input or an unreadable owned channel stops this process."""
    def __init__(self, channel):
        self.channel = channel
        self.finished = threading.Event()
        self.disconnected = threading.Event()
        self.thread = threading.Thread(target=self.watch, name="capture-evidence-channel")

    def watch(self):
        violated = False
        try:
            while not self.finished.is_set():
                readable, _, _ = select.select([self.channel], [], [], 0.05)
                if readable:
                    violated = True  # Never consume input or discard observation.
                    break
        except Exception:
            violated = True
        if violated:
            self.disconnected.set()  # Shutdown suppresses only the late signal.
            if not self.finished.is_set():
                os.kill(os.getpid(), signal.SIGTERM)

    def start(self):
        self.thread.start()

    def close(self, deadline=None):
        self.finished.set()
        self.thread.join(timeout=min(0.5, deadline.remaining()) if deadline else 0.5)
        require(not self.thread.is_alive(), "channel watcher exit unproved; HOLD")


def canonical(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()


def emit_result(value):
    raw = canonical(value) + b"\n"
    require(os.write(sys.stdout.fileno(), raw) == len(raw), "partial stdout result; HOLD")


def contains_secret(value, secret):
    if isinstance(value, str):
        return secret in value
    if isinstance(value, list):
        return any(contains_secret(item, secret) for item in value)
    if isinstance(value, dict):
        return any(contains_secret(key, secret) or contains_secret(item, secret) for key, item in value.items())
    return False


def digest(raw):
    return hashlib.sha256(raw).hexdigest()


def now():
    return dt.datetime.now(dt.timezone.utc).isoformat()


def write_new(path, raw):
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    try:
        # No BufferedWriter: cancellation must not trigger another flush from
        # close(), deallocation or interpreter shutdown. Partial writes refuse.
        require(os.write(fd, raw) == len(raw), "partial artifact write; HOLD")
        os.fsync(fd)
    finally:
        os.close(fd)
    directory = os.open(Path(path).parent, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(directory)
    finally:
        os.close(directory)


def read_regular(path, limit):
    path = Path(path)
    require(path.is_absolute() and path.parent.resolve(strict=True) == path.parent, "noncanonical artifact parent")
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
    try:
        before = os.fstat(fd)
        require(stat.S_ISREG(before.st_mode) and 0 < before.st_size <= limit, "artifact type or size")
        raw = bytearray()
        while len(raw) <= limit:
            chunk = os.read(fd, min(65536, limit + 1 - len(raw)))
            if not chunk:
                break
            raw.extend(chunk)
        after = os.fstat(fd)
        fields = ("st_dev", "st_ino", "st_mode", "st_size", "st_mtime_ns", "st_ctime_ns")
        require(tuple(getattr(before, k) for k in fields) == tuple(getattr(after, k) for k in fields), "artifact changed during read")
        require(len(raw) == before.st_size and len(raw) <= limit, "artifact read bound")
        return bytes(raw)
    finally:
        os.close(fd)


def account(body):
    require(type(body) is dict and type(body.get("account")) is dict, "account body")
    team = body["account"].get("team")
    require(type(team) is dict and team.get("uuid") == TEAM and team.get("name") == "AY", "original AY team identity differs")


def items(value):
    require(type(value) is list, "invoice item list")
    for row in value:
        require(type(row) is dict and BASE_FIELDS <= row.keys() <= BASE_FIELDS | OPTIONAL_FIELDS, "invoice item fields")
        require(all(type(v) is str for v in row.values()), "invoice item value type")
    return value


def page(body, invoice, number, count, total):
    require(type(body) is dict and set(body) == {"invoice_items", "meta", "links"}, "invoice response fields")
    require(type(body["meta"]) is dict and set(body["meta"]) == {"total"}
            and type(body["meta"]["total"]) is int and body["meta"]["total"] == total, "invoice total differs")
    require(len(items(body["invoice_items"])) == count, "invoice page count differs")
    links = body["links"]
    require(type(links) is dict and set(links) <= {"pages"}, "invoice links")
    pages = links.get("pages", {})
    require(type(pages) is dict and set(pages) <= {"first", "prev", "next", "last"}, "invoice page links")
    terminal = count + (number - 1) * 200 == total
    require(("next" not in pages) == terminal, "invoice terminal link differs")
    last = (total + 199) // 200
    targets = {"first": 1, "prev": number - 1, "next": number + 1, "last": last}
    for name, url in pages.items():
        require(type(url) is str, "invoice link type")
        parsed = urlsplit(url)
        require(parsed.scheme == "https" and parsed.netloc == "api.digitalocean.com" and not parsed.fragment, "invoice link authority")
        require(parsed.path in ("/v2/customers/my/invoices/" + invoice, "/v2/customers/" + TEAM + "/invoices/" + invoice), "invoice link account or invoice")
        pairs = parse_qsl(parsed.query, keep_blank_values=True, strict_parsing=True)
        require(len(pairs) == 2 and dict(pairs) == {"page": str(targets[name]), "per_page": "200"}, "invoice link offset")
        require(1 <= targets[name] <= last, "invoice link page range")
    return body["invoice_items"]


def load_baseline(root, pins_path):
    raw = read_regular(pins_path, 65536)
    require(digest(raw) == PINS_SHA, "baseline pin manifest differs")
    pins = strict_json(raw)
    require(type(pins) is dict and set(pins) == BASE_FILES, "baseline file membership")
    baseline = {}
    for name, pin in pins.items():
        require(type(pin) is dict and set(pin) == {"sha256", "size_bytes"}, "baseline file pin fields")
        raw = read_regular(Path(root) / name, BODY_LIMIT)
        require(type(pin["size_bytes"]) is int and len(raw) == pin["size_bytes"] and digest(raw) == pin["sha256"], "baseline file identity differs")
        baseline[name] = strict_json(raw)
    account(baseline["account.json"]["body"])
    for month, invoice, number, count, total in PAGES:
        assembled = baseline["invoice-full-2026-%s.json" % month]
        require(type(assembled) is dict and assembled.get("invoice_uuid") == invoice, "assembled invoice identity")
        require(len(items(assembled["invoice_items"])) == total, "assembled invoice count")
        if number == 2:
            prefix = page(baseline["invoice-2026-%s.json" % month]["body"], invoice, 1, 200, total)
            require(prefix == assembled["invoice_items"][:200], "retained page1 differs from assembled prefix")
    for month, count in COMPLETE_MONTHS.items():
        envelope = baseline["invoice-2026-%s.json" % month]
        require(envelope.get("status") == 200 and type(envelope.get("request_id")) is str and envelope["request_id"], "retained complete envelope")
        path = urlsplit(envelope["path"]).path
        invoice = path.rsplit("/", 1)[1]
        require(re.fullmatch(r"[0-9a-f-]{36}", invoice), "retained invoice UUID")
        page(envelope["body"], invoice, 1, count, count)
    return baseline


def requests():
    result = [("account-before", "/v2/account")]
    result.extend(("invoice-2026-%s-page%d" % (month, number), "/v2/customers/my/invoices/%s?per_page=200&page=%d" % (invoice, number)) for month, invoice, number, _, _ in PAGES)
    return result + [("account-after", "/v2/account")]


def utc(value):
    require(type(value) is str and value.endswith(("Z", "+00:00")), "UTC timestamp")
    return dt.datetime.fromisoformat(value.replace("Z", "+00:00"))


def validate_capture(root, baseline):
    """Reparse raw source bytes; no credentials, network or death assertions."""
    expected_files = {name + suffix for name, _ in requests() for suffix in (".json", ".body.json")}
    actual_files = {p.name for p in Path(root).iterdir()}
    require(expected_files <= actual_files <= expected_files | {"attempt.json", "started.json", "manifest.json", "SHA256SUMS"}, "capture file membership differs")
    responses = []
    previous = None
    total_bytes = 0
    for index, (name, path) in enumerate(requests()):
        envelope_raw = read_regular(Path(root) / (name + ".json"), 4 * BODY_LIMIT)
        envelope = strict_json(envelope_raw)
        require(type(envelope) is dict and set(envelope) == {"schema", "request_name", "started_at", "completed_at", "method", "path", "status", "request_id", "date", "body", "parsed_body"}, "response envelope fields")
        require(envelope["schema"] == "stoarama.capture-invoice-response.v1" and envelope["request_name"] == name and envelope["method"] == "GET" and envelope["path"] == path, "response order or request identity")
        require(type(envelope["status"]) is int and envelope["status"] == 200, "response status")
        require(type(envelope["request_id"]) is str and bool(re.fullmatch(r"[A-Za-z0-9-]{1,256}", envelope["request_id"])), "response request ID")
        require(type(envelope["date"]) is str and len(envelope["date"]) <= 128 and "\n" not in envelope["date"] and "\r" not in envelope["date"], "response date header")
        started, completed = utc(envelope["started_at"]), utc(envelope["completed_at"])
        require(started <= completed and (previous is None or previous <= started), "response chronology")
        previous = completed
        ref = envelope["body"]
        require(type(ref) is dict and set(ref) == {"file", "sha256", "size_bytes"} and ref["file"] == name + ".body.json", "response body reference")
        raw = read_regular(Path(root) / ref["file"], BODY_LIMIT)
        require(type(ref["size_bytes"]) is int and len(raw) == ref["size_bytes"] and digest(raw) == ref["sha256"], "response body identity")
        total_bytes += len(raw)
        require(total_bytes <= 7 * BODY_LIMIT, "aggregate response bound")
        body = strict_json(raw)
        require(canonical(body) == canonical(envelope["parsed_body"]), "response parsed body differs from raw bytes")
        if index in (0, 6):
            account(body)
        else:
            month, invoice, number, count, total = PAGES[index - 1]
            actual = page(body, invoice, number, count, total)
            expected = baseline["invoice-full-2026-%s.json" % month]["invoice_items"]
            require(actual == expected[(number - 1) * 200:], "fresh page differs from retained assembled slice")
        responses.append({"name": name, "envelope_sha256": digest(envelope_raw), "envelope_size_bytes": len(envelope_raw), "body_sha256": digest(raw), "body_size_bytes": len(raw)})
    return {"schema": "stoarama.capture-invoice-validation.v1", "responses": responses, "response_count": 7, "historical_page_rows": sum(p[3] for p in PAGES), "original_placement_proven": False, "owner_death_proven": False, "apply_authority": False}


def prepare(source_sha, deadline):
    """Launcher child, BEFORE sourcing auth. The later exec keeps shell PID."""
    require(bool(re.fullmatch(r"[0-9a-f]{64}", source_sha)), "script identity")
    deadline.check()
    require(REMOTE_OUT.parent.resolve(strict=True) == REMOTE_OUT.parent, "output parent is not canonical")
    os.mkdir(REMOTE_OUT, 0o700)  # Exclusive attempt reservation: never reuse.
    attempt = {"schema": "stoarama.capture-invoice-attempt.v1", "shell_pid": os.getppid(), "started_at": now(),
               "monotonic_start": deadline.start, "deadline": deadline.end,
               "source_sha256": source_sha, "baseline_pins_sha256": PINS_SHA}
    write_new(REMOTE_OUT / "attempt.json", canonical(attempt))
    try:
        load_baseline(REMOTE_BASE, REMOTE_PINS)
        deadline.check()
    except (Exception, Cancelled) as error:
        deadline.begin_cleanup()
        deadline.check_hard()
        write_new(REMOTE_OUT / "failure.json", canonical({"outcome": deadline.outcome if isinstance(error, Cancelled) else Outcome.BASELINE, "stage": "prepare", "exception_type": type(error).__name__, "requests": 0}))
        raise EvidenceError("baseline refused; attempt remains reserved") from None


def request_http(path, token):
    """One fixed-host HTTPS GET; no proxy, redirect, SDK or retry surface."""
    import http.client
    import ssl
    connection = http.client.HTTPSConnection("api.digitalocean.com", timeout=3, context=ssl.create_default_context())
    try:
        connection.connect()
        connection.sock.settimeout(6)
        connection.request("GET", path, headers={"Authorization": "Bearer " + token, "Accept": "application/json"})
        response = connection.getresponse()
        length = response.getheader("Content-Length", "")
        require(re.fullmatch(r"[0-9]{1,7}", length) is not None
                and response.getheader("Transfer-Encoding") is None, "HTTP length framing required")
        expected = int(length)
        require(0 < expected <= BODY_LIMIT and response.length == expected and not response.chunked, "HTTP body bound")
        # With a validated bounded length, read() uses HTTPResponse's exact
        # read and raises IncompleteRead on premature EOF, even after valid JSON.
        raw = response.read()
        require(len(raw) == expected and response.length == 0, "HTTP body incomplete")
        return response.status, response.getheader("x-request-id", ""), response.getheader("date", ""), raw
    finally:
        connection.close()


def collect(source_sha, fetch, channel, deadline):
    """Single remote process after launcher exec; never resumes an attempt."""
    attempt = strict_json(read_regular(REMOTE_OUT / "attempt.json", 4096))
    require(type(attempt) is dict and set(attempt) == {"schema", "shell_pid", "started_at", "monotonic_start", "deadline", "source_sha256", "baseline_pins_sha256"}
            and attempt["schema"] == "stoarama.capture-invoice-attempt.v1", "attempt fields")
    require(attempt["source_sha256"] == source_sha and attempt["baseline_pins_sha256"] == PINS_SHA and attempt["shell_pid"] == os.getpid(), "launcher reservation identity differs")
    require(attempt["monotonic_start"] == deadline.start and attempt["deadline"] == deadline.end, "attempt deadline changed")
    require(stat.S_IMODE(REMOTE_OUT.stat().st_mode) == 0o700 and REMOTE_OUT.stat().st_uid == os.getuid(), "output ownership or mode")
    deadline.check()
    write_new(REMOTE_OUT / "started.json", canonical({"pid": os.getpid(), "source_sha256": source_sha, "at": now()}))
    deadline.check()  # The timer was armed BEFORE the durable started write.
    outcome, stage, current, completed = Outcome.BASELINE, Stage.BASELINE, None, []
    watcher = ChannelWatch(channel)
    try:
        watcher.start()
        baseline = load_baseline(REMOTE_BASE, REMOTE_PINS)
        token = os.environ.pop("DIGITALOCEAN_TOKEN")
        require(bool(token) and token.strip() == token and "\r" not in token and "\n" not in token, "credential unavailable")
        os.environ.clear()  # No proxy/cert/netrc configuration inherited.
        for name, path in requests():
            deadline.check()
            current, stage, outcome = name, Stage.REQUEST, Outcome.REQUEST
            started = now()
            status, request_id, date, raw = fetch(path, token)
            require(token.encode() not in raw and token not in request_id and token not in date, "provider reflected credential; body not retained")
            stage, outcome = Stage.RESPONSE, Outcome.RESPONSE
            parsed = strict_json(raw)
            require(not contains_secret(parsed, token), "decoded provider response reflected credential")
            envelope = {"schema": "stoarama.capture-invoice-response.v1", "request_name": name,
                        "started_at": started, "completed_at": now(), "method": "GET", "path": path,
                        "status": status, "request_id": request_id, "date": date,
                        "body": {"file": name + ".body.json", "sha256": digest(raw), "size_bytes": len(raw)}, "parsed_body": parsed}
            encoded = canonical(envelope)
            require(token.encode() not in encoded, "serialized response reflected credential")
            write_new(REMOTE_OUT / (name + ".body.json"), raw)
            write_new(REMOTE_OUT / (name + ".json"), encoded)
            require(status == 200 and bool(re.fullmatch(r"[A-Za-z0-9-]{1,256}", request_id)), "response status or request ID")
            if name in ("account-before", "account-after"):
                account(parsed)
            else:
                index = len(completed) - 1
                month, invoice, number, count, total = PAGES[index]
                actual = page(parsed, invoice, number, count, total)
                require(actual == baseline["invoice-full-2026-%s.json" % month]["invoice_items"][(number - 1) * 200:], "response coverage differs")
            completed.append(name)
        stage, outcome = Stage.VALIDATION, Outcome.COVERAGE
        validation = validate_capture(REMOTE_OUT, baseline)
        deadline.check()
        manifest = {"schema": "stoarama.capture-invoice-collection.v1", "outcome": Outcome.COMPLETE,
                    "source_sha256": source_sha, "baseline_pins_sha256": PINS_SHA, "completed_at": now(),
                    "elapsed_seconds": time.monotonic() - attempt["monotonic_start"], "validation": validation}
        write_new(REMOTE_OUT / "manifest.json", canonical(manifest))
        pins = []
        for path in sorted(REMOTE_OUT.iterdir()):
            body = read_regular(path, 4 * BODY_LIMIT)
            pins.append(digest(body) + "  " + path.name + "\n")
        write_new(REMOTE_OUT / "SHA256SUMS", "".join(pins).encode())
        deadline.check()
        watcher.close(deadline)
        require(not watcher.disconnected.is_set(), "channel closure before completion")
        require(not select.select([channel], [], [], 0)[0], "channel changed after watcher exit")
        deadline.check()
        outcome = Outcome.COMPLETE
        return {"outcome": outcome, "requests": 7, "manifest_sha256": digest(canonical(manifest)), "apply_authority": False}
    except (Exception, Cancelled) as error:
        if isinstance(error, Cancelled):
            outcome = deadline.outcome
        elif watcher.disconnected.is_set():
            outcome = Outcome.INTERRUPTED
        deadline.begin_cleanup()
        watcher.close(deadline)  # Observe exit before a potentially slow fsync.
        deadline.check_hard()
        failure = {"outcome": outcome, "stage": stage, "request_name": current, "completed_requests": len(completed),
                   "exception_type": type(error).__name__, "channel_disconnected": watcher.disconnected.is_set(), "apply_authority": False}
        write_new(REMOTE_OUT / "failure.json", canonical(failure))
        raise EvidenceError("collection refused; bounded failure preserved; no retry") from None
    finally:
        watcher.close(deadline)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    validate = sub.add_parser("validate")
    validate.add_argument("--baseline-root", required=True)
    validate.add_argument("--pins", required=True)
    validate.add_argument("--capture-root", required=True)
    for name in ("prepare", "collect"):
        command = sub.add_parser(name)
        command.add_argument("--source-sha", required=True)
        command.add_argument("--monotonic-start", required=True, type=float)
        command.add_argument("--deadline", required=True, type=float)
    args = parser.parse_args()
    if args.command in ("prepare", "collect"):
        import sys
        with Deadline(args.monotonic_start, args.deadline) as deadline:
            if args.command == "prepare":
                prepare(args.source_sha, deadline)
            else:
                result = collect(args.source_sha, request_http, sys.stdin.buffer, deadline)
                emit_result(result)
                deadline.check()
    else:
        baseline = load_baseline(Path(args.baseline_root), Path(args.pins))
        emit_result(validate_capture(Path(args.capture_root), baseline))


if __name__ == "__main__":
    try:
        main()
    except (Exception, Cancelled):
        # No late stderr write/flush after a hard deadline. Reserved/partial
        # evidence plus nonzero terminal status is HOLD, never success.
        raise SystemExit(1) from None
