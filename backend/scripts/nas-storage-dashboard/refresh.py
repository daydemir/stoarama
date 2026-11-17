#!/usr/bin/env python3
"""Read-only NAS evidence collector. No SSH, media reads, or media cleanup."""
import argparse
import csv
import datetime as dt
import fcntl
import gzip
import hashlib
import hmac
import io
import json
import os
import re
import uuid
from pathlib import Path
import shutil
import sqlite3
import stat
import subprocess
import sys
import tempfile
import time
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from collections import defaultdict
from zoneinfo import ZoneInfo

APP = "nas-storage-dashboard-v1"
ROOT = Path(__file__).resolve().parent
CLI = Path("/Users/jelly/.stoarama/nas-readonly-audit-20261019/stoaramactl-064f040b")
UTC = dt.timezone.utc


def now():
    return dt.datetime.now(UTC).isoformat()


def private_directory(path):
    path.mkdir(mode=0o700, parents=True, exist_ok=True)
    info = path.lstat()
    if not stat.S_ISDIR(info.st_mode) or info.st_uid != os.getuid():
        raise RuntimeError("unsafe dashboard directory")
    marker = path / ".nas-dashboard-owner"
    if not marker.exists() and any(path.iterdir()):
        raise RuntimeError("unowned nonempty dashboard directory preserved")
    try:
        fd = os.open(str(marker), os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    except FileExistsError:
        if marker.is_symlink() or marker.read_text() != APP:
            raise RuntimeError("unknown dashboard directory preserved")
    else:
        with os.fdopen(fd, "w") as stream:
            stream.write(APP)


def write_json(path, payload):
    if path.exists() or path.is_symlink():
        info = path.lstat()
        if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1:
            raise RuntimeError("dashboard output link preserved")
        if json.loads(path.read_text()).get("app") != APP:
            raise RuntimeError("unknown dashboard output preserved")
    payload = dict(payload, app=APP)
    with tempfile.NamedTemporaryFile("w", dir=path.parent, delete=False, encoding="utf-8") as stream:
        json.dump(payload, stream, ensure_ascii=True, separators=(",", ":"))
        stream.flush()
        os.fsync(stream.fileno())
        pending = stream.name
    os.replace(pending, path)


def pg_env():
    # Scoped credential consumer: no URL/password in arguments, logs or output.
    u = urllib.parse.urlsplit(os.environ["DATABASE_URL"])
    e = os.environ.copy()
    e.update(PGHOST=u.hostname, PGPORT=str(u.port or 5432),
             PGUSER=urllib.parse.unquote(u.username or ""), PGPASSWORD=urllib.parse.unquote(u.password or ""),
             PGDATABASE=u.path.lstrip("/"), PGCONNECT_TIMEOUT="10",
             PGOPTIONS="-c default_transaction_read_only=on -c statement_timeout=180000 -c timezone=UTC")
    for k, v in urllib.parse.parse_qsl(u.query):
        if k in ("sslmode", "connect_timeout"):
            e["PG" + k.upper()] = v
    return e


def query(file, cutoff=None, output=None):
    args = ["/opt/homebrew/opt/libpq/bin/psql", "-XqAt", "--set", "ON_ERROR_STOP=1"]
    if cutoff:
        args += ["--set", "cutoff=" + cutoff]
    args += ["--file", str(ROOT / file)]
    result = subprocess.run(args, env=pg_env(), stdout=output or subprocess.PIPE,
                            stderr=subprocess.DEVNULL, timeout=210)
    if result.returncode:
        raise RuntimeError("read-only database query failed")
    if output is None:
        return json.loads(result.stdout)


def cache(state):
    target = state / "metadata-v2.sqlite3"
    if target.exists() or target.is_symlink():
        info = target.lstat()
        if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1 or info.st_uid != os.getuid():
            raise RuntimeError("unsafe metadata cache preserved")
        with sqlite3.connect(target.as_uri() + "?mode=ro", uri=True) as probe:
            if probe.execute("PRAGMA application_id").fetchone()[0] != 0x4E415344:
                raise RuntimeError("unknown metadata database preserved")
    else:
        fd = os.open(str(target), os.O_CREAT | os.O_EXCL | os.O_WRONLY | os.O_NOFOLLOW, 0o600)
        os.close(fd)
    db = sqlite3.connect(str(target))
    db.execute("PRAGMA application_id=1312904004")
    db.create_function("r2_prefix", 1, r2_prefix)
    db.executescript("""
      CREATE TABLE IF NOT EXISTS meta(key TEXT PRIMARY KEY,value TEXT);
      CREATE TABLE IF NOT EXISTS facts(path TEXT PRIMARY KEY,recording_id INTEGER,kind TEXT,
        bytes INTEGER,ranges TEXT,r2_key TEXT,present INTEGER,observed_at TEXT,source_id TEXT UNIQUE);
      CREATE TABLE IF NOT EXISTS observed(path TEXT PRIMARY KEY,kind TEXT,bytes INTEGER,
        folder TEXT,error TEXT,device TEXT,inode TEXT,observed_at TEXT);
      CREATE TABLE IF NOT EXISTS r2(key TEXT PRIMARY KEY,bytes INTEGER,checked_at TEXT);
      CREATE TABLE IF NOT EXISTS prefixes(prefix TEXT PRIMARY KEY,cursor TEXT,complete_at TEXT,started_at TEXT);
    """)
    if "started_at" not in {r[1] for r in db.execute("PRAGMA table_info(prefixes)")}:
        db.execute("ALTER TABLE prefixes ADD COLUMN started_at TEXT")
    if "known_at" not in {r[1] for r in db.execute("PRAGMA table_info(facts)")}:
        db.execute("ALTER TABLE facts ADD COLUMN known_at TEXT")
        set_meta(db, "legacy_facts_known_at", now())
        db.commit()
    return db


def get_meta(db, key, default=None):
    row = db.execute("SELECT value FROM meta WHERE key=?", (key,)).fetchone()
    return row[0] if row else default


def set_meta(db, key, value):
    db.execute("INSERT OR REPLACE INTO meta VALUES (?,?)", (key, str(value)))


def refresh_facts(db, state, stamp):
    cutoff = get_meta(db, "facts_cutoff", "1970-01-01T00:00:00Z")
    # COPY streams into task-owned scratch; no large captured stdout or DB writes.
    with tempfile.TemporaryFile("w+b", dir=state) as stream:
        query("facts.sql", cutoff=cutoff, output=stream)
        stream.seek(0)
        rows = csv.DictReader(io.TextIOWrapper(stream, encoding="utf-8"))
        batch = []
        for row in rows:
            batch.append((row["path"], int(row["recording_id"]), row["kind"], int(row["bytes"]),
                          row["ranges"], row["r2_key"] or "", row["reported_present"] == "t", row["observed_at"], row["source_id"]))
            if len(batch) >= 5000:
                save_facts(db, batch)
                batch.clear()
        save_facts(db, batch)
    # Overlap refreshes by one minute; immutable ACKs and changed inventory rows
    # are upserted. Only advance after the whole bounded COPY succeeds.
    set_meta(db, "facts_cutoff", (dt.datetime.fromisoformat(stamp) - dt.timedelta(minutes=1)).isoformat())
    db.commit()


def save_facts(db, rows):
    rows = [tuple(row[:9]) + (now(),) for row in rows]
    # Inventory is keyed upstream by clip ID; a rename must retire its old path.
    db.executemany("DELETE FROM facts WHERE source_id=? AND path<>? AND observed_at<=?",
                   [(row[8], row[0], row[7]) for row in rows])
    db.executemany("""INSERT INTO facts VALUES (?,?,?,?,?,?,?,?,?,?) ON CONFLICT(path) DO UPDATE SET
        recording_id=excluded.recording_id,kind=excluded.kind,bytes=excluded.bytes,ranges=excluded.ranges,
        r2_key=excluded.r2_key,present=excluded.present,observed_at=excluded.observed_at,source_id=excluded.source_id,
        known_at=CASE WHEN facts.r2_key IS excluded.r2_key THEN coalesce(facts.known_at,excluded.known_at) ELSE excluded.known_at END
        WHERE excluded.observed_at >= facts.observed_at
        ON CONFLICT(source_id) DO NOTHING""", rows)


def import_report(db, state, audit):
    if not audit or audit["state"] != "complete" or get_meta(db, "report_id") == audit["id"]:
        return
    expected = int(audit["artifact_size_bytes"])
    # Keep at least 5 GiB, plus room for the metadata index. Defer visibly if low.
    if shutil.disk_usage(state).free < 5 * 1024**3 + expected + 1024**3:
        raise RuntimeError("report import deferred for local headroom")
    if not re.fullmatch(r"[a-f0-9-]{36}", audit["id"]):
        raise RuntimeError("invalid audit identifier")
    # A failed exclusive download stays intact. Retry to a new owned filename.
    target = state / ("report-" + audit["id"] + "-" + uuid.uuid4().hex + ".jsonl.gz")
    if not target.exists():
        result = subprocess.run([str(CLI), "nas-space-audit", "report", "--connection-id", "13",
                                 "--download", str(target), "--json"], stdout=subprocess.DEVNULL,
                                stderr=subprocess.DEVNULL, timeout=660)
        if result.returncode:
            raise RuntimeError("verified audit report download failed; partial output preserved")
    digest = hashlib.sha256()
    with target.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    if target.stat().st_size != expected or digest.hexdigest() != audit["artifact_sha256"]:
        raise RuntimeError("audit report checksum differs; file preserved")
    db.execute("DELETE FROM observed")  # Generated metadata index only.
    with gzip.open(target, "rt", encoding="utf-8") as stream:
        first = json.loads(next(stream))
        if first.get("audit_id") != audit["id"]:
            raise RuntimeError("audit report identity differs")
        batch = []
        for line in stream:
            row = json.loads(line)
            # Non-media metadata remains in the verified artifact and summary;
            # the local day-map index only needs media candidates.
            if row["category"] not in ("unstitched_media", "stitched_media"):
                continue
            if "path_bytes_b64" in row:
                # Do not confuse escaped display names with usable path identities.
                row["error"] = "non-UTF8 path; mapping unavailable"
                row["path"] = "unmapped-byte-path:" + row["path_bytes_b64"]
            batch.append((row["path"], row["category"], row["size"], row["folder"], row["error"],
                          str(row["device"]), str(row["inode"]), row["observed_at"]))
            if len(batch) >= 5000:
                db.executemany("INSERT OR REPLACE INTO observed VALUES (?,?,?,?,?,?,?,?)", batch)
                batch.clear()
        db.executemany("INSERT OR REPLACE INTO observed VALUES (?,?,?,?,?,?,?,?)", batch)
    set_meta(db, "report_id", audit["id"])
    set_meta(db, "report_completed_at", audit["completed_at"])
    db.commit()


def r2_list(prefix, cursor):
    # Signed metadata LIST only. No media payload requests or public capabilities.
    account = os.environ["R2_ACCOUNT_ID"]
    host = account + ".r2.cloudflarestorage.com"
    bucket = os.environ["R2_BUCKET"]
    key = os.environ["R2_ACCESS_KEY_ID"]
    secret = os.environ["R2_SECRET_ACCESS_KEY"]
    stamp = dt.datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    day = stamp[:8]
    params = {"list-type": "2", "prefix": prefix, "max-keys": "1000"}
    if cursor:
        params["continuation-token"] = cursor
    q = "&".join(urllib.parse.quote(k, safe="") + "=" + urllib.parse.quote(v, safe="")
                 for k, v in sorted(params.items()))
    uri = "/" + urllib.parse.quote(bucket, safe="")
    empty = hashlib.sha256(b"").hexdigest()
    canonical = "GET\n" + uri + "\n" + q + "\nhost:" + host + "\nx-amz-content-sha256:" + empty + "\nx-amz-date:" + stamp + "\n\nhost;x-amz-content-sha256;x-amz-date\n" + empty
    scope = day + "/auto/s3/aws4_request"
    signing = ("AWS4" + secret).encode()
    for piece in (day, "auto", "s3", "aws4_request"):
        signing = hmac.new(signing, piece.encode(), hashlib.sha256).digest()
    message = "AWS4-HMAC-SHA256\n" + stamp + "\n" + scope + "\n" + hashlib.sha256(canonical.encode()).hexdigest()
    signature = hmac.new(signing, message.encode(), hashlib.sha256).hexdigest()
    headers = {"x-amz-date": stamp, "x-amz-content-sha256": empty,
               "Authorization": "AWS4-HMAC-SHA256 Credential=" + key + "/" + scope +
               ", SignedHeaders=host;x-amz-content-sha256;x-amz-date, Signature=" + signature}
    req = urllib.request.Request("https://" + host + uri + "?" + q, headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=30) as response:
            data = response.read(8 * 1024 * 1024 + 1)
        if len(data) > 8 * 1024 * 1024:
            raise RuntimeError("oversized R2 listing")
        root = ET.fromstring(data)
    except Exception:
        raise RuntimeError("R2 metadata listing failed") from None
    ns = {"s": "http://s3.amazonaws.com/doc/2006-03-01/"}
    objects = [(x.findtext("s:Key", namespaces=ns), int(x.findtext("s:Size", namespaces=ns)))
               for x in root.findall("s:Contents", ns)]
    return objects, root.findtext("s:NextContinuationToken", namespaces=ns)


def r2_prefix(key):
    return "/".join(key.split("/")[:2]) + "/" if key else ""


class LocalHeadroom(RuntimeError):
    pass


def refresh_r2(db, budget=100, state=None):
    expected_keys = {key for (key,) in db.execute("SELECT DISTINCT r2_key FROM facts WHERE r2_key<>''")}
    prefixes = {r2_prefix(key) for key in expected_keys}
    # Cache expected NAS keys only; unrelated R2 objects need no local row.
    # This prunes only this app's disposable metadata index, never R2 objects.
    irrelevant = [(key,) for (key,) in db.execute("SELECT key FROM r2") if key not in expected_keys]
    db.executemany("DELETE FROM r2 WHERE key=?", irrelevant)
    db.executemany("INSERT OR IGNORE INTO prefixes(prefix) VALUES (?)", [(x,) for x in prefixes])
    stale = (dt.datetime.now(UTC) - dt.timedelta(hours=24)).isoformat()
    for (prefix,) in db.execute("SELECT prefix FROM prefixes WHERE complete_at < ?", (stale,)).fetchall():
        # Rebuild only our local metadata index; no R2 or NAS mutation.
        db.execute("DELETE FROM r2 WHERE substr(key,1,?)=?", (len(prefix), prefix))
        db.execute("UPDATE prefixes SET cursor=NULL,complete_at=NULL,started_at=NULL WHERE prefix=?", (prefix,))
    db.commit()
    deadline = time.monotonic() + budget
    for prefix, cursor in db.execute("SELECT prefix,cursor FROM prefixes WHERE complete_at IS NULL ORDER BY prefix").fetchall():
        while time.monotonic() < deadline:
            reserve = (5 * 1024**3 + 128 * 1024**2) if get_meta(db, "report_id") else (6 * 1024**3 + 256 * 1024**2)
            if state is not None and shutil.disk_usage(state).free < reserve:
                raise LocalHeadroom("R2 metadata paused to reserve NAS report import space")
            if not cursor:
                db.execute("UPDATE prefixes SET started_at=? WHERE prefix=?", (now(), prefix))
                db.commit()
            objects, next_cursor = r2_list(prefix, cursor)
            stamp = now()
            db.executemany("INSERT OR REPLACE INTO r2 VALUES (?,?,?)", [(k, n, stamp) for k, n in objects if k in expected_keys])
            db.execute("UPDATE prefixes SET cursor=?,complete_at=? WHERE prefix=?",
                       (next_cursor, None if next_cursor else stamp, prefix))
            db.commit()
            if not next_cursor:
                break
            cursor = next_cursor
        if time.monotonic() >= deadline:
            break


def merge_ranges(ranges):
    result = []
    for start, end in sorted(ranges):
        if end <= start:
            continue
        if result and start <= result[-1][1]:
            result[-1][1] = max(result[-1][1], end)
        else:
            result.append([start, end])
    return result


def split_days(start, end, timezone):
    zone = ZoneInfo(timezone)
    while start < end:
        date = dt.datetime.fromtimestamp(start, zone).date()
        midnight = dt.datetime.combine(date, dt.time(), zone).timestamp()
        next_midnight = dt.datetime.combine(date + dt.timedelta(days=1), dt.time(), zone).timestamp()
        stop = min(end, next_midnight)
        yield str(date), start - midnight, stop - midnight, next_midnight - midnight
        start = stop


def parse_time(value):
    # PostgreSQL may serialize UTC offsets as +00; Python 3.9 needs +00:00.
    value = value.replace(" ", "T").replace("Z", "+00:00")
    if re.search(r"[+-]\d{2}$", value):
        value += ":00"
    # Python 3.9 accepts only 3 or 6 fractional digits. PostgreSQL trims
    # trailing zeroes, so valid timestamps can have any fractional width.
    value = re.sub(r"\.(\d+)(?=[+-]\d{2}:\d{2}$|$)",
                   lambda match: "." + match.group(1)[:6].ljust(6, "0"), value)
    return dt.datetime.fromisoformat(value)


def retention(streams):
    result = defaultdict(set)
    for count in (14, 20):
        for row in json.loads((ROOT / ("retention%d.json" % count)).read_text()):
            meta = streams.get(int(row["recording_id"]))
            if meta:
                date = parse_time(row["start"]).astimezone(ZoneInfo(meta["timezone"])).date()
                result[(meta["stream_id"], str(date))].add(count)
    return result


def assemble(db, snapshot):
    recordings = {x["recording_id"]: x for x in snapshot["streams"]}
    by_stream = {}
    for meta in recordings.values():
        sid = meta["stream_id"] or ("recording-" + str(meta["recording_id"]))
        if not re.fullmatch(r"[A-Za-z0-9_-]+", sid):
            raise RuntimeError("invalid stream identifier")
        meta["stream_id"] = sid
        stream = by_stream.setdefault(sid, dict(id=sid, name=meta["name"], timezone=meta["timezone"],
                                              recordings=[], days={}, folders=set()))
        stream["recordings"].append(meta["recording_id"])
        if meta["folder"]:
            stream["folders"].add(meta["folder"])
    protected = retention(recordings)
    fresh = bool(get_meta(db, "report_id"))
    if fresh:
        rows = db.execute("""SELECT o.path,f.recording_id,f.kind,o.bytes,f.ranges,f.r2_key,o.observed_at,r.bytes,
                  CASE WHEN o.path LIKE '#recycle/%' THEN 'recycle' ELSE 'active' END,p.complete_at,p.started_at,r.checked_at,coalesce(f.known_at,(SELECT value FROM meta WHERE key='legacy_facts_known_at'))
                FROM observed o JOIN facts f ON f.path=CASE WHEN o.path LIKE '#recycle/%' THEN substr(o.path,10) ELSE o.path END
                LEFT JOIN r2 r ON r.key=f.r2_key LEFT JOIN prefixes p ON p.prefix=r2_prefix(f.r2_key)
                WHERE o.kind IN ('unstitched_media','stitched_media') AND o.error=''
                  AND o.bytes=f.bytes""")
    else:
        rows = db.execute("""SELECT f.path,f.recording_id,f.kind,f.bytes,f.ranges,f.r2_key,f.observed_at,r.bytes,'active',p.complete_at,p.started_at,r.checked_at,coalesce(f.known_at,(SELECT value FROM meta WHERE key='legacy_facts_known_at'))
                              FROM facts f LEFT JOIN r2 r ON r.key=f.r2_key LEFT JOIN prefixes p ON p.prefix=r2_prefix(f.r2_key) WHERE f.present=1""")
    totals = defaultdict(int)
    for path, rid, kind, size, encoded, key, observed_at, r2_size, area, r2_complete, r2_started, r2_checked, known_at in rows:
        meta = recordings.get(rid)
        if not meta:
            continue
        stream = by_stream[meta["stream_id"]]
        ranges = merge_ranges(json.loads(encoded))
        if not ranges:
            totals["undated_files"] += 1
            totals["undated_bytes"] += size
            continue
        totals["mapped_files"] += 1
        totals["mapped_bytes"] += size
        first_date = next(split_days(ranges[0][0], ranges[0][1], meta["timezone"]))[0]
        for start, end in ranges:
            for date, a, b, length in split_days(float(start), float(end), meta["timezone"]):
                day = stream["days"].setdefault((date, area), dict(date=date, area=area, day_seconds=length,
                    keep=sorted(protected[(meta["stream_id"], date)]), clips=[], stitched=[], clips_files=0,
                    stitched_files=0, clips_bytes=0, stitched_bytes=0, r2_size_matched_files=0,
                    r2_size_mismatch_files=0, r2_unchecked_files=0, r2_not_found_files=0, r2_checked_at=None, latest_evidence_at=None, earliest_evidence_at=None))
                day[kind].append([a, b])
        day = stream["days"][(first_date, area)]
        day[kind + "_files"] += 1
        day[kind + "_bytes"] += size
        if r2_size is None:
            absent = (key and r2_complete and r2_started and known_at
                      and parse_time(r2_started) >= max(parse_time(observed_at), parse_time(known_at)))
            day["r2_not_found_files" if absent else "r2_unchecked_files"] += 1
        elif r2_size == size:
            day["r2_size_matched_files"] += 1
        else:
            day["r2_size_mismatch_files"] += 1
        checked = r2_checked or r2_complete
        if checked:
            day["r2_checked_at"] = min(day["r2_checked_at"] or checked, checked)
        day["latest_evidence_at"] = max(day["latest_evidence_at"] or observed_at, observed_at)
        day["earliest_evidence_at"] = min(day["earliest_evidence_at"] or observed_at, observed_at)
    for stream in by_stream.values():
        days = []
        for day in stream["days"].values():
            for kind in ("clips", "stitched"):
                day[kind] = merge_ranges(day[kind])
                day[kind + "_seconds"] = round(sum(b - a for a, b in day[kind]), 3)
            both = merge_ranges(day["clips"] + day["stitched"])
            day["combined_seconds"] = round(sum(b - a for a, b in both), 3)
            day["overlap_seconds"] = round(day["clips_seconds"] + day["stitched_seconds"] - day["combined_seconds"], 3)
            days.append(day)
        stream["days"] = sorted(days, key=lambda d: (d["date"], d["area"]))
        stream["folders"] = sorted(stream["folders"])
        active = [d for d in days if d["area"] == "active"]
        stream.update(days_count=len(active), clips_bytes=sum(d["clips_bytes"] for d in active),
                      stitched_bytes=sum(d["stitched_bytes"] for d in active),
                      clips_seconds=sum(d["clips_seconds"] for d in active),
                      stitched_seconds=sum(d["stitched_seconds"] for d in active))
    if fresh:
        totals["unmapped_observed_files"] = db.execute("""SELECT count(*) FROM observed o LEFT JOIN facts f
          ON f.path=CASE WHEN o.path LIKE '#recycle/%' THEN substr(o.path,10) ELSE o.path END
          WHERE o.kind IN ('unstitched_media','stitched_media') AND (f.path IS NULL OR o.bytes<>f.bytes OR o.error<>'')""").fetchone()[0]
    return by_stream, dict(totals), fresh


def publish_scan_progress(snapshot, directory):
    """Keep scan/heartbeat evidence current even if a day-map stage fails."""
    target = directory / "data.json"
    if not target.exists():
        return  # A first successful day-map publication supplies the full schema.
    if target.is_symlink():
        raise RuntimeError("dashboard output link preserved")
    previous = json.loads(target.read_text())
    if previous.get("app") != APP:
        raise RuntimeError("unknown dashboard output preserved")
    previous.update(audit=snapshot["audit"], connection=snapshot["connection"], scan_status_refreshed_at=now())
    # generated_at continues to date the unchanged coverage and R2 map.
    write_json(target, previous)


def publish(db, snapshot, directory):
    streams, totals, fresh = assemble(db, snapshot)
    stamp = now()
    r2 = db.execute("SELECT count(*),min(checked_at),max(checked_at) FROM r2").fetchone()
    prefixes = db.execute("SELECT count(*),sum(complete_at IS NOT NULL) FROM prefixes").fetchone()
    write_json(directory / "data.json", dict(generated_at=stamp, scan_status_refreshed_at=stamp, refresh_every_minutes=30,
        basis="fresh_stat_report" if fresh else "last_reported_inventory_and_acknowledgements",
        imported_report_id=get_meta(db, "report_id"), imported_report_completed_at=get_meta(db, "report_completed_at"), totals=totals, connection=snapshot["connection"],
        audit=snapshot["audit"], streams=sorted(streams.values(), key=lambda x: x["name"].casefold()),
        r2=dict(objects_indexed=r2[0], earliest_check=r2[1], latest_check=r2[2],
                prefixes=prefixes[0], completed_prefixes=prefixes[1] or 0),
        retention_snapshot="2026-10-19", cleanup_enabled=False))


def retire_obsolete_cache(state, expected_sha256):
    """Owner cleanup for the exact generated v1 preview index, never source data."""
    if not re.fullmatch(r"[a-f0-9]{64}", expected_sha256):
        raise RuntimeError("invalid obsolete-cache digest")
    target = state / "metadata.sqlite3"
    fd = os.open(str(target), os.O_RDONLY | os.O_NOFOLLOW)
    try:
        before = os.fstat(fd)
        if not stat.S_ISREG(before.st_mode) or before.st_nlink != 1 or before.st_uid != os.getuid():
            raise RuntimeError("unsafe obsolete cache preserved")
        digest = hashlib.sha256()
        while True:
            block = os.read(fd, 1024 * 1024)
            if not block:
                break
            digest.update(block)
        current = target.lstat()
        identity = lambda x: (x.st_dev, x.st_ino, x.st_size, x.st_mtime_ns, x.st_ctime_ns, x.st_nlink)
        if digest.hexdigest() != expected_sha256 or identity(before) != identity(current) or identity(before) != identity(os.fstat(fd)):
            raise RuntimeError("changed obsolete cache preserved")
        # The private directory and collector lock serialize this owner operation.
        # Refuse any SQLite companions: an active/recoverable v1 index is retained.
        if any(Path(str(target) + suffix).exists() or Path(str(target) + suffix).is_symlink()
               for suffix in ("-journal", "-wal", "-shm")):
            raise RuntimeError("obsolete cache with companions preserved")
        target.unlink()
    finally:
        os.close(fd)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--state", type=Path, required=True)
    parser.add_argument("--publish", type=Path, required=True)
    parser.add_argument("--r2-budget", type=int, default=100)
    parser.add_argument("--retire-obsolete-cache-sha256")
    args = parser.parse_args()
    private_directory(args.state)
    private_directory(args.publish)
    lock = (args.state / "refresh.lock").open("a")
    try:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        if args.retire_obsolete_cache_sha256:
            print("NAS dashboard owner cleanup busy; obsolete cache preserved", file=sys.stderr)
            return 2
        return 0
    if args.retire_obsolete_cache_sha256:
        try:
            retire_obsolete_cache(args.state, args.retire_obsolete_cache_sha256)
            return 0
        finally:
            lock.close()
    start = now()
    write_json(args.publish / "refresh-status.json", dict(state="refreshing", started_at=start))
    db = None
    stage = "scan_status"
    try:
        snapshot = query("snapshot.sql")
        publish_scan_progress(snapshot, args.publish)
        stage = "cache"
        db = cache(args.state)
        stage = "inventory"
        refresh_facts(db, args.state, start)
        stage = "report_import"
        import_report(db, args.state, snapshot["audit"])
        # Publish useful progress before the bounded R2 metadata work.
        stage = "day_map"
        publish(db, snapshot, args.publish)
        try:
            refresh_r2(db, args.r2_budget, args.state)
            r2_error = None
        except Exception as exc:
            r2_error = type(exc).__name__
        publish(db, snapshot, args.publish)
        write_json(args.publish / "refresh-status.json", dict(state="ok", completed_at=now(),
                   next_refresh_at=(dt.datetime.now(UTC) + dt.timedelta(minutes=30)).isoformat(), r2_error=r2_error))
        # Empty stdout means a silent successful script-only scheduler run.
        return 0
    except Exception as exc:
        if db is not None:
            db.rollback()
        write_json(args.publish / "refresh-status.json", dict(state="failed", failed_at=now(),
                   error=type(exc).__name__, stage=stage, last_good_data_preserved=True))
        print("NAS dashboard refresh failed (" + type(exc).__name__ + "); last good snapshot retained", file=sys.stderr)
        return 1
    finally:
        if db is not None:
            db.close()
        lock.close()


if __name__ == "__main__":
    sys.exit(main())
