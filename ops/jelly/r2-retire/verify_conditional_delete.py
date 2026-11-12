#!/usr/bin/env python3
"""Operator-only probe. Deletes only its newly created 64-byte random object."""
import argparse
import datetime
import hashlib
import json
import os
from pathlib import Path
import subprocess
import tempfile
import uuid


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", required=True)
    args = parser.parse_args()
    target = Path(args.out)
    if target.exists():
        raise SystemExit("proof path already exists")
    env = dict(os.environ, AWS_ACCESS_KEY_ID=os.environ["R2_ACCESS_KEY_ID"],
               AWS_SECRET_ACCESS_KEY=os.environ["R2_SECRET_ACCESS_KEY"], AWS_DEFAULT_REGION="auto",
               AWS_MAX_ATTEMPTS="1", AWS_RETRY_MODE="standard")
    bucket = os.environ["R2_BUCKET"]
    endpoint = os.environ.get("R2_ENDPOINT") or "https://%s.r2.cloudflarestorage.com" % os.environ["R2_ACCOUNT_ID"]
    aws = str(Path.home() / ".local/bin/aws")
    key = "r2-retire-probes/%s.bin" % uuid.uuid4().hex

    def call(operation, *rest):
        return subprocess.run([aws, "--endpoint-url", endpoint, "s3api", operation,
                               "--bucket", bucket, "--key", key, *rest],
                              env=env, capture_output=True, text=True)

    with tempfile.TemporaryDirectory(prefix="r2-delete-probe-") as directory:
        body = os.urandom(64)
        source = Path(directory) / "probe.bin"
        source.write_bytes(body)
        put = call("put-object", "--body", str(source), "--if-none-match", "*")
        if put.returncode:
            raise SystemExit("create-only probe PUT failed; no proof written")
        etag = json.loads(put.stdout)["ETag"]
        print("Probe key (only this new object is touched): " + key)
        if etag.strip('"') == "00000000000000000000000000000000":
            raise SystemExit("probe ETag equals test mismatch; no proof written")
        wrong = call("delete-object", "--if-match", '"00000000000000000000000000000000"')
        proof = {"bucket": bucket, "endpoint_sha256": hashlib.sha256(endpoint.rstrip("/").encode()).hexdigest(),
                 "created_at": datetime.datetime.now(datetime.timezone.utc).isoformat(), "probe_key": key}
        if wrong.returncode == 0:
            head = call("head-object")
            if head.returncode == 0 or not any(code in head.stderr for code in ("(404)", "(NotFound)", "(NoSuchKey)")):
                raise SystemExit("wrong-ETag DELETE absence unconfirmed; no proof written; inspect " + key)
            proof.update(conditional_delete_unsupported=True, wrong_etag_delete_succeeded=True,
                         wrong_etag_delete_confirmed_absent=True,
                         wrong_etag_rejected=False, object_preserved=False, matching_delete_confirmed=False)
        else:
            if "(PreconditionFailed)" not in wrong.stderr:
                raise SystemExit("conditional DELETE result ambiguous; no proof written; inspect " + key)
            restored = Path(directory) / "readback.bin"
            get = call("get-object", str(restored))
            if get.returncode or restored.read_bytes() != body:
                raise SystemExit("probe bytes were not preserved after rejected delete; no proof written")
            matched = call("delete-object", "--if-match", etag)
            head = call("head-object")
            if matched.returncode or head.returncode == 0 or not any(code in head.stderr for code in ("(404)", "(NotFound)", "(NoSuchKey)")):
                raise SystemExit("matching delete was not confirmed; no proof written; inspect " + key)
            proof.update(wrong_etag_rejected=True, object_preserved=True, matching_delete_confirmed=True)
    raw = (json.dumps(proof, sort_keys=True, separators=(",", ":")) + "\n").encode()
    with target.open("xb") as output:
        os.chmod(target, 0o600)
        output.write(raw)
        output.flush()
        os.fsync(output.fileno())
    print("proof_sha256=" + hashlib.sha256(raw).hexdigest())


if __name__ == "__main__":
    main()
