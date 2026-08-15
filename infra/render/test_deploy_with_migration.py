import json
import os
import pathlib
import subprocess
import tempfile
import textwrap
import unittest


SCRIPT = pathlib.Path(__file__).with_name("deploy-with-migration.sh")


FAKE_CURL = r'''#!/usr/bin/env python3
import datetime,json,os,pathlib,sys,urllib.parse

args=sys.argv[1:]
method="GET"
output=None
body=""
url=args[-1]
for i,arg in enumerate(args):
    if arg=="-X": method=args[i+1]
    elif arg=="-o": output=args[i+1]
    elif arg=="--data": body=args[i+1]
state_path=pathlib.Path(os.environ["FAKE_RENDER_STATE"])
state=json.loads(state_path.read_text())
path=urllib.parse.urlsplit(url).path
query=urllib.parse.urlsplit(url).query
state["calls"].append({"method":method,"path":path,"query":query,"body":body})

service=path.split("/")[3] if path.startswith("/v1/services/") else ""
if method=="GET" and path=="/v1/services/"+service:
    response={"service":{"id":service,"name":state["names"][service]}}
elif path.endswith("/env-vars"):
    keys=state["envs"][service]
    response=[{"envVar":{"key":key,"value":"redacted"}} for key in keys]
elif method=="POST" and path.endswith("/deploys"):
    commit=json.loads(body)["commitId"]
    response={"id":"dep-"+service,"commit":{"id":commit}}
elif "/deploys/dep-" in path:
    response={"deploy":{"id":path.rsplit("/",1)[-1],"status":"live","commit":{"id":state["commit"]}}}
elif method=="POST" and path.endswith("/runs"):
    response={"id":"run-migration"}
elif path.endswith("/jobs"):
    response=[{"job":{"id":"job-migration","status":"succeeded","createdAt":datetime.datetime.now(datetime.timezone.utc).isoformat()}}]
else:
    raise SystemExit("unexpected fake Render request: %s %s" % (method,url))
state_path.write_text(json.dumps(state))
pathlib.Path(output).write_text(json.dumps(response))
'''


class DeployWithMigrationTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix="render-migration-test-")
        self.root = pathlib.Path(self.tmp.name)
        self.remote = self.root / "origin.git"
        self.repo = self.root / "repo"
        subprocess.run(["git", "init", "--bare", str(self.remote)], check=True, capture_output=True)
        subprocess.run(["git", "clone", str(self.remote), str(self.repo)], check=True, capture_output=True)
        subprocess.run(["git", "config", "user.email", "test@example.invalid"], cwd=self.repo, check=True)
        subprocess.run(["git", "config", "user.name", "Render Test"], cwd=self.repo, check=True)
        (self.repo / "README").write_text("exact commit\n")
        subprocess.run(["git", "add", "README"], cwd=self.repo, check=True)
        subprocess.run(["git", "commit", "-m", "exact"], cwd=self.repo, check=True, capture_output=True)
        subprocess.run(["git", "branch", "-M", "main"], cwd=self.repo, check=True)
        subprocess.run(["git", "push", "origin", "main"], cwd=self.repo, check=True, capture_output=True)
        self.commit = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=self.repo, text=True).strip()
        self.bin = self.root / "bin"
        self.bin.mkdir()
        curl = self.bin / "curl"
        curl.write_text(FAKE_CURL)
        curl.chmod(0o755)
        self.state = self.root / "state.json"

    def tearDown(self):
        self.tmp.cleanup()

    def run_deploy(self, *, invalid_api=False, omit_runtime=False):
        common = ["DATABASE_URL", "STOARAMA_DATABASE_RUNTIME_ROLE", "STOARAMA_DATABASE_ROLE_KIND", "STOARAMA_ADMISSION_AUTHORITY_ROLE"]
        envs = {
            "crn-migrate": ["MIGRATION_DATABASE_URL", "RUNTIME_DATABASE_URL", "ADMISSION_DATABASE_URL"],
            "srv-api": common + ["ADMISSION_DATABASE_URL", "STOARAMA_ADMISSION_EXECUTOR_ROLE"],
            "srv-control": common,
            "crn-health": common, "crn-live": common, "crn-summary": common,
            "crn-preopen": common, "crn-media": common, "crn-relay": common,
        }
        if invalid_api:
            envs["srv-api"].append("MIGRATION_DATABASE_URL")
        names = {
            "crn-migrate": "stoarama-db-migrate", "srv-api": "stoarama-api",
            "srv-control": "stoarama-recorder-control", "crn-health": "stoarama-recording-health",
            "crn-live": "stoarama-recording-live-health", "crn-summary": "stoarama-recording-health-summary",
            "crn-preopen": "stoarama-recording-preopen", "crn-media": "stoarama-recording-media-health",
            "crn-relay": "stoarama-relay-connectivity",
        }
        self.state.write_text(json.dumps({"commit": self.commit, "envs": envs, "names": names, "calls": []}))
        runtime_ids = "srv-control crn-health crn-live crn-summary crn-preopen crn-media crn-relay"
        if omit_runtime:
            runtime_ids = "srv-control crn-health crn-live crn-summary crn-preopen crn-media"
        env = {
            "PATH": str(self.bin) + os.pathsep + os.environ["PATH"],
            "HOME": str(self.root / "empty-home"),
            "TMPDIR": str(self.root),
            "FAKE_RENDER_STATE": str(self.state),
            "RENDER_API_KEY": "redacted-test-token",
            "RENDER_MIGRATOR_ID": "crn-migrate",
            "RENDER_API_SERVICE_ID": "srv-api",
            "RENDER_RUNTIME_SERVICE_IDS": runtime_ids,
            "COMMIT_SHA": self.commit,
        }
        return subprocess.run([str(SCRIPT)], cwd=self.repo, env=env, text=True, capture_output=True)

    def test_exact_commit_migrates_before_any_runtime_deploy(self):
        completed = self.run_deploy()
        self.assertEqual(completed.returncode, 0, completed.stderr)
        calls = json.loads(self.state.read_text())["calls"]
        mutations = [(c["method"], c["path"]) for c in calls if c["method"] == "POST"]
        self.assertEqual(
            mutations,
            [
                ("POST", "/v1/services/crn-migrate/deploys"),
                ("POST", "/v1/cron-jobs/crn-migrate/runs"),
                ("POST", "/v1/services/srv-control/deploys"),
                ("POST", "/v1/services/crn-health/deploys"),
                ("POST", "/v1/services/crn-live/deploys"),
                ("POST", "/v1/services/crn-summary/deploys"),
                ("POST", "/v1/services/crn-preopen/deploys"),
                ("POST", "/v1/services/crn-media/deploys"),
                ("POST", "/v1/services/crn-relay/deploys"),
                ("POST", "/v1/services/srv-api/deploys"),
            ],
        )
        deploys = [json.loads(c["body"])["commitId"] for c in calls if c["method"] == "POST" and c["path"].endswith("/deploys")]
        self.assertEqual(deploys, [self.commit] * 9)
        combined = completed.stdout + completed.stderr
        self.assertNotIn("redacted-test-token", combined)

    def test_privileged_url_on_api_fails_before_any_deploy(self):
        completed = self.run_deploy(invalid_api=True)
        self.assertNotEqual(completed.returncode, 0)
        calls = json.loads(self.state.read_text())["calls"]
        self.assertFalse(any(c["method"] == "POST" for c in calls), calls)
        self.assertIn("invalid api env-key manifest", completed.stderr)

    def test_omitted_runtime_fails_before_any_deploy(self):
        completed = self.run_deploy(omit_runtime=True)
        self.assertNotEqual(completed.returncode, 0)
        calls = json.loads(self.state.read_text())["calls"]
        self.assertFalse(any(c["method"] == "POST" for c in calls), calls)
        self.assertIn("runtime service manifest is not exact", completed.stderr)


if __name__ == "__main__":
    unittest.main()
