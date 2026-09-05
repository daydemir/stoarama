#!/usr/bin/env python3
"""Static safety checks for the exact-commit worker artifact workflow."""

from pathlib import Path
import re
import unittest


ROOT = Path(__file__).resolve().parents[2]
WORKFLOW = ROOT / ".github" / "workflows" / "build-stoaramactl-worker.yml"


class WorkerArtifactWorkflowTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.text = WORKFLOW.read_text(encoding="utf-8")

    def test_is_manual_only_and_read_only(self) -> None:
        event_block = self.text.split("\npermissions:\n", 1)[0]
        self.assertIn("\non:\n  workflow_dispatch:\n", event_block)
        for event in ("push", "pull_request", "schedule", "workflow_run"):
            self.assertNotRegex(event_block, rf"(?m)^  {event}:")
        self.assertIn("\npermissions:\n  contents: read\n", self.text)
        self.assertNotIn("write", self.text.split("\nconcurrency:\n", 1)[0])

    def test_every_action_is_commit_pinned(self) -> None:
        uses = re.findall(r"(?m)^\s*uses:\s*([^\s#]+)", self.text)
        self.assertEqual(len(uses), 5)
        for action in uses:
            self.assertRegex(action, r"^[^@]+@[0-9a-f]{40}$")
        self.assertIn(
            "actions/upload-artifact@ea165f8d65b6e75b540449e92b4886f43607fa02",
            uses,
        )

    def test_source_is_exact_ancestor_and_clones_are_full_clean_repositories(self) -> None:
        required = (
            "^[0-9a-f]{40}$",
            "cat-file -e \"${REQUESTED_COMMIT}^{commit}\"",
            "merge-base --is-ancestor",
            "MINIMUM_WORKER_COMMIT: c8c64847eff3ab8aa4211e6865adc9a0ac51a4e8",
            'merge-base --is-ancestor "$MINIMUM_WORKER_COMMIT" "$REQUESTED_COMMIT"',
            '[[ "$GITHUB_REF" != "refs/heads/$DEFAULT_BRANCH" ]]',
            "rev-parse --is-shallow-repository",
            '[[ ! -d "$source/.git" ]]',
            "status --porcelain=v1 --untracked-files=all",
        )
        for fragment in required:
            self.assertIn(fragment, self.text)
        self.assertEqual(self.text.count("fetch-depth: 0"), 3)
        self.assertEqual(self.text.count("persist-credentials: false"), 3)

    def test_build_is_reproducible_and_metadata_is_verified(self) -> None:
        for fragment in (
            "go-version: 1.25.14",
            'GOTOOLCHAIN: local',
            'CGO_ENABLED: "1"',
            "GOOS: linux",
            "GOARCH: amd64",
            "GOAMD64: v1",
            'SOURCE_DATE_EPOCH="$(git -C source-a show -s --format=%ct HEAD)"',
            "export SOURCE_DATE_EPOCH",
            "go build -mod=readonly -trimpath -buildvcs=true",
            'build_one source-a "$root/stoaramactl-a"',
            'build_one source-b "$root/stoaramactl-b"',
            'cmp --silent "$root/stoaramactl-a" "$root/stoaramactl-b"',
            'metadata_value "$binary" vcs.revision',
            'metadata_value "$binary" vcs.time',
            'metadata_value "$binary" vcs.modified',
            'metadata_value "$binary" -trimpath',
        ):
            self.assertIn(fragment, self.text)

    def test_four_hour_budget_tests_and_job_timeout_are_fixed(self) -> None:
        job_timeout = re.search(r"(?m)^    timeout-minutes: (\d+)$", self.text)
        test_timeout = re.search(r"-timeout=(\d+)h", self.text)
        self.assertIsNotNone(job_timeout)
        self.assertIsNotNone(test_timeout)
        self.assertGreater(int(job_timeout.group(1)), int(test_timeout.group(1)) * 60)
        self.assertLessEqual(int(job_timeout.group(1)), 360)
        self.assertIn("TestJoinedWorkerTaskHasHardDeadline", self.text)
        self.assertIn("TestJoinedWorkerTaskBudgetCoversMeasuredStrictHourRuntime", self.text)
        self.assertIn("TestJoinedWorkerTaskPreservesCompletedSuccessAtDeadline", self.text)
        self.assertIn('go test -mod=readonly ./cmd/stoaramactl -list "$test_regex"', self.text)
        self.assertIn('grep -Fxq "$required_test" <<< "$test_list"', self.text)
        self.assertIn('-json | tee "$test_root/results.json"', self.text)
        self.assertIn('event.get("Action") == "pass"', self.text)
        self.assertIn("-timeout=4h", self.text)
        tests_at = self.text.index("- name: Run focused four-hour worker-budget tests")
        builds_at = self.text.index("- name: Build twice and verify embedded provenance")
        attestation_at = self.text.index("- name: Write nonsecret build attestation")
        upload_at = self.text.index("- name: Upload binary and attestation")
        self.assertLess(tests_at, builds_at)
        self.assertLess(builds_at, attestation_at)
        self.assertLess(attestation_at, upload_at)
        self.assertIn(
            'root="$(mktemp --directory "$RUNNER_TEMP/stoaramactl-worker.XXXXXXXX")"',
            self.text,
        )

    def test_only_binary_and_nonsecret_attestation_are_uploaded(self) -> None:
        self.assertNotRegex(self.text, r"\$\{\{\s*secrets\.")
        for forbidden in (
            "DATABASE_URL",
            "API_TOKEN",
            "R2_ACCESS_KEY",
            "R2_SECRET_ACCESS_KEY",
            "FFMPEG_BIN",
            "FFPROBE_BIN",
            "curl ",
            "wget ",
        ):
            self.assertNotIn(forbidden, self.text)
        self.assertRegex(
            self.text,
            r"path: \|\n\s+\$\{\{ steps\.artifact\.outputs\.artifact_dir \}\}/stoaramactl\n"
            r"\s+\$\{\{ steps\.artifact\.outputs\.artifact_dir \}\}/attestation\.json\n",
        )
        self.assertIn("retention-days: 3", self.text)
        self.assertIn("if-no-files-found: error", self.text)
        self.assertIn('"reproducible_build_count": 2', self.text)
        self.assertIn('"vcs_modified": False', self.text)


if __name__ == "__main__":
    unittest.main()
