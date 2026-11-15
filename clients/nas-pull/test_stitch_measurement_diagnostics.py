"""Checked policy source plus recorded/synthetic results; no media/network/auth."""
import copy
import datetime
import gzip
import hashlib
import importlib.util
import json
from pathlib import Path
import subprocess
import tempfile
import threading
from types import SimpleNamespace
import unittest
from unittest import mock

import test_stoarama_pull as base

pull = base.pull
ROOT = Path(__file__).parent


class MeasurementDiagnosticTests(unittest.TestCase):
    def setUp(self):
        namespace = dict(__name__="test_driver")
        exec(compile(pull.STITCH_MEASURE_DRIVER, "<reviewed diagnostic driver>", "exec"), namespace)
        self.driver = namespace["run_driver"]
        self.runner = ROOT / "v85r/runner.py"
        self.runner_sha = hashlib.sha256(self.runner.read_bytes()).hexdigest()
        spec = importlib.util.spec_from_file_location("original_measurement_runner", self.runner)
        self.original = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(self.original)
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.request_path = Path(self.temp.name) / "request.json"
        raw = (ROOT / "testdata/v85r-october-evidence.json.gz").read_bytes()
        self.assertEqual(hashlib.sha256(raw).hexdigest(), "e62739b52a73cdd10dd20ab8d023aab801f0efbb1ef165cf8bba23bf9ce374e2")
        self.cases = json.loads(gzip.decompress(raw))["rows"]

    def request(self, row):
        bindings = row["source_bindings"]
        request = dict(prev=bindings[0], next=bindings[1], tools=dict(ffmpeg="synthetic", ffprobe="synthetic"), actual_span=False)
        self.request_path.write_text(json.dumps(request))
        return request

    def original_main(self, request, native):
        with mock.patch.object(self.original.subprocess, "run", side_effect=native):
            try:
                result = self.original.run(request)
            except Exception:
                result = dict(policy="v85r", join=False, nomination=False, status="UNKNOWN_measurement_failed",
                    decision="hold", reason="measurement_failed")
        return json.dumps(result, allow_nan=False)

    def native_result(self, row, **extra):
        row = copy.deepcopy(row)
        # The archived research wrapper has extra custody fields. The pinned
        # live native producer emits exactly these three identity fields.
        row["source_bindings"] = [{k:b[k] for k in ("clip_id","sha256","size_bytes")} for b in row["source_bindings"]]
        row["spatial_provenance"]["source_bindings"] = copy.deepcopy(row["source_bindings"])
        return dict(evidence_row=row, diagnostic_window20=copy.deepcopy(row["diagnostic_window20"]), **extra)

    def invoke(self, native):
        return self.driver(str(self.runner), str(self.request_path), self.runner_sha, native)

    def test_recorded_positive_joins_serialization_and_native_calls_are_identical(self):
        for case in self.cases:
            with self.subTest(case=case["case"]):
                row = case["row"]
                request = self.request(row)
                calls = []
                def native(*args, **kwargs):
                    calls.append(hashlib.sha256(json.dumps(dict(args=args,kwargs=kwargs),sort_keys=True).encode()).hexdigest())
                    return SimpleNamespace(stdout=json.dumps(self.native_result(row)))
                expected = self.original_main(request, native)
                result, encoded, diagnostic = self.invoke(native)
                self.assertTrue(encoded == expected, "driver changed a recorded policy outcome or serialization")
                self.assertEqual(json.loads(encoded), result)
                self.assertEqual((result["decision"],result["join"],result["nomination"],result["status"]),
                    ("join",True,*case["expected"]))
                self.assertEqual(len(calls),2)
                self.assertEqual(calls[0],calls[1],"native command, request, environment or timeout changed")

    def test_policy_split_and_safety_hold_are_explicitly_exercised(self):
        positive = self.cases[0]["row"]
        split = copy.deepcopy(positive)
        split["spatial_temporal_diagnostic"].update(tail_dx_pixels_per_frame=0,tail_dy_pixels_per_frame=0,
            head_dx_pixels_per_frame=0,head_dy_pixels_per_frame=0,motion_observable=False,
            ambiguities=["no_observable_motion"],texture_gradient_mad=3,minimum_spatial_novelty_margin=2,
            heldout_pixels=1000,forward_heldout_residual_mad=10,backward_heldout_residual_mad=10,
            internal_heldout_residual_mad=.1,direction_speed_disagreement=0,
            max_acceleration_pixels_per_frame_squared=0,boundary_unwarped_mad=10)
        missing = copy.deepcopy(positive);del missing["full_source_integrity"]
        for row, decision, status in ((split,"split","REJECT_stationary_severe_seam_forecast"),
                (missing,"hold","UNKNOWN_missing_authenticated_evidence")):
            with self.subTest(decision=decision):
                request = self.request(row)
                native = lambda *a,**k:SimpleNamespace(stdout=json.dumps(self.native_result(row)))
                expected = self.original_main(request,native)
                result,encoded,_ = self.invoke(native)
                self.assertEqual((result["decision"],result["status"],result["join"]),(decision,status,False))
                self.assertTrue(encoded==expected,"driver changed a policy negative or safety HOLD")

    def test_frozen_current_policy_join_and_identity_hold_are_preserved(self):
        row = self.cases[0]["row"]
        for valid in (True,False):
            request = self.request(row)
            keys = [[b["clip_id"],b["sha256"],b["size_bytes"]] for b in row["source_bindings"]]
            request["frozen"] = dict(source_key=keys,nomination=valid,status=self.original.APPROVED)
            self.request_path.write_text(json.dumps(request))
            native = lambda *a,**k:SimpleNamespace(stdout=json.dumps(self.native_result(row)))
            expected = self.original_main(request,native)
            result,encoded,_ = self.invoke(native)
            self.assertEqual(result["decision"],"join" if valid else "hold")
            self.assertTrue(encoded==expected,"driver changed frozen current-policy admission")

    def test_returned_source_integrity_failure_has_its_own_bounded_class(self):
        row = self.cases[0]["row"]
        request = self.request(row)
        private = "Bearer SYNTHETIC_TOKEN /private/untrusted-path"
        native = lambda *a, **k: SimpleNamespace(stdout=json.dumps(self.native_result(row, source_integrity_error=private)))
        expected = self.original_main(request, native)
        result, encoded, diagnostic = self.invoke(native)
        self.assertTrue(encoded == expected)
        self.assertEqual(result["decision"], "hold")
        self.assertTrue(diagnostic["v"])
        self.assertEqual(diagnostic["e"], "NativeSourceIntegrity")
        self.assertNotIn(private, json.dumps(diagnostic))

    def test_native_exception_class_phase_and_status_survive_without_text(self):
        row = self.cases[0]["row"]
        for error in (subprocess.TimeoutExpired("SYNTHETIC_TOKEN", 185),
                subprocess.CalledProcessError(-9, "SYNTHETIC_TOKEN", stderr="/private/untrusted-path"),
                OSError("SYNTHETIC_TOKEN"), ValueError("SYNTHETIC_TOKEN"), KeyError("SYNTHETIC_TOKEN")):
            with self.subTest(error=type(error).__name__):
                request = self.request(row)
                def failed(*a, **k): raise error
                expected = self.original_main(request, failed)
                result, encoded, diagnostic = self.invoke(failed)
                self.assertTrue(encoded == expected)
                self.assertEqual(result["decision"], "hold")
                self.assertEqual(diagnostic["e"], type(error).__name__)
                self.assertEqual((diagnostic["s"],diagnostic["p"],diagnostic["i"]), ("native","requested","pair"))
                self.assertNotIn("SYNTHETIC_TOKEN", json.dumps(diagnostic))
                self.assertLessEqual(len(json.dumps(diagnostic,separators=(",",":")).encode()),320)

    def test_actual_span_retry_is_distinguishable(self):
        row = self.cases[0]["row"]
        request = self.request(row)
        count = 0
        def native(*args, **kwargs):
            nonlocal count
            count += 1
            if count % 2:
                return SimpleNamespace(stdout=json.dumps(dict(diagnostic_window20=dict(reason="prev_clip_cut_short"))))
            self.assertTrue(json.loads(kwargs["input"])["actual_span"])
            raise subprocess.TimeoutExpired("SYNTHETIC_TOKEN", 185)
        expected = self.original_main(request, native)
        _, encoded, diagnostic = self.invoke(native)
        self.assertTrue(encoded == expected)
        self.assertEqual((diagnostic["s"],diagnostic["p"],diagnostic["e"]), ("native","actual","TimeoutExpired"))

    def test_malformed_native_result_holds_with_safe_parse_class(self):
        self.request(self.cases[0]["row"])
        for text, error in (("{", "JSONDecodeError"), ("[]", "AttributeError")):
            with self.subTest(text=text):
                result, _, diagnostic = self.invoke(lambda *a, **k: SimpleNamespace(stdout=text))
                self.assertEqual(result["decision"], "hold")
                self.assertEqual((diagnostic["s"],diagnostic["e"]), ("result",error))

    def test_checked_source_tamper_cannot_execute(self):
        self.request(self.cases[0]["row"])
        native = mock.Mock()
        result, _, diagnostic = self.driver(str(self.runner), str(self.request_path), "0"*64, native)
        self.assertEqual(result["decision"], "hold")
        self.assertEqual(diagnostic["s"], "driver")
        native.assert_not_called()

    def proof_stop(self):
        task = dict(expires_at=(datetime.datetime.now(datetime.timezone.utc)+datetime.timedelta(minutes=5)).isoformat(),
            plan_sha256="a"*64)
        return pull.StitchStop(SimpleNamespace(stitch_proof_enabled=True), threading.Event(), task, pull.StitchRunKind.PROOF)

    def test_parent_diagnostic_and_outer_logging_never_use_raw_exception_text(self):
        stop = self.proof_stop()
        value = dict(s="native",p="requested",i="pair",e="CalledProcessError",r=-9,t=None,n=False,v=False,h="measurement_failed")
        logs = []
        with mock.patch.object(pull, "log", side_effect=lambda *a:logs.append(a)):
            pull.stitch_measure_diagnostic(stop,"STITCH_MEASURE "+json.dumps(value,separators=(",",":")))
        self.assertEqual(stop.failure["returncode"], -9)
        self.assertEqual(stop.failure["stage"], "native")
        for failure in (pull.BenchmarkDeadline("SYNTHETIC_TOKEN"), OSError("/private/SYNTHETIC_TOKEN")):
            event = threading.Event()
            def failed(*args): event.set();raise failure
            with mock.patch.object(pull,"stitch_priority"), mock.patch.object(pull,"stitch_proof_once",side_effect=failed), \
                    mock.patch.object(pull,"log",side_effect=lambda *a:logs.append(a)):
                pull.stitch_loop(SimpleNamespace(stitch_proof_enabled=True),None,event)
        self.assertNotIn("SYNTHETIC_TOKEN",json.dumps(logs))

    def test_context_preserves_first_input_and_exception_and_driver_argv_limits(self):
        stop = self.proof_stop()
        with mock.patch.object(pull,"log"):
            with self.assertRaises(pull.MediaCertificationError):
                with pull.stitch_measure_stage(stop,pull.StitchMeasureStage.DRIVER):
                    with pull.stitch_measure_stage(stop,pull.StitchMeasureStage.SOURCE,pull.StitchMeasureInput.LEFT):
                        raise pull.MediaCertificationError("/private/SYNTHETIC_TOKEN")
        self.assertEqual((stop.failure["stage"],stop.failure["input"],stop.failure["error"]),
                         ("source","left","MediaCertificationError"))
        args = pull.stitch_measure_driver_argv(ROOT/"v85r",self.request_path)
        self.assertLessEqual(len(args),100)
        self.assertTrue(all(len(item)<=2048 for item in args))

    def test_measurement_boundaries_preserve_exception_input_and_stage(self):
        def clip(identity):
            binding = dict(clip_id=identity,sha256="a"*64,size_bytes=1,nominal_seconds=60,
                recording_id=7,start_utc="2026-11-15T00:00:00Z",end_utc="2026-11-15T00:01:00Z")
            return dict(source_id=identity,relative_path="synthetic.mp4",sha256="a"*64,size_bytes=1,
                recording_id=7,start_at=binding["start_utc"],end_at=binding["end_utc"],feature_input=binding)
        scenarios = (("source","left",OSError),("source","right",OSError),
            ("feature","left",pull.StitchHold),("feature","right",pull.StitchHold),
            ("frozen","pair",KeyError),("request","pair",OSError),
            ("driver","pair",pull.BenchmarkDeadline),("result","pair",json.JSONDecodeError))
        for stage, side, error in scenarios:
            with self.subTest(stage=stage,side=side):
                stop = self.proof_stop();left,right=clip(1),clip(2)
                if stage == "feature":
                    (left if side == "left" else right)["feature_input"]["nominal_seconds"] = 0
                calls = 0
                def hashed(*args):
                    nonlocal calls
                    calls += 1
                    if stage == "source" and calls == (1 if side == "left" else 2):
                        raise OSError("/private/SYNTHETIC_TOKEN")
                    return self.request_path,None,None
                def measured(*args):
                    if stage == "driver": raise pull.BenchmarkDeadline("SYNTHETIC_TOKEN")
                    args[3].write_text("{")
                    return dict(stderr="")
                with mock.patch.object(pull,"stitch_hash",side_effect=hashed), \
                        mock.patch.object(pull,"stitch_frozen",side_effect=KeyError("SYNTHETIC_TOKEN") if stage == "frozen" else None,return_value=None), \
                        mock.patch.object(pull,"stitch_run",side_effect=measured), mock.patch.object(pull,"log"):
                    if stage == "request":
                        with mock.patch.object(pull.Path,"write_bytes",side_effect=OSError("SYNTHETIC_TOKEN")):
                            with self.assertRaises(error):
                                pull.stitch_v85r_measure(None,None,left,right,{},Path(self.temp.name),stop.deadline,stop,ROOT/"v85r")
                    else:
                        with self.assertRaises(error):
                            pull.stitch_v85r_measure(None,None,left,right,{},Path(self.temp.name),stop.deadline,stop,ROOT/"v85r")
                self.assertEqual((stop.failure["stage"],stop.failure["input"],stop.failure["error"]),
                    (stage,side,error.__name__))
                self.assertNotIn("SYNTHETIC_TOKEN",json.dumps(stop.failure))

    def test_invalid_diagnostic_never_falls_back_to_text(self):
        value = dict(s="native",p="requested",i="pair",e="CalledProcessError",r=-9,t=None,n=False,v=False,h="measurement_failed")
        bad = ["SYNTHETIC_TOKEN", "STITCH_MEASURE "+"x"*337,
            "STITCH_MEASURE "+json.dumps(dict(value,e="SYNTHETIC_TOKEN")),
            "STITCH_MEASURE "+json.dumps(dict(value,r=True)),
            "STITCH_MEASURE "+json.dumps(dict(value,extra="SYNTHETIC_TOKEN")),
            'STITCH_MEASURE {"s":"native","s":"SYNTHETIC_TOKEN"}']
        for text in bad:
            with self.subTest(text=text[:30]), self.assertRaises((pull.StitchHold,ValueError)):
                pull.stitch_measure_diagnostic(self.proof_stop(),text)


if __name__ == "__main__": unittest.main()
