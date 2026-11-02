
"""Pure timestamp regression checks; never reads credentials, NAS, R2 or PostgreSQL."""
import ast
import datetime
import hashlib
import json
from pathlib import Path
import re
import unittest

ROOT=Path(__file__).resolve().parent

def load_parser(name):
    tree=ast.parse((ROOT/name).read_text())
    nodes=[node for node in tree.body if isinstance(node,ast.FunctionDef) and node.name=='instant']
    if len(nodes)!=1:raise AssertionError('expected one isolated timestamp parser')
    scope={'datetime':datetime,'re':re}
    exec(compile(ast.Module(body=nodes,type_ignores=[]),name,'exec'),scope)
    return scope['instant']

class TimestampTests(unittest.TestCase):
    def test_valid_postgres_precision_and_equivalent_offsets(self):
        for name in ('compile_full_manifest_20261102.py','repair_manifest_timestamps.py'):
            parse=load_parser(name)
            for digits in range(1,7):
                fraction='123456'[:digits]
                expected=datetime.datetime(2026,11,2,12,34,56,int(fraction.ljust(6,'0')),datetime.timezone.utc)
                self.assertEqual(parse('2026-11-02T12:34:56.'+fraction+'Z'),expected)
                self.assertEqual(parse('2026-11-02T07:34:56.'+fraction+'-05:00'),expected)
            self.assertEqual(parse('2026-11-02T12:34:56Z'),datetime.datetime(2026,11,2,12,34,56,tzinfo=datetime.timezone.utc))

    def test_naive_malformed_and_overprecision_fail_closed(self):
        for name in ('compile_full_manifest_20261102.py','repair_manifest_timestamps.py'):
            parse=load_parser(name)
            for value in (None,'','2026-11-02','2026-11-02T12:34:56','2026-11-02T12:34:56.1234567Z','2026-02-30T12:34:56Z','2026-11-02T12:34:56+99:00','2026-11-02T12:34:56+00:60','2026-11-02T12:34:56+01:99',' 2026-11-02T12:34:56Z'):
                with self.assertRaises((TypeError,ValueError)):parse(value)

    def test_half_open_boundaries_preserve_protected_overlap(self):
        for name in ('compile_full_manifest_20261102.py','repair_manifest_timestamps.py'):
            parse=load_parser(name)
            start=parse('2026-11-01T00:00:00-04:00')
            end=parse('2026-11-29T00:00:00-05:00')
            self.assertEqual((end-start).total_seconds(),673*3600)
            before_start=parse('2026-11-01T03:59:00Z')
            self.assertFalse(before_start<end and start>start)
            self.assertTrue(parse('2026-11-01T03:59:59.99999Z')<end and parse('2026-11-01T04:00:00.00001Z')>start)
            self.assertEqual(parse('2026-11-01T04:00:00.00001Z')-start,datetime.timedelta(microseconds=10))


    def test_compiler_and_preparer_accept_same_content_bound_recipes(self):
        def recipes(name):
            tree=ast.parse((ROOT/name).read_text())
            nodes=[n for n in tree.body if
                (isinstance(n,ast.FunctionDef) and n.name in ('cleanup_recipe','checked_manifest_recipe')) or
                (isinstance(n,ast.Assign) and any(isinstance(t,ast.Name) and t.id in ('SUPPORTED_MANIFEST_COMPILERS','INDEX_SHA','POLICY_SHA') for t in n.targets))]
            scope={'hashlib':hashlib,'json':json}
            exec(compile(ast.Module(body=nodes,type_ignores=[]),name,'exec'),scope)
            return scope
        compiler=recipes('compile_full_manifest_20261102.py')
        preparer=recipes('prepare_full_manifest.py')
        repair=recipes('repair_manifest_timestamps.py')
        tree=ast.parse((ROOT/'compile_full_manifest_20261102.py').read_text())
        assignment=next(n for n in tree.body if isinstance(n,ast.Assign) and any(isinstance(t,ast.Name) and t.id=='recipe' for t in n.targets))
        exec(compile(ast.Module(body=[assignment],type_ignores=[]),'compiler-recipe','exec'),compiler)
        self.assertEqual(preparer['checked_manifest_recipe']({'recipe':compiler['recipe']}),compiler['recipe'])
        for name in ('full-cleanup-v1','full-cleanup-v2-strict-time'):
            digest=compiler['cleanup_recipe'](name)
            self.assertEqual(preparer['checked_manifest_recipe']({'recipe':digest}),digest)
            self.assertEqual(repair['checked_manifest_recipe']({'recipe':digest}),digest)
        for invalid in ('',None,'f'*64):
            with self.assertRaises(ValueError):preparer['checked_manifest_recipe']({'recipe':invalid})

if __name__=='__main__':unittest.main()
