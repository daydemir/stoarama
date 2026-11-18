import hashlib
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
import cli_binding as binding
from cli_binding_fixture import environment


class BindingTests(unittest.TestCase):
    def fixture(self, root):
        launcher = root / 'stoaramactl'
        launcher.write_text('#!/bin/sh\nexit 0\n')
        return environment(root, launcher)

    def rewrite(self, env, change):
        path = Path(env['STOARAMA_CLI_BINDING'])
        record = json.loads(path.read_bytes())
        change(record)
        raw = json.dumps(record).encode()
        path.write_bytes(raw)
        env['STOARAMA_CLI_BINDING_SHA256'] = hashlib.sha256(raw).hexdigest()

    def test_runner_and_d_resolve_the_same_bound_artifact_and_launcher(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            with patch.dict(os.environ, self.fixture(root)):
                self.assertEqual(binding.bound_cli(), root/'stoaramactl.bin')
                self.assertEqual(binding.bound_launcher(), root/'stoaramactl')
                self.assertEqual(binding.bound_launcher(root/'stoaramactl'), root/'stoaramactl')
                with self.assertRaisesRegex(binding.ContractError, 'different CLI'):
                    binding.bound_launcher('/Users/jelly/tmp-safeguards/stoaramactl')

    def test_old_path_old_hash_second_lineage_and_changed_artifact_are_rejected(self):
        mutations = [
            lambda r: r['binary'].update(path='/Users/jelly/tmp-safeguards/stoaramactl'),
            lambda r: r['binary'].update(sha256=binding.RETIRED_CLI_SHA256),
            lambda r: r.update(second_cli=r['binary']),
        ]
        for change in mutations:
            with self.subTest(change=change), tempfile.TemporaryDirectory() as tmp:
                env = self.fixture(Path(tmp))
                self.rewrite(env, change)
                with patch.dict(os.environ, env), self.assertRaises(binding.ContractError):
                    binding.bound_cli()
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            env = self.fixture(root)
            (root/'stoaramactl.bin').write_text('changed')
            with patch.dict(os.environ, env), self.assertRaisesRegex(binding.ContractError, 'digest differs'):
                binding.bound_cli()

    def test_runtime_source_cannot_reintroduce_another_cli_path_or_hash(self):
        binary = dict(path='/bound/stoaramactl.bin', sha256='a'*64)
        launcher = dict(path='/bound/stoaramactl', sha256='b'*64)
        for text in ("exec /Users/jelly/tmp-safeguards/stoaramactl \"$@\"",
                     "subprocess.run(['stoaramactl', 'nas-cleanup', 'verify-recovery'])",
                     "subprocess.run([str(Path('/Users/jelly/tmp-safeguards') / 'stoaramactl')])",
                     "CLI_SHA256 = '" + binding.RETIRED_CLI_SHA256 + "'",
                     "CLI_SHA256: str = '" + 'c'*64 + "'",
                     "#!/bin/sh\nCLI_SHA256=" + 'c'*64 + "\nexec /bound/stoaramactl \"$@\"",
                     "CLI_SHA256 = '" + 'c'*64 + "'"):
            with self.subTest(text=text), self.assertRaises(binding.ContractError):
                binding.verify_references(text, binary, launcher)


if __name__ == '__main__':
    unittest.main()
