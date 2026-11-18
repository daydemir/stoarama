import hashlib
import json
import os
from pathlib import Path
import tempfile
import unittest
import subprocess
from unittest.mock import patch
import cli_binding as binding
from cli_binding_fixture import environment
from cli_exec_guard import install_guard

install_guard()


class BindingTests(unittest.TestCase):
    def test_exec_rechecks_actual_bytes_immediately_and_uses_the_bound_path(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            with patch.dict(os.environ, self.fixture(root)), patch.object(binding.os, 'execv') as execute:
                validated = binding.load()
                binding.exec_bound_cli(['nas-cleanup', 'verify-recovery'])
                path = str(root/'stoaramactl.bin')
                execute.assert_called_once_with(path, [path, 'nas-cleanup', 'verify-recovery'])
                execute.reset_mock()
                (root/'stoaramactl.bin').write_text('changed before exec')
                with patch.object(binding, 'load', return_value=validated), self.assertRaisesRegex(binding.ContractError, 'immediately before exec'):
                    binding.exec_bound_cli(['nas-cleanup', 'verify-recovery'])
                execute.assert_not_called()

    def test_execution_guard_blocks_another_named_cli_and_retired_bytes_under_any_name(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            env = self.fixture(root)
            marker = root/'executed'
            wrong = root/'stoaramactl_other'
            wrong.write_text('#!/bin/sh\ntouch "' + str(marker) + '"\n')
            wrong.chmod(0o700)
            with patch.dict(os.environ, env), self.assertRaises(AssertionError):
                subprocess.run([str(wrong)], check=True)
            self.assertFalse(marker.exists())
            alias = root/'verifier_alias'
            alias.write_bytes(wrong.read_bytes())
            alias.chmod(0o700)
            with patch.object(binding, 'digest', return_value=binding.RETIRED_CLI_SHA256), self.assertRaisesRegex(AssertionError, 'retired'):
                subprocess.run([str(alias)], check=True)
            self.assertFalse(marker.exists())

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
        for text in (r"subprocess.run([b'/Users/jelly/tmp-safeguards/\x73toaramactl'])",
                     "subprocess.run(['/Users/jelly/tmp-safeguards/stoaramactl_old'])",
                     "#!/bin/sh\nexec /Users/jelly/tmp-safeguards/stoaramactl_a889 \"$@\"",
                     "subprocess.run('exec stoa\"\"ramactl nas-cleanup verify-recovery', shell=True)",
                     "subprocess.run(['stoa' + 'ramactl'])",
                     "subprocess.run([f\"sto{'ara'}mactl\"])",
                     "#!/bin/sh\nstoaramactl\n",
                     "#!/bin/sh\nstoaramactl --help\n",
                     "#!/bin/sh\nexec \"$(command -v stoaramactl)\" \"$@\"",
                     "subprocess.run(\"exec stoaramactl nas-cleanup verify-recovery\", shell=True)",
                     "subprocess.run(['sh', '-c', 'exec stoaramactl nas-cleanup verify-recovery'])",
                     "pins = {'cli_sha256': '" + 'c'*64 + "'}",
                     "pins['CLI_SHA256'] = '" + 'c'*64 + "'",
                     "exec /Users/jelly/tmp-safeguards/stoaramactl \"$@\"",
                     "subprocess.run(['stoaramactl', 'nas-cleanup', 'verify-recovery'])",
                     "subprocess.run([str(Path('/Users/jelly/tmp-safeguards') / 'stoaramactl')])",
                     "CLI_SHA256 = '" + binding.RETIRED_CLI_SHA256 + "'",
                     "CLI_SHA256: str = '" + 'c'*64 + "'",
                     "#!/bin/sh\nCLI_SHA256=" + 'c'*64 + "\nexec /bound/stoaramactl \"$@\"",
                     "CLI_SHA256 = '" + 'c'*64 + "'"):
            with self.subTest(text=text), self.assertRaises(binding.ContractError):
                binding.verify_references(text)


if __name__ == '__main__':
    unittest.main()
