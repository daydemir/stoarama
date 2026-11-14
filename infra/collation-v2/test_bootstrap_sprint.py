import base64
import hashlib
import importlib.util
import io
import json
import pathlib
import subprocess
import tempfile
import unittest
from unittest import mock

spec=importlib.util.spec_from_file_location('bootstrap',pathlib.Path(__file__).with_name('bootstrap-sprint.py'))
b=importlib.util.module_from_spec(spec);spec.loader.exec_module(b)

class BootstrapTest(unittest.TestCase):
    def setUp(self):
        self.sources={'commit':'a'*40,'files':{n:base64.b64encode(n.encode()).decode() for n in ('capability-worker.py','capability_export.py')}}
        self.policy={'allow_start':True,'additional_budget_usd':150,'worker_source_sha256':{n:hashlib.sha256(n.encode()).hexdigest() for n in self.sources['files']}}
        self.review={'commit':'a'*40,'review_passed':True,'merged_default':True}
        self.tracker={'prepared':True,'reviewed_commit':'a'*40,'resources':{'cpu-'+s:{'receipt':{'id':i}} for i,s in enumerate('abc',1)},'firewall':{'receipt':{'status':'succeeded','droplet_ids':[1,2,3],'tags':[],'inbound_rules':[{'protocol':'tcp','ports':'22','sources':{'addresses':['8.8.4.4/32']}},{'protocol':'tcp','ports':'8080-8101','sources':{'addresses':['10.10.0.3/32']}}]}}}
    def test_strict_ssh_uses_existing_identity_without_copy(self):
        argv=b.ssh_command('203.0.113.1','/private/known-hosts','print(1)')
        self.assertIn('StrictHostKeyChecking=yes',argv);self.assertIn('UserKnownHostsFile=/private/known-hosts',argv)
        self.assertNotIn('-i',argv);self.assertNotIn('scp',argv);self.assertEqual(argv[-2],'root@203.0.113.1')
    def test_capability_only_on_stdin_never_argv(self):
        result=mock.Mock(returncode=0,stdout=b'{}')
        with mock.patch.object(b.subprocess,'run',return_value=result) as run:
            b.ssh('203.0.113.1','/pins',b.REMOTE_BOOTSTRAP,{'binary_get':'secret-test-capability'})
        args,kwargs=run.call_args
        self.assertNotIn('secret-test-capability',str(args));self.assertIn(b'secret-test-capability',kwargs['input'])
    def test_source_manifest_and_firewall_gate(self):
        self.assertEqual(b.validate(self.policy,self.review,self.sources,self.tracker),self.policy['worker_source_sha256'])
        self.policy['worker_source_sha256']['capability-worker.py']='wrong'
        with self.assertRaises(b.BootstrapError):b.validate(self.policy,self.review,self.sources,self.tracker)
    def test_public_worker_firewall_refused(self):
        self.tracker['firewall']['receipt']['inbound_rules'][1]['sources']['addresses']=['0.0.0.0/0']
        with self.assertRaises(b.BootstrapError):b.validate(self.policy,self.review,self.sources,self.tracker)
    def test_firewall_tag_source_cannot_broaden_private_rule(self):
        self.tracker['firewall']['receipt']['inbound_rules'][1]['sources']['tags']=['everyone']
        with self.assertRaises(b.BootstrapError):b.validate(self.policy,self.review,self.sources,self.tracker)
    def test_unapproved_main_never_connects(self):
        with tempfile.TemporaryDirectory() as tmp:
            policy=pathlib.Path(tmp)/'policy.json';policy.write_text(json.dumps({'allow_start':False}))
            review=pathlib.Path(tmp)/'review.json';review.write_text('{}')
            with mock.patch('sys.argv',['bootstrap','--apply','--policy',str(policy),'--review-receipt',str(review)]),mock.patch.object(b,'ssh') as ssh:
                with self.assertRaises(b.BootstrapError):b.main()
            ssh.assert_not_called()
    def test_missing_exact_volume_stops_before_mount_or_packages(self):
        payload={'host':{'name':'expected','volume_name':'best14-sprint-20261026-a','private_ip':'10.10.0.4'}}
        with mock.patch('sys.stdin',io.StringIO(json.dumps(payload))),mock.patch.object(subprocess,'check_output',return_value='expected'),mock.patch.object(pathlib.Path,'exists',return_value=False),mock.patch.object(subprocess,'run') as run:
            with self.assertRaises(RuntimeError):exec(compile(b.REMOTE_BOOTSTRAP,'remote','exec'),{})
        run.assert_not_called()
    def test_wrong_filesystem_never_formats_or_mounts(self):
        payload={'host':{'name':'expected','volume_name':'best14-sprint-20261026-a','private_ip':'10.10.0.4'}}
        with tempfile.TemporaryDirectory() as tmp:
            source=pathlib.Path(tmp)/'block';source.write_bytes(b'')
            with mock.patch('sys.stdin',io.StringIO(json.dumps(payload))),mock.patch.object(subprocess,'check_output',side_effect=['expected','xfs']),mock.patch.object(pathlib.Path,'exists',return_value=True),mock.patch.object(pathlib.Path,'is_symlink',return_value=True),mock.patch.object(pathlib.Path,'resolve',return_value=source),mock.patch.object(subprocess,'run') as run:
                with self.assertRaises(RuntimeError):exec(compile(b.REMOTE_BOOTSTRAP,'remote','exec'),{})
            run.assert_not_called()
    def test_mount_failure_cannot_write_workers_on_boot_root(self):
        payload={'host':{'name':'expected','volume_name':'best14-sprint-20261026-a','private_ip':'10.10.0.4'}}
        with tempfile.TemporaryDirectory() as tmp:
            root=pathlib.Path(tmp);block=root/'block';block.touch()
            (root/'byid').symlink_to(block);(root/'byuuid').symlink_to(block)
            mount=root/'mount'
            code=b.REMOTE_BOOTSTRAP.replace("device=pathlib.Path('/dev/disk/by-id/scsi-0DO_Volume_'+volume)","device=pathlib.Path("+repr(str(root/'byid'))+")")
            code=code.replace("pathlib.Path('/dev/disk/by-uuid',uuid)","pathlib.Path("+repr(str(root/'byuuid'))+")")
            code=code.replace("mount=pathlib.Path('/mnt')/volume","mount=pathlib.Path("+repr(str(mount))+")")
            values=['expected','ext4','uuid-test',json.dumps({'blockdevices':[{'type':'disk','uuid':'uuid-test','mountpoints':[]}]})]
            with mock.patch('sys.stdin',io.StringIO(json.dumps(payload))),mock.patch.object(subprocess,'check_output',side_effect=values),mock.patch('os.chown'),mock.patch.object(subprocess,'run',side_effect=subprocess.CalledProcessError(1,['mount'])) as run:
                with self.assertRaises(subprocess.CalledProcessError):exec(compile(code,'remote','exec'),{})
            self.assertEqual(run.call_args.args[0][0],'mount');self.assertEqual(run.call_count,1)
            self.assertFalse((mount/'workers').exists());self.assertEqual(mount.stat().st_mode&0o777,0o555)

    def test_remote_source_syntax_and_no_media_dispatch(self):
        compile(b.REMOTE_BOOTSTRAP,'remote','exec');compile(b.RECORD,'record','exec')
        self.assertNotIn('mkfs',b.REMOTE_BOOTSTRAP);self.assertNotIn('/job',b.REMOTE_BOOTSTRAP)
        self.assertIn('RequiresMountsFor=',b.REMOTE_BOOTSTRAP)
        self.assertIn('User=stoarama',b.REMOTE_BOOTSTRAP)
        self.assertIn('ReadWritePaths=',b.REMOTE_BOOTSTRAP)
        self.assertIn('PYTHONDONTWRITEBYTECODE=1',b.REMOTE_BOOTSTRAP)

if __name__=='__main__':unittest.main()
