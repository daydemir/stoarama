import copy
import importlib.util
import json
import pathlib
import tempfile
import unittest

spec=importlib.util.spec_from_file_location('resources',pathlib.Path(__file__).with_name('sprint-resources.py'))
r=importlib.util.module_from_spec(spec);spec.loader.exec_module(r)

class Missing(Exception):code=404

class Provider:
    def __init__(self):
        self.host={'id':123,'name':'stoarama-'+r.TAG+'-cpu-1','volume_id':'00000000-0000-4000-8000-000000000001','volume_name':r.TAG+'-disk-1','drain_confirmed':True,'checkpoint_confirmed':True,'proof_at':1000,'drain_receipt':{'active_jobs':0,'pending_exports':0,'synced':True,'all_mutable_on_volume':True},'checkpoint_receipt':{'full_readback_verified':True,'durable':True}}
        self.droplet={'id':123,'name':self.host['name'],'tags':[r.TAG],'region':{'slug':'nyc1'},'status':'active','volume_ids':[self.host['volume_id']]}
        self.volume={'id':self.host['volume_id'],'name':self.host['volume_name'],'tags':[r.TAG],'size_gigabytes':80,'region':{'slug':'nyc1'},'droplet_ids':[123]}
        self.actions={};self.calls=[];self.ambiguous=set();self.hide_action=False;self.on_mutation=None
    def api(self,method,path,payload=None):
        self.calls.append((method,path,copy.deepcopy(payload)))
        vp='/v2/volumes/'+self.host['volume_id'];dp='/v2/droplets/123'
        if method=='GET' and path==vp:return {'volume':copy.deepcopy(self.volume)}
        if method=='GET' and path==dp:
            if self.droplet is None:raise Missing()
            return {'droplet':copy.deepcopy(self.droplet)}
        if '/actions' in path and method=='GET':
            kind='detach' if path.startswith(vp) else 'shutdown'
            if '?' in path:return {'actions':[] if self.hide_action else [copy.deepcopy(a) for a in self.actions.values() if a['_kind']==kind]}
            return {'action':copy.deepcopy(self.actions[int(path.rsplit('/',1)[1])])}
        if method=='POST':
            self.assert_payload(path,payload,dp,vp)
            if self.on_mutation:self.on_mutation(method,path,payload)
            identifier=len(self.actions)+1;kind=payload['type']
            a={'id':identifier,'type':'detach_volume' if kind=='detach' else kind,'status':'in-progress','started_at':'1970-01-01T00:16:40Z','_kind':kind};self.actions[identifier]=a
            if kind in self.ambiguous:raise RuntimeError('secret provider response deliberately hidden')
            return {'action':copy.deepcopy(a)}
        if method=='DELETE':
            assert path==dp and self.volume['droplet_ids']==[] and self.droplet['status']=='off'
            if self.on_mutation:self.on_mutation(method,path,payload)
            self.droplet=None
            if 'delete' in self.ambiguous:raise RuntimeError('secret response')
            return {}
        raise AssertionError('unexpected provider call')
    def assert_payload(self,path,payload,dp,vp):
        assert (path==dp+'/actions' and payload=={'type':'shutdown'}) or (path==vp+'/actions' and payload=={'type':'detach','droplet_id':123,'region':'nyc1'})
    def complete_shutdown(self):
        self.actions[1]['status']='completed';self.droplet['status']='off'
    def complete_detach(self,detach=True):
        self.actions[2]['status']='completed'
        if detach:self.volume['droplet_ids']=[];self.droplet['volume_ids']=[]

class RetirementTest(unittest.TestCase):
    def setUp(self):
        self.directory=tempfile.TemporaryDirectory();self.addCleanup(self.directory.cleanup)
        self.root=pathlib.Path(self.directory.name);self.p=Provider()
    def run_step(self,**kwargs):return r.retire_host(self.p.host,self.root,self.p.api,now=1000,**kwargs)
    def mutations(self):return [c for c in self.p.calls if c[0]!='GET']
    def state(self):return json.loads((self.root/'retire-123.json').read_text())
    def through_detach(self):
        self.run_step();self.p.complete_shutdown();self.run_step();self.p.complete_detach()
    def test_full_graceful_sequence_retains_volume(self):
        self.assertEqual(self.run_step()['status'],'pending')
        self.assertEqual(self.run_step()['reason'],'shutdown_action');self.assertEqual(len(self.mutations()),1)
        self.p.complete_shutdown();self.run_step()
        self.assertEqual(self.run_step()['reason'],'detach_action');self.assertEqual(len(self.mutations()),2)
        self.p.complete_detach();self.run_step()
        self.assertEqual(self.run_step()['status'],'retired')
        self.assertEqual(self.run_step()['status'],'retired')
        self.assertEqual([m[0] for m in self.mutations()],['POST','POST','DELETE'])
        self.assertIsNotNone(self.p.volume);self.assertEqual(self.p.volume['droplet_ids'],[])
    def test_durable_intents_precede_each_mutation(self):
        def validate(method,path,payload):
            s=self.state()
            if method=='DELETE':self.assertTrue(s['delete_intent']);self.assertTrue(s['detach_confirmed'])
            else:self.assertIn(payload['type']+'_action',s)
        self.p.on_mutation=validate
        self.through_detach();self.run_step()
    def test_ambiguous_shutdown_reconciles_without_duplicate(self):
        self.p.ambiguous.add('shutdown');self.run_step()
        self.p.hide_action=True
        for _ in range(3):self.assertEqual(self.run_step()['status'],'pending')
        self.assertEqual(len(self.mutations()),1)
        self.p.hide_action=False;self.p.complete_shutdown();self.run_step()
        self.assertEqual([m[2]['type'] for m in self.mutations()],['shutdown','detach'])
    def test_ambiguous_detach_reconciles_without_duplicate(self):
        self.run_step();self.p.complete_shutdown();self.p.ambiguous.add('detach');self.run_step()
        self.p.complete_detach();self.run_step();self.assertEqual(self.run_step()['status'],'retired')
        self.assertEqual(len(self.mutations()),3)
    def test_ambiguous_delete_confirmed_by_absence_and_retained_volume(self):
        self.through_detach();self.p.ambiguous.add('delete');self.run_step()
        self.assertEqual(self.run_step()['status'],'retired');self.assertEqual(len(self.mutations()),3)
    def retain_after_delete_timeout(self):
        self.through_detach()
        original=self.p.api
        def ambiguous(method,path,payload=None):
            if method=='DELETE':
                self.p.calls.append((method,path,payload))
                raise RuntimeError('response lost before provider applied deletion')
            return original(method,path,payload)
        self.assertEqual(r.retire_host(self.p.host,self.root,ambiguous,now=1000)['reason'],'delete_response_unknown')

    def test_lost_unapplied_delete_retries_same_id_after_backoff(self):
        self.retain_after_delete_timeout()
        self.assertEqual(self.state()['delete_intent']['attempts'],1)
        r.retire_host(self.p.host,self.root,self.p.api,now=1030)
        deletes=[m for m in self.mutations() if m[0]=='DELETE']
        self.assertEqual([m[1] for m in deletes],['/v2/droplets/123','/v2/droplets/123'])
        self.assertEqual(self.state()['delete_intent']['attempts'],2)
        self.assertEqual(r.retire_host(self.p.host,self.root,self.p.api,now=1031)['status'],'retired')

    def test_no_early_delete_retry(self):
        self.retain_after_delete_timeout()
        r.retire_host(self.p.host,self.root,self.p.api,now=1029)
        self.assertEqual(len([m for m in self.mutations() if m[0]=='DELETE']),1)

    def test_changed_ownership_blocks_delete_retry(self):
        self.retain_after_delete_timeout();self.p.droplet['tags']=[]
        with self.assertRaises(r.RetirementError):r.retire_host(self.p.host,self.root,self.p.api,now=1030)
        self.assertEqual(len([m for m in self.mutations() if m[0]=='DELETE']),1)

    def test_reattached_volume_blocks_delete_retry(self):
        self.retain_after_delete_timeout();self.p.volume['droplet_ids']=[123];self.p.droplet['volume_ids']=[self.p.host['volume_id']]
        self.assertEqual(r.retire_host(self.p.host,self.root,self.p.api,now=1030)['reason'],'volume_still_attached')
        self.assertEqual(len([m for m in self.mutations() if m[0]=='DELETE']),1)

    def test_powered_on_cpu_blocks_delete_retry(self):
        self.retain_after_delete_timeout();self.p.droplet['status']='active'
        self.assertEqual(r.retire_host(self.p.host,self.root,self.p.api,now=1030)['reason'],'graceful_shutdown')
        self.assertEqual(len([m for m in self.mutations() if m[0]=='DELETE']),1)

    def test_completed_detach_still_attached_prevents_delete(self):
        self.run_step();self.p.complete_shutdown();self.run_step();self.p.complete_detach(detach=False)
        self.assertEqual(self.run_step()['reason'],'volume_still_attached')
        self.assertFalse(any(m[0]=='DELETE' for m in self.mutations()))
    def test_missing_droplet_without_retirement_evidence_refused(self):
        self.p.droplet=None;self.p.volume['droplet_ids']=[]
        with self.assertRaises(r.RetirementError):self.run_step()
        self.assertEqual(self.mutations(),[])
    def test_foreign_attachment_refused_even_after_delete(self):
        self.through_detach();self.run_step();self.p.volume['droplet_ids']=[999]
        with self.assertRaises(r.RetirementError):self.run_step()
    def test_ownership_and_region_size_mismatch_refused_before_mutation(self):
        for field,value in [('id','foreign'),('size_gigabytes',81),('tags',[]),('region',{'slug':'nyc3'}),('name','other')]:
            with self.subTest(field=field):
                self.p=Provider();self.p.volume[field]=value
                with self.assertRaises(r.RetirementError):self.run_step()
                self.assertEqual(self.mutations(),[])
    def test_droplet_ownership_mismatch_refused(self):
        self.p.droplet['tags']=[]
        with self.assertRaises(r.RetirementError):self.run_step()
        self.assertEqual(self.mutations(),[])
    def test_proofs_must_be_fresh_durable_flushed_and_idle(self):
        cases=[('drain_confirmed',False),('checkpoint_confirmed',False),('proof_at',0),('drain_receipt',{'active_jobs':1}),('checkpoint_receipt',{'full_readback_verified':False,'durable':True})]
        for key,value in cases:
            with self.subTest(key=key):
                self.p=Provider();self.p.host[key]=value
                with self.assertRaises(r.RetirementError):self.run_step()
                self.assertEqual(self.mutations(),[])
    def test_durable_recorder_failure_prevents_post(self):
        def fail(*args):raise OSError('disk full')
        with self.assertRaises(OSError):self.run_step(record=fail)
        self.assertEqual(self.mutations(),[])
    def test_action_id_is_persisted_before_poll(self):
        self.run_step();self.assertEqual(self.state()['shutdown_action']['id'],1)
        self.assertFalse(any('/actions/1' in c[1] for c in self.p.calls))
    def test_completed_shutdown_requires_provider_off_before_detach(self):
        self.run_step();self.p.actions[1]['status']='completed'
        self.assertEqual(self.run_step()['reason'],'graceful_shutdown')
        self.assertEqual(len(self.mutations()),1)
    def test_changed_action_identity_refuses_detach(self):
        self.run_step();self.p.actions[1]['id']=99;self.p.complete_shutdown()
        with self.assertRaises(r.RetirementError):self.run_step()
        self.assertEqual(len(self.mutations()),1)
    def test_same_resource_action_candidates_choose_latest_without_resend(self):
        self.p.ambiguous.add('shutdown');self.run_step()
        self.p.actions[9]=dict(self.p.actions[1],id=9)
        self.assertEqual(self.run_step()['status'],'pending')
        self.assertEqual(self.state()['shutdown_action']['id'],9)
        self.assertEqual(len(self.mutations()),1)
    def test_errored_action_keeps_cpu_and_volume(self):
        self.run_step();self.p.actions[1]['status']='errored'
        with self.assertRaises(r.RetirementError):self.run_step()
        self.assertEqual(len(self.mutations()),1);self.assertIsNotNone(self.p.droplet)
    def test_unexpected_second_attached_volume_refuses_shutdown(self):
        self.p.droplet['volume_ids'].append('foreign')
        with self.assertRaises(r.RetirementError):self.run_step()
        self.assertEqual(self.mutations(),[])
    def test_no_credential_extras_copied_into_proof_record(self):
        self.p.host['drain_receipt']['secret']='never-copy'
        self.run_step()
        self.assertNotIn('secret',self.state()['proof']['drain'])
    def rejected_submission(self,kind):
        if kind=='detach':self.run_step();self.p.complete_shutdown()
        original=self.p.api
        def reject(method,path,payload=None):
            if method=='POST' and payload.get('type')==kind:
                self.p.calls.append((method,path,payload));raise RuntimeError('not accepted')
            return original(method,path,payload)
        self.assertEqual(r.retire_host(self.p.host,self.root,reject,now=1000)['status'],'pending')

    def test_unaccepted_shutdown_retry_after_thirty_seconds_then_retire(self):
        self.rejected_submission('shutdown')
        self.assertEqual(r.retire_host(self.p.host,self.root,self.p.api,now=1029)['reason'],'shutdown_action')
        self.assertEqual(len(self.mutations()),1)
        r.retire_host(self.p.host,self.root,self.p.api,now=1030)
        self.assertEqual(self.state()['shutdown_action']['attempts'],2)
        self.p.complete_shutdown();r.retire_host(self.p.host,self.root,self.p.api,now=1031)
        self.p.complete_detach();r.retire_host(self.p.host,self.root,self.p.api,now=1032)
        self.assertEqual(r.retire_host(self.p.host,self.root,self.p.api,now=1033)['status'],'retired')
        self.assertEqual([m[2] for m in self.mutations()[:2]],[{'type':'shutdown'},{'type':'shutdown'}])

    def test_unaccepted_detach_retry_after_thirty_seconds_then_retire(self):
        self.rejected_submission('detach')
        r.retire_host(self.p.host,self.root,self.p.api,now=1029);self.assertEqual(len(self.mutations()),2)
        r.retire_host(self.p.host,self.root,self.p.api,now=1030)
        self.assertEqual(self.state()['detach_action']['attempts'],2)
        self.p.complete_detach();r.retire_host(self.p.host,self.root,self.p.api,now=1031)
        self.assertEqual(r.retire_host(self.p.host,self.root,self.p.api,now=1032)['status'],'retired')

    def test_changed_identity_blocks_uncertain_shutdown_resend(self):
        self.rejected_submission('shutdown');self.p.droplet['tags']=[]
        with self.assertRaises(r.RetirementError):r.retire_host(self.p.host,self.root,self.p.api,now=1030)
        self.assertEqual(len(self.mutations()),1)

    def test_powered_on_host_blocks_uncertain_detach_resend(self):
        self.rejected_submission('detach');self.p.droplet['status']='active'
        self.assertEqual(r.retire_host(self.p.host,self.root,self.p.api,now=1030)['reason'],'graceful_shutdown')
        self.assertEqual(len(self.mutations()),2)

    def test_completed_late_action_preferred_over_newer_pending(self):
        self.p.ambiguous.add('shutdown');self.run_step();self.p.complete_shutdown()
        self.p.actions[9]=dict(self.p.actions[1],id=9,status='in-progress')
        self.run_step();self.assertEqual(self.state()['shutdown_action']['id'],1)
        self.assertEqual(self.mutations()[-1][2]['type'],'detach')

    def test_foreign_resource_action_blocks_reconciliation(self):
        self.p.ambiguous.add('shutdown');self.run_step();self.p.actions[1]['resource_id']=999
        with self.assertRaises(r.RetirementError):self.run_step()
        self.assertEqual(len(self.mutations()),1)

    def test_plain_off_is_not_graceful_action_evidence(self):
        self.p.droplet['status']='off';self.run_step()
        self.assertEqual(self.mutations()[0][2],{'type':'shutdown'})
    def test_changed_durable_identity_refused(self):
        self.run_step();self.p.host['name']='stoarama-'+r.TAG+'-different'
        with self.assertRaises(r.RetirementError):self.run_step()
    def test_provider_errors_are_sanitized(self):
        def bad(*args):raise RuntimeError('credential contents')
        with self.assertRaisesRegex(r.ProviderUnavailable,'^provider request unavailable$'):r.retire_host(self.p.host,self.root,bad,now=1000)

if __name__=='__main__':unittest.main()
