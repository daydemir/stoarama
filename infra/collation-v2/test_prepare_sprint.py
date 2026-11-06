import copy
import importlib.util
import json
import pathlib
import tempfile
import unittest

spec=importlib.util.spec_from_file_location('prepare',pathlib.Path(__file__).with_name('prepare-sprint.py'))
p=importlib.util.module_from_spec(spec);spec.loader.exec_module(p)

class FakeProvider:
    def __init__(self):self.volumes=[];self.cpus=[];self.firewalls=[];self.calls=[];self.lose=None;self.hide=False;self.before_post=None
    def __call__(self,method,path,body=None):
        self.calls.append((method,path,copy.deepcopy(body)))
        if method=='GET':
            if path=='/v2/droplets/612617424':return {'droplet':{'id':p.CONTROLLER,'name':'stoarama-'+p.TAG,'region':{'slug':'nyc1'},'tags':[p.TAG],'vpc_uuid':'vpc-id','networks':{'v4':[{'type':'private','ip_address':'10.10.0.3'}]}}}
            for kind,items,single in [('volumes',self.volumes,'volume'),('droplets',self.cpus,'droplet'),('firewalls',self.firewalls,'firewall')]:
                base='/v2/'+kind
                if path.startswith(base+'?'):return {kind:[] if self.hide else copy.deepcopy(items)}
                if path.startswith(base+'/'):return {single:copy.deepcopy(next(v for v in items if str(v['id'])==path.rsplit('/',1)[1]))}
        if method=='POST':
            if self.before_post:self.before_post(path,body)
            value=copy.deepcopy(body)
            if path=='/v2/volumes':
                value.update(id='00000000-0000-4000-8000-'+str(len(self.volumes)+1).zfill(12),region={'slug':'nyc1'},droplet_ids=[]);self.volumes.append(value);singular='volume'
            elif path=='/v2/droplets':
                value.update(id=100+len(self.cpus),region={'slug':'nyc1'},size_slug='c-32',image={'slug':'ubuntu-24-04-x64'},volume_ids=body['volumes'],status='active',networks={'v4':[{'type':'public','ip_address':'203.0.113.1'}]});self.cpus.append(value);singular='droplet'
                next(v for v in self.volumes if v['id']==body['volumes'][0])['droplet_ids']=[value['id']]
            elif path=='/v2/firewalls':
                value.update(id='firewall-id',status='succeeded',tags=[]);self.firewalls.append(value);singular='firewall'
            else:raise AssertionError(path)
            if self.lose==path:raise RuntimeError('private error')
            return {singular:value}
        raise AssertionError((method,path))

class PrepareTest(unittest.TestCase):
    def setUp(self):
        self.directory=tempfile.TemporaryDirectory();self.addCleanup(self.directory.cleanup);self.root=pathlib.Path(self.directory.name)
        self.api=FakeProvider();self.policy={'schema':'best14-retained-sprint-v1','allow_start':True,'additional_budget_usd':150,'baseline_upper_usd':75,'compute_reserve_usd':15,'retained_volume_reserve_usd':24,'controller_reserve_usd':3,'r2_reserve_usd':5}
        self.commit='a'*40;self.review={'commit':self.commit,'review_passed':True,'merged_default':True};self.ip={'ip':'8.8.4.4','source':'jelly-public-ip','verified_at':1000}
    def run_prepare(self):return p.prepare(self.root,self.policy,self.review,self.commit,self.ip,self.api,now=1000)
    def posts(self):return [c for c in self.api.calls if c[0]=='POST']
    def test_three_cpus_three_fixed_volumes_and_firewall_idempotent(self):
        result=self.run_prepare();self.assertEqual(result['status'],'prepared');self.assertFalse(result['worker_started'])
        self.assertEqual(len(self.posts()),7);self.run_prepare();self.assertEqual(len(self.posts()),7)
        self.assertTrue(all(c[2]['volumes'] for c in self.posts() if c[1]=='/v2/droplets'))
        self.assertTrue(all('volume_ids' not in c[2] for c in self.posts() if c[1]=='/v2/droplets'))
    def test_no_creation_without_start_authority(self):
        self.policy['allow_start']=False
        with self.assertRaises(p.PreparationError):self.run_prepare()
        self.assertEqual(self.api.calls,[])
    def test_budget_reserve_and_review_gates(self):
        for field,value in [('additional_budget_usd',100),('baseline_upper_usd',99),('retained_volume_reserve_usd',23)]:
            with self.subTest(field=field):
                original=self.policy[field];self.policy[field]=value
                with self.assertRaises(p.PreparationError):self.run_prepare()
                self.policy[field]=original
        self.review['merged_default']=False
        with self.assertRaises(p.PreparationError):self.run_prepare()
        self.assertEqual(self.posts(),[])
    def test_creation_intent_fsynced_before_post(self):
        def before(path,spec):
            state=json.loads((self.root/p.RESOURCE_FILE).read_text())
            if path!='/v2/firewalls':self.assertTrue(any(e['spec_sha256']==p.digest(spec) for e in state['resources'].values()))
            else:self.assertEqual(state['firewall']['spec_sha256'],p.digest(spec))
        self.api.before_post=before;self.run_prepare()
    def test_lost_volume_response_reconciles_no_duplicate(self):
        self.api.lose='/v2/volumes';self.assertEqual(self.run_prepare()['status'],'pending')
        self.api.lose=None;self.run_prepare();self.assertEqual(len(self.api.volumes),3)
    def test_lost_cpu_response_with_attached_volume_reconciles(self):
        self.api.lose='/v2/droplets';self.assertEqual(self.run_prepare()['status'],'pending')
        self.api.lose=None;self.assertEqual(self.run_prepare()['status'],'prepared');self.assertEqual(len(self.api.cpus),3)
    def test_no_repeat_when_uncertain_resource_not_yet_visible(self):
        self.api.lose='/v2/volumes';self.run_prepare();self.api.hide=True
        self.assertEqual(self.run_prepare()['status'],'pending');self.assertEqual(len(self.posts()),1)
    def test_foreign_name_not_adopted_without_intent(self):
        self.api.volumes=[dict(p.plan('vpc-id')[0]['volume'],id='foreign')]
        with self.assertRaises(p.PreparationError):self.run_prepare()
        self.assertEqual(self.posts(),[])
    def test_reconciled_resource_wrong_size_retained(self):
        self.api.lose='/v2/volumes';self.run_prepare();self.api.volumes[0]['size_gigabytes']=81
        with self.assertRaises(p.PreparationError):self.run_prepare()
        self.assertEqual(len(self.posts()),1)
    def test_firewall_only_after_cpu_networks_and_exact_sources(self):
        self.run_prepare();firewall=self.posts()[-1][2]
        self.assertEqual(firewall['inbound_rules'][0]['sources']['addresses'],['8.8.4.4/32'])
        self.assertEqual(firewall['inbound_rules'][1]['sources']['addresses'],['10.10.0.3/32'])
        self.assertEqual(firewall['inbound_rules'][1]['ports'],'8080-8101');self.assertNotIn('tags',firewall)
    def test_stale_or_private_jelly_ip_blocked(self):
        self.ip['verified_at']=0
        with self.assertRaises(p.PreparationError):self.run_prepare()
        self.assertEqual(self.posts(),[])
    def test_lost_firewall_response_reconciles_without_duplicate(self):
        self.api.lose='/v2/firewalls';self.assertEqual(self.run_prepare()['status'],'pending')
        self.api.lose=None;self.assertEqual(self.run_prepare()['status'],'prepared')
        self.assertEqual(len(self.api.firewalls),1)
    def test_private_jelly_address_never_creates(self):
        self.ip['ip']='10.0.0.1'
        with self.assertRaises(p.PreparationError):self.run_prepare()
        self.assertEqual(self.api.calls,[])
    def test_existing_attached_volume_drift_blocks_reentry(self):
        self.run_prepare();self.api.volumes[0]['droplet_ids']=[999]
        with self.assertRaises(p.PreparationError):self.run_prepare()
        self.assertEqual(len(self.posts()),7)
    def test_firewall_cannot_gain_public_worker_access(self):
        self.run_prepare();self.api.firewalls[0]['inbound_rules'][1]['sources']['addresses']=['0.0.0.0/0']
        with self.assertRaises(p.PreparationError):self.run_prepare()
    def test_normalized_api_empty_peer_arrays_accepted(self):
        self.run_prepare();self.api.firewalls[0]['inbound_rules'][0]['sources']['tags']=[]
        self.assertEqual(self.run_prepare()['status'],'prepared')
    def test_tracker_refreshes_ready_cpu_networks(self):
        self.run_prepare();self.api.cpus[0]['created_at']='2026-10-26T02:00:00Z'
        self.api.cpus[0]['networks']['v4'].append({'type':'private','ip_address':'10.10.0.4'})
        self.run_prepare();state=json.loads((self.root/p.RESOURCE_FILE).read_text())
        self.assertEqual(state['resources']['cpu-a']['receipt'],self.api.cpus[0])

    def test_no_master_keys_or_worker_start_in_userdata(self):
        for v in p.plan('vpc-id'):
            self.assertEqual(v['droplet']['user_data'],p.USER_DATA)
            self.assertNotIn('runcmd',v['droplet']['user_data']);self.assertNotIn('R2_',v['droplet']['user_data'])

if __name__=='__main__':unittest.main()
