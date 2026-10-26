import contextlib,hashlib,importlib.util,io,json,pathlib,sqlite3,tarfile,tempfile,time,unittest,urllib.error
from unittest import mock
HERE=pathlib.Path(__file__).resolve().parent
spec=importlib.util.spec_from_file_location('sprint',HERE/'sprint-control.py');s=importlib.util.module_from_spec(spec);spec.loader.exec_module(s)
class Tests(unittest.TestCase):
 def setUp(self):
  s.REQUEST_DEADLINE=None
  self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup);self.root=pathlib.Path(self.temp.name)
  self.db=sqlite3.connect(':memory:');self.addCleanup(self.db.close);self.db.row_factory=sqlite3.Row
  self.db.executescript('CREATE TABLE jobs(id TEXT PRIMARY KEY,state TEXT,worker_id TEXT,source_bytes INTEGER,attempt INTEGER,result TEXT,path TEXT,source_files INTEGER);CREATE TABLE export_attempts(id TEXT PRIMARY KEY,count INTEGER);CREATE TABLE deferred_jobs(id TEXT PRIMARY KEY,until REAL);')
  self.owner=s.Owner.__new__(s.Owner);o=self.owner;o.root=self.root;o.policy={'baseline_upper_usd':75,'compute_reserve_usd':15,'retained_volume_reserve_usd':24,'controller_reserve_usd':3,'r2_reserve_usd':5,'baseline_export_exposure_usd':0,'additional_budget_usd':150,'deadline_utc':'2026-10-26T05:55:00+00:00'}
  o.deadline=time.time()+600;o.last_checkpoint=time.monotonic();o.stopping=False
  self.w={'id':'h1-00','host_id':1,'host':'10.116.0.2','port':8080,'volume_identity':['1',2,3],'root_device':2}
  o.workers=[self.w];o.byid={self.w['id']:self.w};o.hosts=[{'id':1}];o.byhost={'1':['h1-00','h1-01']}
  self.ready={'drained':True,'active_jobs':0,'build_complete':True,'volume_identity':self.w['volume_identity'],'root_device':2,'free_bytes':20*s.GIB}
 def job(self,name='a',state='queued',worker=None,size=500000000,attempt=0,result='{}'):
  self.db.execute('INSERT INTO jobs VALUES(?,?,?,?,?,?,?,?)',(name,state,worker,size,attempt,result,'unused',1));self.db.commit()
  return self.db.execute('SELECT * FROM jobs WHERE id=?',(name,)).fetchone()
 def test_uncertain_jobs_reserve_shared_host_space(self):
  self.job('a','submitted','h1-01',2*s.GIB,1);self.job('foreign','submitted','other',3*s.GIB,1)
  self.assertEqual(s.reserved_bytes(self.db,['h1-00','h1-01']),6*s.GIB+128*1024**2)
 def test_same_host_reservation_can_block_new_job(self):
  self.job('a','submitted','h1-01',3*s.GIB,1)
  row=self.job('b',size=2*s.GIB);self.ready['free_bytes']=19*s.GIB
  self.assertFalse(self.owner.can_admit(self.db,self.w,row,self.ready,{'upper_bound_usd':122}))
 def test_other_host_does_not_consume_this_reservation(self):
  self.job('a','submitted','other',3*s.GIB,1);row=self.job('b')
  self.assertTrue(self.owner.can_admit(self.db,self.w,row,self.ready,{'upper_bound_usd':122}))
 def test_volume_change_fail_closed(self):
  row=self.job();self.ready['root_device']=4
  with self.assertRaisesRegex(ValueError,'volume_differs'):self.owner.can_admit(self.db,self.w,row,self.ready,{'upper_bound_usd':122})
 def test_draining_slot_never_admitted(self):
  row=self.job();self.ready['draining']=True
  self.assertFalse(self.owner.can_admit(self.db,self.w,row,self.ready,{'upper_bound_usd':122}))
 def test_budget_includes_export_and_recovery_before_submit(self):
  row=self.job(size=2*s.GIB)
  self.assertFalse(self.owner.can_admit(self.db,self.w,row,self.ready,{'upper_bound_usd':144.9}))
 def test_exposure_counts_repeated_exports(self):
  result=json.dumps({'export_bytes':s.GIB})
  self.job('a','staged_verified','h1-00',s.GIB,1,result);self.db.execute('INSERT INTO export_attempts VALUES(?,?)',('a',3));self.db.commit()
  self.assertAlmostEqual(s.exposure(self.db),.03)
 def test_pending_export_reserves_worst_attempts(self):
  self.job('a','submitted','h1-00',s.GIB,1)
  self.assertAlmostEqual(s.exposure(self.db),.06375)
 def test_failed_media_without_export_has_zero_exposure(self):
  self.job('a','held','h1-00',s.GIB,1)
  self.assertEqual(s.exposure(self.db),0)
 def test_reserves_are_prepaid_even_before_first_job(self):
  b=self.owner.budget(self.db);self.assertEqual(b['upper_bound_usd'],122);self.assertFalse(b['actual_bill'])
 def test_carry_baseline_never_refunded_on_new_run(self):
  self.owner.policy['baseline_export_exposure_usd']=20
  self.assertEqual(self.owner.budget(self.db)['upper_bound_usd'],122)
 def test_public_ip_refused(self):
  with self.assertRaises(ValueError):s.allowed_network(dict(self.w,host='8.8.8.8'))
 def test_port_escape_refused(self):
  with self.assertRaises(ValueError):s.allowed_network(dict(self.w,port=22))
 def test_slot_id_path_escape_refused(self):
  with self.assertRaises(ValueError):s.allowed_network(dict(self.w,id='../x'))
 def test_redirect_handler_never_forwards(self):
  self.assertIsNone(s.NoRedirect().redirect_request(None,None,None,None,None,None))
 def test_network_error_sanitized(self):
  with mock.patch.object(s.OPENER,'open',side_effect=RuntimeError('https://signed.invalid/?secret=bad')):
   with self.assertRaisesRegex(RuntimeError,'^worker_transport_unconfirmed$'):s.guest(self.w,'/ready')
 def test_arbitrary_route_refused(self):
  with self.assertRaises(ValueError):s.guest(self.w,'/job?id='+('a'*64)+'&escape=yes')
 def test_expired_checkpoint_failure_blocks_all_dispatch(self):
  self.job();self.owner.last_checkpoint=None
  with mock.patch.object(self.owner,'checkpoint',side_effect=RuntimeError('bad')),mock.patch.object(s.finite,'submit') as submit:
   with self.assertRaises(RuntimeError):self.owner.pass_once(self.db)
   submit.assert_not_called()
 def test_budget_exhaustion_stops_without_guest_requests(self):
  self.owner.policy['additional_budget_usd']=100
  with mock.patch.object(s,'guest') as guest:self.owner.pass_once(self.db);guest.assert_not_called()
  self.assertTrue(self.owner.stopping)
 def test_deadline_stops_without_guest_requests(self):
  self.owner.deadline=time.time()-1
  with mock.patch.object(s,'guest') as guest:self.owner.pass_once(self.db);guest.assert_not_called()
  self.assertTrue(self.owner.stopping)
 def test_drain_transport_failure_retains_not_ready_to_retire(self):
  with mock.patch.object(s,'guest',side_effect=RuntimeError('unconfirmed')),mock.patch.object(self.owner,'checkpoint') as checkpoint:
   self.assertFalse(self.owner.drain(self.db));checkpoint.assert_not_called()
  self.assertTrue((self.root/'sprint-stop.json').exists())
 def test_drain_pending_worker_not_retired(self):
  with mock.patch.object(s,'guest',return_value={'drained':False}),mock.patch.object(self.owner,'checkpoint') as checkpoint:
   self.assertFalse(self.owner.drain(self.db));checkpoint.assert_not_called()
 def test_drained_checkpoint_precedes_retirement_readiness(self):
  with mock.patch.object(s,'guest',return_value=self.ready),mock.patch.object(self.owner,'checkpoint') as checkpoint:
   self.assertTrue(self.owner.drain(self.db));checkpoint.assert_called_once()
 def test_checkpoint_only_allowlisted_evidence_never_environment(self):
  (self.root/'controller.env').write_text('NOT-TO-ARCHIVE');(self.root/'request.json').write_text('NOT-TO-ARCHIVE')
  (self.root/'sprint-policy.json').write_text('{}');captured={}
  def opened(request,timeout=None):
   if hasattr(request,'data') and request.data is not None:captured['body']=request.data;return contextlib.closing(io.BytesIO(b''))
   return contextlib.closing(io.BytesIO(captured['body']))
  with mock.patch.dict(s.os.environ,{'R2_BUCKET':'test'}),mock.patch.object(s.finite,'presign',return_value={'url':'https://test.invalid','headers':{}}),mock.patch.object(s.urllib.request,'urlopen') as openmock:
   class Response(io.BytesIO):
    status=200
   def req(request,timeout=None):
    if isinstance(request,s.urllib.request.Request) and request.data is not None:captured['body']=request.data;return Response(b'')
    return Response(captured['body'])
   openmock.side_effect=req;receipt=self.owner.checkpoint(self.db)
  with tarfile.open(fileobj=io.BytesIO(captured['body']),mode='r:gz') as tar:names=tar.getnames()
  self.assertEqual(set(names),{'execution.sqlite3','sprint-policy.json'});self.assertTrue(receipt['full_readback_verified'])
 def test_checkpoint_readback_mismatch_has_no_verified_receipt(self):
  class Response(io.BytesIO):status=200
  with mock.patch.dict(s.os.environ,{'R2_BUCKET':'test'}),mock.patch.object(s.finite,'presign',return_value={'url':'https://test.invalid','headers':{}}),mock.patch.object(s.urllib.request,'urlopen',side_effect=[Response(b''),Response(b'corrupt')]):
   with self.assertRaisesRegex(ValueError,'readback_differs'):self.owner.checkpoint(self.db)
  self.assertFalse((self.root/'sprint-checkpoint-receipt.json').exists())

 def test_restart_retirement_phase_does_not_require_http_workers(self):
  (self.root/'sprint-drain-proof.json').write_text('{}')
  (self.root/'retire-1.json').write_text('{"phase":"retired"}')
  self.assertTrue(self.owner.retirement_started())
 def test_drain_proof_without_retirement_not_a_retirement_resume(self):
  (self.root/'sprint-drain-proof.json').write_text('{}')
  self.assertFalse(self.owner.retirement_started())
 def test_source_heads_after_deadline_never_touch_network(self):
  self.owner.deadline=time.time()-1
  with mock.patch.object(s.OPENER,'open') as request:self.assertIsNone(self.owner.bounded_head({}));request.assert_not_called()
 def test_source_head_timeout_respects_remaining_deadline(self):
  self.owner.deadline=time.time()+2
  with mock.patch.object(s.finite,'presign',return_value={'url':'https://test.invalid'}),mock.patch.object(s.OPENER,'open',side_effect=OSError) as request:
   self.assertIsNone(self.owner.bounded_head({'bucket':'x','object_key':'k','size_bytes':1}))
   self.assertLessEqual(request.call_args.kwargs['timeout'],2)
 def test_stopping_never_resubmits_missing_old_intent(self):
  self.owner.stopping=True
  with self.assertRaises(s.finite.WorkerBusy):self.owner.bounded_submit()
 def test_guest_after_phase_deadline_makes_no_request(self):
  s.REQUEST_DEADLINE=time.time()-1
  with mock.patch.object(s.OPENER,'open') as request:
   with self.assertRaisesRegex(RuntimeError,'worker_phase_deadline'):s.guest(self.w,'/ready')
   request.assert_not_called()

 def test_active_slot_deadline_checked_before_reconcile(self):
  self.job('a','submitted','h1-00',500,1)
  self.owner.deadline=time.time()+100
  def times():return self.owner.deadline+1
  with mock.patch.object(self.owner,'budget',return_value={'upper_bound_usd':122}),mock.patch.object(s.time,'time',side_effect=[self.owner.deadline-1,self.owner.deadline+1]),mock.patch.object(s.finite,'reconcile') as reconcile:
   self.owner.pass_once(self.db);reconcile.assert_not_called()
  self.assertTrue(self.owner.stopping)
 def test_pending_export_is_reconciled_during_grace_not_retired(self):
  self.job('a','submitted','h1-00',500,1)
  with mock.patch.object(s,'guest',return_value=self.ready),mock.patch.object(s.finite,'reconcile') as reconcile,mock.patch.object(self.owner,'checkpoint') as checkpoint:
   self.assertFalse(self.owner.drain(self.db));reconcile.assert_called_once();checkpoint.assert_not_called()
 def test_after_grace_only_confirmed_drained_work_becomes_recovery_hold(self):
  self.job('a','submitted','h1-00',500,1);self.owner.deadline=time.time()-46*60
  with mock.patch.object(s,'guest',return_value=self.ready),mock.patch.object(self.owner,'checkpoint'):
   self.assertTrue(self.owner.drain(self.db))
  self.assertEqual(self.db.execute('SELECT state FROM jobs').fetchone()[0],'held_shutdown')
 def test_after_grace_unreachable_worker_keeps_inflight_intent(self):
  self.job('a','submitted','h1-00',500,1);self.owner.deadline=time.time()-46*60
  with mock.patch.object(s,'guest',side_effect=RuntimeError('unknown')),mock.patch.object(self.owner,'checkpoint') as checkpoint:
   self.assertFalse(self.owner.drain(self.db));checkpoint.assert_not_called()
  self.assertEqual(self.db.execute('SELECT state FROM jobs').fetchone()[0],'submitted')

 def test_elapsed_compute_overrun_is_not_hidden_by_fixed_reservation(self):
  self.owner.hosts=[{'id':1,'created_at_epoch':time.time()-20*3600,'hourly_usd':1}]
  b=self.owner.budget(self.db)
  self.assertGreaterEqual(b['reserved_sprint_compute_usd'],20)
  self.assertGreaterEqual(b['upper_bound_usd'],127)
 def test_retired_compute_stops_accruing_at_confirmed_deletion(self):
  at=time.time()
  self.owner.hosts=[{'id':1,'created_at_epoch':at-20*3600,'hourly_usd':1}]
  (self.root/'retire-1.json').write_text(json.dumps({'phase':'retired','retired_at':at-10*3600}))
  b=self.owner.budget(self.db);self.assertEqual(b['reserved_sprint_compute_usd'],15)
 def test_real_run_resumes_retirement_without_http_dead_hosts(self):
  self.job('a','held',None,500,1)
  with sqlite3.connect(self.root/'execution.sqlite3') as disk:self.db.backup(disk)
  (self.root/'sprint-stop.json').write_text('{}')
  (self.root/'sprint-drain-proof.json').write_text('{}')
  (self.root/'retire-1.json').write_text('{"phase":"retired"}')
  old_submit=s.finite.submit;old_head=s.finite.head;old_guest=s.finite.guest
  self.addCleanup(setattr,s.finite,'submit',old_submit);self.addCleanup(setattr,s.finite,'head',old_head);self.addCleanup(setattr,s.finite,'guest',old_guest)
  with mock.patch.object(self.owner,'validate'),mock.patch.object(self.owner,'drain') as drain,mock.patch.object(self.owner,'checkpoint'),mock.patch.object(self.owner,'retire') as retire:
   self.owner.run();drain.assert_not_called();retire.assert_called_once()
  self.assertTrue((self.root/'results').is_dir())
  self.assertEqual((self.root/'results').stat().st_mode&0o777,0o700)

 def test_existing_frozen_executor_lock_fences_sprint_owner(self):
  with (self.root/'executor.lock').open('a+') as holder:
   s.fcntl.flock(holder,s.fcntl.LOCK_EX|s.fcntl.LOCK_NB)
   with mock.patch.object(self.owner,'validate') as validate:
    self.owner.run();validate.assert_not_called()
if __name__=='__main__':unittest.main()
