#!/usr/bin/env python3
"""One finite retained-volume sprint owner. Stages only; no NAS/public changes."""
import argparse,collections,contextlib,datetime,fcntl,hashlib,importlib.util,io,ipaddress,json,os,pathlib,sqlite3,tarfile,tempfile,time,urllib.request,urllib.error,urllib.parse
HERE=pathlib.Path(__file__).resolve().parent
def module(name,file):
 s=importlib.util.spec_from_file_location(name,HERE/file);m=importlib.util.module_from_spec(s);s.loader.exec_module(m);return m
finite=module('finite','finite-staging.py')
GIB=1024**3
class NoRedirect(urllib.request.HTTPRedirectHandler):
 def redirect_request(self,*args,**kwargs):return None
OPENER=urllib.request.build_opener(NoRedirect)
REQUEST_DEADLINE=None
def stamp():return datetime.datetime.now(datetime.timezone.utc).isoformat()
def atomic(path,value):
 p=path.with_suffix(path.suffix+'.pending')
 with p.open('w') as f:json.dump(value,f,sort_keys=True);f.flush();os.fsync(f.fileno())
 p.chmod(0o600);p.replace(path)
 fd=os.open(path.parent,os.O_RDONLY|os.O_DIRECTORY)
 try:os.fsync(fd)
 finally:os.close(fd)
def exposure(db):
 total=0.
 for row in db.execute("SELECT j.state,j.source_bytes,j.result,coalesce(e.count,0) exports FROM jobs j LEFT JOIN export_attempts e ON e.id=j.id WHERE j.attempt>0"):
  state,size,result,exports=row
  if state=='staged_verified':
   result=json.loads(result);exported=result.get('export_bytes')
   if exported is None:exported=sum(x['bytes'] for x in result['export_receipts'])
   total+=exported/GIB*.01*exports
  elif state not in ('held','held_structure'):total+=(6*size+384*1024**2)/GIB*.01
 return total
def reserved_bytes(db,ids):
 if not ids:return 0
 marks=','.join('?' for _ in ids)
 return db.execute("SELECT coalesce(sum(3*source_bytes+134217728),0) FROM jobs WHERE state='submitted' AND worker_id IN ("+marks+")",ids).fetchone()[0]
def allowed_network(worker):
 ip=ipaddress.ip_address(worker['host'])
 if ip.version!=4 or not ip.is_private or not 8080<=worker['port']<=8101:raise ValueError('private_worker_route_refused')
 if len(worker['id'])>80 or any(c not in 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_' for c in worker['id']):raise ValueError('worker_identity_refused')
def guest(worker,route,payload=None):
 allowed_network(worker)
 remaining=REQUEST_DEADLINE-time.time() if REQUEST_DEADLINE is not None else 20
 if remaining<=0:raise RuntimeError('worker_phase_deadline')
 if route not in ('/ready','/job','/export','/release','/drain'):
  parsed=urllib.parse.urlsplit(route);q=urllib.parse.parse_qs(parsed.query)
  if parsed.path!='/job' or set(q)!={'id'} or len(q['id'])!=1 or len(q['id'][0])!=64 or any(c not in '0123456789abcdef' for c in q['id'][0]):raise ValueError('worker_route_refused')
 request=urllib.request.Request('http://'+worker['host']+':'+str(worker['port'])+route,data=json.dumps(payload).encode() if payload is not None else None,headers={'Content-Type':'application/json'},method='POST' if payload is not None else 'GET')
 try:
  with OPENER.open(request,timeout=min(20,max(.1,remaining))) as response:raw=response.read(16*1024**2+1)
 except urllib.error.HTTPError as e:raise finite.GuestStatus(e.code) from None
 except Exception:raise RuntimeError('worker_transport_unconfirmed') from None
 if len(raw)>16*1024**2:raise ValueError('worker_response_too_large')
 return json.loads(raw)
class Owner:
 def __init__(self,root):
  self.root=pathlib.Path(root);self.policy=json.loads((self.root/'sprint-policy.json').read_text());self.roster=json.loads((self.root/'sprint-roster.json').read_text())
  self.workers=self.roster['workers'];self.hosts=self.roster['hosts'];self.byid={w['id']:w for w in self.workers}
  self.byhost={str(h['id']):[w['id'] for w in self.workers if str(w['host_id'])==str(h['id'])] for h in self.hosts}
  self.deadline=datetime.datetime.fromisoformat(self.policy['deadline_utc']).timestamp()
  self.native_submit=finite.submit
  self.last_checkpoint=None;self.stopping=False

 def bounded_head(self,clip):
  remaining=self.deadline-time.time()
  if self.stopping or remaining<=0:return None
  cap=finite.presign('HEAD',clip['bucket'],clip['object_key'])
  try:
   with OPENER.open(urllib.request.Request(cap['url'],method='HEAD'),timeout=min(15,max(.1,remaining))) as response:
    if int(response.headers.get('Content-Length','-1'))!=clip['size_bytes']:return None
    return response.headers.get('ETag') or None
  except Exception:return None
 def bounded_submit(self,*args,**kwargs):
  if self.stopping or time.time()>=self.deadline:raise finite.WorkerBusy('sprint_admission_closed')
  return self.native_submit(*args,**kwargs)
 def retirement_started(self):
  return (self.root/'sprint-drain-proof.json').is_file() and any(self.root.glob('retire-*.json'))

 def validate(self,db):
  p=self.policy
  if p.get('schema')!='best14-retained-sprint-v1' or p.get('allow_start') is not True:raise ValueError('sprint_start_not_authorized')
  if p['additional_budget_usd'] not in (100,150) or p['baseline_upper_usd']<75:raise ValueError('sprint_budget_authority_differs')
  if p['selection_sha256']!=finite.SELECTION or len(self.workers)>66 or len(self.byid)!=len(self.workers):raise ValueError('frozen_scope_differs')
  if len(self.hosts)>3 or any(h['volume_gib']!=80 or h['hourly_usd']>1.01 or h['size']!='c-32' or not 0<h.get('created_at_epoch',0)<=time.time() for h in self.hosts):raise ValueError('sprint_resource_scope_differs')
  if p['compute_reserve_usd']<15 or p['retained_volume_reserve_usd']<8*len(self.hosts) or p['controller_reserve_usd']<3 or p['r2_reserve_usd']<5:raise ValueError('sprint_reserves_incomplete')
  if self.deadline>time.time()+4*3600 or p['shutdown_reserve_seconds']<3600:raise ValueError('sprint_deadline_differs')
  if db.execute('select count(*) from jobs').fetchone()[0]!=4342 or db.execute('pragma quick_check').fetchone()[0]!='ok':raise ValueError('queue_integrity_differs')
  if hashlib.sha256((self.root/'audit/selection-final-v2.json').read_bytes()).hexdigest()!=finite.SELECTION:raise ValueError('selection_bytes_differ')
  for w in self.workers:allowed_network(w)
  for h in self.hosts:
   if not h['name'].startswith('stoarama-best14-sprint-20261026-') or not h['volume_name'].startswith('best14-sprint-20261026-') or len(self.byhost[str(h['id'])])>22:raise ValueError('host_scope_differs')
  for row in db.execute("SELECT id,worker_id,path FROM jobs"):
   if row['worker_id'] and row['worker_id'] not in self.byid and db.execute("SELECT state FROM jobs WHERE id=?",(row['id'],)).fetchone()[0]=='submitted':raise ValueError('foreign_inflight_job_preserved')
   finite.read_work(row)
 def budget(self,db):
  p=self.policy;delta=max(0,exposure(db)-p['baseline_export_exposure_usd'])
  elapsed=0.
  for host in self.hosts:
   start=host.get('created_at_epoch')
   if start is None:continue
   end=time.time();path=self.root/('retire-'+str(host['id'])+'.json')
   if path.is_file():
    value=json.loads(path.read_text())
    if value.get('phase')=='retired':end=value['retired_at']
   elapsed+=max(0,end-start)/3600*host['hourly_usd']
  compute=max(p['compute_reserve_usd'],elapsed)
  total=p['baseline_upper_usd']+compute+p['retained_volume_reserve_usd']+p['controller_reserve_usd']+p['r2_reserve_usd']+delta
  result={'observed_at':stamp(),'additional_limit_usd':p['additional_budget_usd'],'baseline_upper_usd':p['baseline_upper_usd'],'reserved_sprint_compute_usd':compute,'observed_sprint_compute_upper_usd':elapsed,'reserved_retained_volumes_usd':p['retained_volume_reserve_usd'],'reserved_controller_usd':p['controller_reserve_usd'],'reserved_extra_r2_usd':p['r2_reserve_usd'],'incremental_export_exposure_usd':delta,'upper_bound_usd':total,'recovery_reserve_usd':5,'actual_bill':False}
  atomic(self.root/'sprint-budget.json',result);return result
 def checkpoint(self,db):
  names=['sprint-policy.json','sprint-roster.json','sprint-budget.json','sprint-status.json','sprint-prior-holds.json','sprint-source-receipt.json','sprint-stop.json','sprint-drain-proof.json','sprint-retirement-status.json','sprint-owner-error.json']
  with tempfile.TemporaryDirectory(dir=self.root,prefix='sprint-checkpoint-') as temp:
   temp=pathlib.Path(temp)
   with contextlib.closing(sqlite3.connect(temp/'execution.sqlite3')) as dest:db.backup(dest)
   out=io.BytesIO()
   with tarfile.open(fileobj=out,mode='w:gz') as tar:
    tar.add(temp/'execution.sqlite3',arcname='execution.sqlite3',recursive=False)
    for name in names:
     path=self.root/name
     if path.is_file() and not path.is_symlink():tar.add(path,arcname=name,recursive=False)
    for path in sorted(self.root.glob('retire-*.json')):
     if path.is_file() and not path.is_symlink():tar.add(path,arcname=path.name,recursive=False)
   body=out.getvalue()
  if len(body)>64*1024**2:raise ValueError('sprint_checkpoint_capacity')
  digest=hashlib.sha256(body).hexdigest();bucket=os.environ['R2_BUCKET'];key='staging/best14-20261021/sprint-20261026/checkpoints/'+digest+'.tar.gz'
  cap=finite.presign('PUT',bucket,key,{'If-None-Match':'*','Content-Length':str(len(body)),'Content-Type':'application/gzip'})
  try:
   with urllib.request.urlopen(urllib.request.Request(cap['url'],data=body,headers=cap['headers'],method='PUT'),timeout=60) as response:
    if response.status not in (200,201):raise RuntimeError('checkpoint_upload_status')
  except urllib.error.HTTPError as e:
   if e.code!=412:raise RuntimeError('checkpoint_upload_rejected') from None
  except (OSError,TimeoutError):pass
  cap=finite.presign('GET',bucket,key)
  try:
   with urllib.request.urlopen(cap['url'],timeout=60) as response:back=response.read(len(body)+1)
  except Exception:raise RuntimeError('checkpoint_readback_unconfirmed') from None
  if len(back)!=len(body) or hashlib.sha256(back).hexdigest()!=digest:raise ValueError('checkpoint_readback_differs')
  receipt={'observed_at':time.time(),'bucket':bucket,'key':key,'bytes':len(body),'sha256':digest,'full_readback_verified':True,'durable':True}
  atomic(self.root/'sprint-checkpoint-receipt.json',receipt);self.last_checkpoint=time.monotonic();return receipt
 def status(self,db,phase):
  result={'observed_at':stamp(),'phase':phase,'deadline_utc':self.policy['deadline_utc'],'counts':dict(db.execute('SELECT state,count(*) FROM jobs GROUP BY state')),'workers':len(self.workers),'hosts':len(self.hosts),'new_publication_ready':False,'public_changed':False,'nas_changed':False,'source_deletion':False}
  atomic(self.root/'sprint-status.json',result);return result
 def can_admit(self,db,worker,row,ready,ledger):
  if ready.get('draining') or not ready.get('build_complete') or ready.get('active_jobs')!=0:return False
  if ready.get('volume_identity')!=worker['volume_identity'] or ready.get('root_device')!=worker['root_device']:raise ValueError('worker_retained_volume_differs')
  ids=self.byhost[str(worker['host_id'])]
  needed=3*row['source_bytes']+128*1024**2
  # Deliberately reserve complete source/output bounds for every submitted or
  # uncertain job, even bytes already allocated: never credit running jobs.
  if ready['free_bytes']<5*GIB+reserved_bytes(db,ids)+needed:return False
  export_reservation=(6*row['source_bytes']+384*1024**2)/GIB*.01
  return ledger['upper_bound_usd']+export_reservation+5<=self.policy['additional_budget_usd']
 def pass_once(self,db):
  global REQUEST_DEADLINE
  REQUEST_DEADLINE=self.deadline
  ledger=self.budget(db)
  if time.time()>=self.deadline or ledger['upper_bound_usd']+5>self.policy['additional_budget_usd']:self.stopping=True;return
  if self.last_checkpoint is None or time.monotonic()-self.last_checkpoint>=300:self.checkpoint(db)
  for worker in self.workers:
   if time.time()>=self.deadline:self.stopping=True;break
   active=db.execute("SELECT * FROM jobs WHERE state='submitted' AND worker_id=?",(worker['id'],)).fetchall()
   if len(active)>1:raise ValueError('multiple_jobs_per_slot')
   try:
    if active:finite.reconcile(self.root,db,worker,active[0]);continue
    if time.time()>=self.deadline:self.stopping=True;break
    ledger=self.budget(db)
    if ledger['upper_bound_usd']+5>=self.policy['additional_budget_usd']:self.stopping=True;break
    ready=guest(worker,'/ready')
    rows=db.execute("SELECT * FROM jobs WHERE state='queued' AND attempt=0 AND source_bytes>0 AND source_bytes<=? AND source_files<=240 AND id NOT IN (SELECT id FROM deferred_jobs WHERE until>?) ORDER BY source_bytes DESC",(8*GIB,time.time())).fetchall()
    for row in rows:
     if not self.can_admit(db,worker,row,ready,ledger):continue
     if finite.submit(db,worker,row,finite.read_work(row)):break
   except Exception as error:
    # Identity/volume/safety failures stop all admission. Uncertain transport
    # leaves submitted intent and its host reservation intact for reconciliation.
    if isinstance(error,ValueError):raise
    atomic(self.root/('sprint-error-'+worker['id']+'.json'),{'observed_at':stamp(),'class':type(error).__name__,'detail':'worker_pass_unconfirmed'})
  self.status(db,'running')
  if not db.execute("SELECT 1 FROM jobs WHERE state IN ('queued','submitted') LIMIT 1").fetchone():self.stopping=True
 def drain(self,db):
  global REQUEST_DEADLINE
  REQUEST_DEADLINE=None;self.stopping=True
  atomic(self.root/'sprint-stop.json',{'observed_at':stamp(),'deadline_reached':time.time()>=self.deadline})
  grace=time.time()<self.deadline+45*60
  mode='drain' if grace else 'stop'
  all_drained=True;proof_hosts={str(h['id']):{'workers':[],'observed_at':time.time()} for h in self.hosts}
  for worker in self.workers:
   try:
    state=guest(worker,'/drain',{'mode':mode})
    if not state.get('drained') or state.get('active_jobs')!=0 or state.get('volume_identity')!=worker['volume_identity'] or state.get('root_device')!=worker['root_device']:all_drained=False
    else:proof_hosts[str(worker['host_id'])]['workers'].append(worker['id'])
   except Exception:all_drained=False
  # Drain existing work and its strict exports, but bounded_submit forbids
  # admission/retries. Never classify an unverified stage as an exported job.
  pending=db.execute("SELECT * FROM jobs WHERE state='submitted'").fetchall()
  if grace:
   for row in pending:
    worker=self.byid.get(row['worker_id'])
    if not worker:raise ValueError('foreign_inflight_during_drain')
    if time.time()>=self.deadline+45*60:break
    try:finite.reconcile(self.root,db,worker,row)
    except Exception:all_drained=False
   if db.execute("SELECT 1 FROM jobs WHERE state='submitted' LIMIT 1").fetchone():all_drained=False
  elif all_drained:
   # Every known worker has stopped and synced; unresolved job artifacts stay
   # on their funded volume for recovery rather than being called complete.
   db.execute("UPDATE jobs SET state='held_shutdown',result=? WHERE state='submitted'",(json.dumps({'failure':'not_exported_before_shutdown','recovery':'retained_volume'}),));db.commit()
  self.status(db,'draining')
  if not all_drained:return False
  atomic(self.root/'sprint-drain-proof.json',{'observed_at':time.time(),'hosts':proof_hosts,'all_mutable_on_volume':True})
  self.checkpoint(db);return True

 def retire(self,db):
  resources=module('sprint_resources','sprint-resources.py')
  global REQUEST_DEADLINE
  REQUEST_DEADLINE=None
  proof=json.loads((self.root/'sprint-drain-proof.json').read_text())
  checkpoint=json.loads((self.root/'sprint-checkpoint-receipt.json').read_text())
  def api(method,path,payload=None):
   if not path.startswith(('/v2/droplets/','/v2/volumes/')):raise ValueError('provider_scope_refused')
   req=urllib.request.Request('https://api.digitalocean.com'+path,data=json.dumps(payload).encode() if payload is not None else None,headers={'Authorization':'Bearer '+os.environ['DIGITALOCEAN_TOKEN'],'Content-Type':'application/json'},method=method)
   with OPENER.open(req,timeout=30) as response:raw=response.read(2*1024**2)
   return json.loads(raw) if raw else {}
  while True:
   outcomes=[]
   for host in self.hosts:
    hostproof=proof['hosts'][str(host['id'])]
    statepath=self.root/('retire-'+str(host['id'])+'.json')
    prior=json.loads(statepath.read_text()) if statepath.is_file() else {}
    if not prior.get('shutdown_action') and time.time()-hostproof['observed_at']>240:
     states=[guest(w,'/drain',{'mode':'stop'}) for w in self.workers if w['host_id']==host['id']]
     if not states or any(x.get('drained') is not True or x.get('active_jobs')!=0 for x in states):continue
     hostproof['observed_at']=time.time();atomic(self.root/'sprint-drain-proof.json',proof);checkpoint=self.checkpoint(db)
    value=dict(host,drain_confirmed=True,checkpoint_confirmed=True,proof_at=hostproof['observed_at'],
     drain_receipt={'active_jobs':0,'pending_exports':0,'synced':True,'all_mutable_on_volume':True},checkpoint_receipt=checkpoint)
    if str(host['id']) not in proof['hosts']:raise ValueError('host_drain_proof_missing')
    try:outcomes.append(resources.retire_host(value,self.root,api))
    except Exception as error:outcomes.append({'droplet_id':host['id'],'status':'pending','error_class':type(error).__name__})
   self.budget(db)
   atomic(self.root/'sprint-retirement-status.json',{'observed_at':stamp(),'outcomes':outcomes})
   self.status(db,'retiring_cpu');self.checkpoint(db)
   if len(outcomes)==len(self.hosts) and all(x['status']=='retired' for x in outcomes):break
   time.sleep(15)
  self.status(db,'finished_compute_retired_volumes_preserved');receipt=self.checkpoint(db)
  source=json.loads((self.root/'sprint-source-receipt.json').read_text())
  if source.get('full_readback_verified') is not True or not source.get('key','').startswith('staging/best14-20261021/sprint-20261026/source/'):raise ValueError('controller_source_backup_missing')
  ident=self.policy['controller_id'];value=api('GET','/v2/droplets/'+str(ident))['droplet']
  if value['id']!=ident or value['name']!='stoarama-best14-sprint-20261026' or 'best14-sprint-20261026' not in value.get('tags',[]):raise ValueError('controller_retirement_scope_differs')
  if receipt['full_readback_verified'] is not True or time.time()-receipt['observed_at']>120:raise ValueError('final_checkpoint_unconfirmed')
  api('DELETE','/v2/droplets/'+str(ident))

 def run(self):
  self.root.mkdir(mode=0o700,exist_ok=True)
  (self.root/'results').mkdir(mode=0o700,exist_ok=True)
  with (self.root/'executor.lock').open('a+') as lock:
   try:fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
   except BlockingIOError:return
   with contextlib.closing(sqlite3.connect(self.root/'execution.sqlite3',timeout=60)) as db:
    db.row_factory=sqlite3.Row;self.validate(db);finite.guest=guest;finite.head=self.bounded_head;finite.submit=self.bounded_submit
    for sql in ['CREATE TABLE IF NOT EXISTS export_attempts(id TEXT PRIMARY KEY,count INTEGER)','CREATE TABLE IF NOT EXISTS submission_attempts(id TEXT PRIMARY KEY,count INTEGER)','CREATE TABLE IF NOT EXISTS deferred_jobs(id TEXT PRIMARY KEY,until REAL)','CREATE TABLE IF NOT EXISTS archive_attempts(id TEXT PRIMARY KEY,count INTEGER)','CREATE TABLE IF NOT EXISTS archive_receipts(id TEXT PRIMARY KEY,receipt TEXT)','CREATE TABLE IF NOT EXISTS head_attempts(count INTEGER)']:db.execute(sql)
    db.commit()
    self.stopping=(self.root/'sprint-stop.json').exists()
    while not self.stopping:
     if __import__('shutil').disk_usage(self.root).free<5*GIB:self.stopping=True;break
     try:self.pass_once(db)
     except Exception as error:
      atomic(self.root/'sprint-owner-error.json',{'observed_at':stamp(),'class':type(error).__name__,'detail':'owner_pass_failed'});self.stopping=True;break
     time.sleep(5)
    if not self.retirement_started():
     while not self.drain(db):time.sleep(10)
    self.status(db,'drained_retirement_required');self.checkpoint(db);self.retire(db)
if __name__=='__main__':
 parser=argparse.ArgumentParser();parser.add_argument('--run-root',required=True);args=parser.parse_args();Owner(args.run_root).run()
