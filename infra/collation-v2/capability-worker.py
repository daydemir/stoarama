#!/usr/bin/env python3
"""Private MicroVM staging endpoint. Gateway authenticates requests; no master keys here."""
import base64,hashlib,http.server,json,os,pathlib,signal,subprocess,threading,time,tarfile,io,urllib.parse
import capability_export
ROOT=pathlib.Path(os.environ.get('STOARAMA_WORKER_ROOT','/opt/best14'))
BINARY=pathlib.Path(os.environ.get('STOARAMA_WORKER_BINARY',str(ROOT/'collation-job')))
BIND=os.environ.get('STOARAMA_WORKER_BIND','127.0.0.1')
PORT=int(os.environ.get('STOARAMA_WORKER_PORT','8080'))
MOUNT=os.environ.get('STOARAMA_VOLUME_MOUNT','')
UUID=os.environ.get('STOARAMA_VOLUME_UUID','')
LOCK=threading.Lock();ACTIVE=set();DRAINING=False;STOPPING=False;VOLUME_IDENTITY=None
class VolumeUnavailable(RuntimeError):pass

def mounted_identity():
 if not MOUNT or not UUID:raise VolumeUnavailable('volume configuration incomplete')
 mount=pathlib.Path(MOUNT)
 if not mount.is_absolute() or mount.resolve()!=mount or not ROOT.is_absolute() or ROOT.resolve()!=ROOT or not ROOT.is_relative_to(mount):raise VolumeUnavailable('volume path refused')
 # Root-owned deployment configuration supplies the filesystem UUID. A mount
 # record, not directory existence, establishes retained storage ownership.
 rows=[line.split() for line in pathlib.Path('/proc/self/mountinfo').read_text().splitlines()]
 rows=[r for r in rows if r[4]==str(mount)]
 if len(rows)!=1:raise VolumeUnavailable('required volume is not mounted')
 row=rows[0];st=mount.stat();device=pathlib.Path('/dev/disk/by-uuid')/UUID
 try:expected=device.stat().st_rdev
 except OSError:raise VolumeUnavailable('required volume UUID missing') from None
 if st.st_dev!=expected or row[2]!=str(os.major(expected))+':'+str(os.minor(expected)):raise VolumeUnavailable('required volume differs')
 return (row[0],st.st_dev,st.st_ino)

def require_volume():
 if MOUNT or UUID:
  if VOLUME_IDENTITY is None or mounted_identity()!=VOLUME_IDENTITY:raise VolumeUnavailable('volume identity changed')

def initialize_root():
 global VOLUME_IDENTITY
 if MOUNT or UUID:VOLUME_IDENTITY=mounted_identity()
 require_volume()
 ROOT.mkdir(mode=0o700,parents=True,exist_ok=True)
 if MOUNT:
  if ROOT.stat().st_dev!=VOLUME_IDENTITY[1]:raise VolumeUnavailable('worker root device differs')
  # Keep Python and native temporary files on the same retained volume.
  (ROOT/'tmp').mkdir(mode=0o700,exist_ok=True)
  os.environ['TMPDIR']=str(ROOT/'tmp')

def ready_state():
 require_volume();space=os.statvfs(ROOT)
 with LOCK:active=len(ACTIVE);draining=DRAINING
 if draining and not active:os.sync()
 return {'build_complete':(ROOT/'build-complete').exists(),'active_jobs':active,
         'draining':draining,'drained':draining and not active,
         'free_bytes':space.f_bavail*space.f_frsize,'root_device':ROOT.stat().st_dev,
         'volume_identity':list(VOLUME_IDENTITY) if VOLUME_IDENTITY else None}

def begin_drain(mode):
 global DRAINING,STOPPING
 if mode not in ('drain','stop'):raise ValueError('invalid drain mode')
 require_volume()
 with LOCK:
  DRAINING=True;STOPPING=STOPPING or mode=='stop';jobs=list(ACTIVE)
  write(ROOT/'drain.json',{'mode':'stop' if STOPPING else 'drain'})
 if mode=='stop':
  for job in jobs:
   state=json.loads((job_root(job)/'state.json').read_text())
   if state['state'] not in ('accepted','running') and state.get('failure')!='worker_recovery_hold':continue
   with LOCK:pid=live_runner(job,state['attempt'])
   if pid is not None:
    # Revalidate exact argv and process group immediately before signalling.
    if live_runner(job,state['attempt'])!=pid or os.getpgid(pid)!=pid:raise RuntimeError('runner ownership uncertain')
    os.killpg(pid,signal.SIGTERM)
 return ready_state()

initialize_root()
if (ROOT/'drain.json').exists():
 DRAINING=True;STOPPING=json.loads((ROOT/'drain.json').read_text()).get('mode')=='stop'
def write(path,value):
 require_volume()
 temp=path.with_suffix(path.suffix+'.pending');fd=os.open(temp,os.O_WRONLY|os.O_CREAT|os.O_TRUNC,0o600)
 with os.fdopen(fd,'w') as f:json.dump(value,f,separators=(',',':'));f.flush();os.fsync(f.fileno())
 require_volume();os.replace(temp,path)
 parent_fd=os.open(path.parent,os.O_RDONLY|os.O_DIRECTORY)
 try:os.fsync(parent_fd)
 finally:os.close(parent_fd)
def job_root(job):
 require_volume()
 if len(job)!=64 or any(c not in '0123456789abcdef' for c in job):raise ValueError('invalid job identity')
 return ROOT/'jobs'/job

def execute(job,attempt):
 owner=job_root(job);root=owner/('attempt-'+str(attempt));state=owner/'state.json'
 try:
  write(state,{'job_id':job,'state':'running','attempt':attempt,'started_at':time.time()})
  require_volume()
  with (root/'worker.log').open('ab') as log:
   with LOCK:
    if STOPPING:raise RuntimeError('worker stopping')
    child=subprocess.Popen([str(BINARY),'--input',str(root/'request.json'),'--root',str(root/'staged')],stdout=log,stderr=log,start_new_session=True)
   try:code=child.wait(timeout=1830)
   except subprocess.TimeoutExpired:
    os.killpg(child.pid,signal.SIGKILL);child.wait();code=-1
  if code:write(state,{'job_id':job,'state':'failed','attempt':attempt,'failure':'media_job_failed','finished_at':time.time()})
  else:
   result=json.loads((root/'staged'/'result.json').read_text());write(state,{'job_id':job,'state':'staged','attempt':attempt,'result':result,'finished_at':time.time()})
 except VolumeUnavailable:return  # Retain ACTIVE until volume owner recovery.
 except Exception:write(state,{'job_id':job,'state':'failed','attempt':attempt,'failure':'worker_execution_failure','finished_at':time.time()})
 finally:
  try:require_volume()
  except VolumeUnavailable:pass
  else:
   with LOCK:ACTIVE.discard(job)

def live_runner(job,attempt):
 root=job_root(job)/('attempt-'+str(attempt));wanted=[str(BINARY),'--input',str(root/'request.json'),'--root',str(root/'staged')]
 found=[]
 for proc in pathlib.Path('/proc').iterdir():
  if not proc.name.isdigit():continue
  try:argv=[p.decode() for p in (proc/'cmdline').read_bytes().split(b'\0') if p]
  except (OSError,UnicodeError):continue
  if argv==wanted:found.append(int(proc.name))
 if len(found)>1:raise RuntimeError('multiple matching job runners')
 return found[0] if found else None

def recover_execution(job,attempt,deadline):
 owner=job_root(job);state=owner/'state.json';root=owner/('attempt-'+str(attempt))
 try:
  while True:
   pid=live_runner(job,attempt)
   if pid is None:break
   if time.time()>=deadline:
    # Newly started runners own their process group; never signal another
    # group's processes or trust a stale PID after a restart.
    if live_runner(job,attempt)==pid and os.getpgid(pid)==pid:os.killpg(pid,signal.SIGKILL)
    else:raise RuntimeError('legacy runner retained for owner recovery')
   time.sleep(1)
  result_path=root/'staged'/'result.json'
  if result_path.exists():
   result=json.loads(result_path.read_text());request=json.loads((root/'request.json').read_text())
   if result.get('ready_to_publish') is not False or result['manifest']['hour_id']!=request['work']['hour_id']:raise ValueError('recovered result identity differs')
   write(state,{'job_id':job,'attempt':attempt,'state':'staged','result':result,'finished_at':time.time()})
  else:write(state,{'job_id':job,'attempt':attempt,'state':'failed','failure':'worker_restarted','finished_at':time.time()})
 except Exception:
  write(state,{'job_id':job,'attempt':attempt,'state':'failed','failure':'worker_recovery_hold','recovery_deadline':deadline,'finished_at':time.time()})
  return  # Retain ACTIVE: uncertain live processes must block new assignments.
 else:
  try:require_volume()
  except VolumeUnavailable:pass
  else:
   with LOCK:ACTIVE.discard(job)

def recover_orphans():
 require_volume()
 for path in (ROOT/'jobs').glob('*/state.json'):
  old=json.loads(path.read_text());job=old['job_id'];assert path.parent==job_root(job)
  if old['state'] in ('accepted','running') or (old['state']=='failed' and old.get('failure')=='worker_recovery_hold'):
   deadline=old.get('recovery_deadline',old.get('started_at',old.get('accepted_at',time.time()))+1830)
   ACTIVE.add(job);threading.Thread(target=recover_execution,args=(job,old['attempt'],deadline),daemon=True).start()
  elif old['state']=='exporting':
   # The previous PUT may have succeeded. Re-export uses create-only PUT plus
   # full readback, so preserve artifacts and reconcile with fresh capabilities.
   old.update(state='export_failed',failure='worker_restarted');write(path,old)

def export_job(job,capabilities):
 owner=job_root(job);state=owner/'state.json';old=json.loads(state.read_text())
 try:
  require_volume()
  receipts=capability_export.export(owner/('attempt-'+str(old['attempt']))/'staged',old['result'],capabilities)
  old.update(state='exported',export_receipts=receipts,exported_at=time.time());write(state,old)
 except Exception:
  # Keep every local artifact for reconciliation. Do not expose capability URLs
  # through error strings, and do not turn staging failure into coverage.
  old.update(state='export_failed',failure='artifact_export_failed');write(state,old)
 finally:
  try:require_volume()
  except VolumeUnavailable:pass
  else:
   with LOCK:ACTIVE.discard(job)
class Handler(http.server.BaseHTTPRequestHandler):
 def log_message(self,*args):pass
 def answer(self,status,value):
  body=json.dumps(value,separators=(',',':')).encode();self.send_response(status);self.send_header('Content-Type','application/json');self.send_header('Content-Length',str(len(body)));self.end_headers();self.wfile.write(body)
 def do_GET(self):
  try:require_volume()
  except VolumeUnavailable:return self.answer(503,{'error':'volume_unavailable'})
  route=urllib.parse.urlsplit(self.path)
  if route.path=='/ready':return self.answer(200,ready_state())
  if route.path=='/job':
   try:
    job=urllib.parse.parse_qs(route.query)['id'][0];root=job_root(job);state=json.loads((root/'state.json').read_text());return self.answer(200,state)
   except (KeyError,ValueError,FileNotFoundError):return self.answer(404,{'error':'job_missing'})
  if route.path=='/artifact':
   try:
    q=urllib.parse.parse_qs(route.query);job=q['id'][0];key=q['key'][0];owner=job_root(job);state=json.loads((owner/'state.json').read_text());result=state['result'];entry=next(o for o in result['manifest']['outputs'] if o['object_key']==key)
    path=owner/('attempt-'+str(state['attempt']))/'staged'/'outputs'/key
    if not path.is_file() or path.is_symlink() or path.stat().st_size!=entry['size_bytes']:return self.answer(409,{'error':'artifact_identity'})
    self.send_response(200);self.send_header('Content-Type','video/mp4');self.send_header('Content-Length',str(entry['size_bytes']));self.send_header('X-Artifact-SHA256',entry['sha256']);self.end_headers()
    with path.open('rb') as f:
     while True:
      block=f.read(65536)
      if not block:break
      self.wfile.write(block)
    return
   except (KeyError,ValueError,FileNotFoundError,StopIteration):return self.answer(404,{'error':'artifact_missing'})
  return self.answer(404,{'error':'route_missing'})
 def do_POST(self):
  try:
   require_volume()
   size=int(self.headers.get('Content-Length','0'))
   if size<=0 or size>4*1024**2:return self.answer(413,{'error':'request_size'})
   body=self.rfile.read(size)
   if len(body)!=size:return self.answer(400,{'error':'incomplete_body'})
   if self.path=='/drain':return self.answer(200,begin_drain(json.loads(body).get('mode','drain')))
   if self.path=='/source':
    if hashlib.sha256(body).hexdigest()!=self.headers.get('X-Source-SHA256'):return self.answer(400,{'error':'source_digest'})
    if (ROOT/'source.tar.gz').exists():return self.answer(409,{'error':'source_already_uploaded'})
    fd=os.open(ROOT/'source.tar.gz',os.O_WRONLY|os.O_CREAT|os.O_EXCL,0o600)
    with os.fdopen(fd,'wb') as f:f.write(body)
    return self.answer(200,{'source_bytes':len(body),'sha256':hashlib.sha256(body).hexdigest()})
   if self.path=='/job':
    if not (ROOT/'build-complete').exists():return self.answer(503,{'error':'worker_not_ready'})
    data=json.loads(body);request=data['request'];job=data['job_id'];root=job_root(job)
    encoded=json.dumps(request['work'],sort_keys=True,separators=(',',':')).encode()
    if hashlib.sha256(encoded).hexdigest()!=job:return self.answer(400,{'error':'work_identity'})
    if request['work'].get('batch_id')!='best14-20261021' or not 30<=request.get('timeout_seconds',0)<=1800:return self.answer(400,{'error':'work_scope'})
    if len(request['work']['clips'])>240 or any(not isinstance(c.get('size_bytes'),int) or c['size_bytes']<=0 or c['size_bytes']>1024**3 for c in request['work']['clips']) or len(request.get('sources',{}))!=len(request['work']['clips']):return self.answer(400,{'error':'source_accounting'})
    # Reserve 5GiB plus whole sources, staged parts and a bounded metadata margin.
    source_bytes=sum(c['size_bytes'] for c in request['work']['clips']);space=os.statvfs(ROOT)
    if source_bytes<=0 or source_bytes>8*1024**3 or space.f_bavail*space.f_frsize<5*1024**3+3*source_bytes+128*1024**2:return self.answer(507,{'error':'scratch_capacity'})
    attempt=data.get('attempt',1)
    if not isinstance(attempt,int) or not 1<=attempt<=3:return self.answer(400,{'error':'attempt_scope'})
    with LOCK:
     if root.exists():
      old=json.loads((root/'state.json').read_text())
      if old.get('state')!='failed' or attempt!=old.get('attempt',1)+1:return self.answer(200,old)
     elif attempt!=1:return self.answer(400,{'error':'attempt_sequence'})
     if DRAINING:return self.answer(409,{'error':'worker_draining'})
     if ACTIVE:return self.answer(409,{'error':'worker_busy'})
     require_volume()
     root.mkdir(mode=0o700,parents=True,exist_ok=True);attempt_root=root/('attempt-'+str(attempt));attempt_root.mkdir(mode=0o700);write(attempt_root/'request.json',request);write(root/'state.json',{'job_id':job,'state':'accepted','attempt':attempt,'accepted_at':time.time()});ACTIVE.add(job)
    threading.Thread(target=execute,args=(job,attempt),daemon=True).start();return self.answer(202,{'job_id':job,'state':'accepted','attempt':attempt})
   if self.path=='/export':
    data=json.loads(body);job=data['job_id'];root=job_root(job)
    with LOCK:
     old=json.loads((root/'state.json').read_text())
     if old['state'] in ('exporting','exported'):return self.answer(200,old)
     if old['state'] not in ('staged','export_failed') or ACTIVE:return self.answer(409,{'error':'job_not_exportable'})
     keys=[o['object_key'] for o in old['result']['manifest']['outputs']]
     if len(keys)!=len(set(keys)) or set(keys)!=set(data['capabilities']):return self.answer(400,{'error':'export_accounting'})
     old['state']='exporting';write(root/'state.json',old);ACTIVE.add(job)
    threading.Thread(target=export_job,args=(job,data['capabilities']),daemon=True).start();return self.answer(202,{'job_id':job,'state':'exporting'})
   if self.path=='/release':
    data=json.loads(body);job=data['job_id'];root=job_root(job)
    with LOCK:
     old=json.loads((root/'state.json').read_text())
     if old['state']=='released':return self.answer(200,old)
     if old['state']!='exported' or ACTIVE:return self.answer(409,{'error':'job_not_verified'})
     outputs=old['result']['manifest']['outputs'];receipts=old['export_receipts']
     if len(outputs)!=len(receipts) or any(not r['full_readback_verified'] for r in receipts):return self.answer(409,{'error':'export_proof_missing'})
     # Only release this job's generated artifacts, after complete R2 readback.
     for output in outputs:
      key=output['object_key'];path=root/('attempt-'+str(old['attempt']))/'staged'/'outputs'/key
      if not key.startswith('joined/best14-20261021/') or '..' in pathlib.PurePosixPath(key).parts or path.is_symlink():return self.answer(409,{'error':'artifact_path'})
      require_volume()
      if path.exists():path.unlink()
     old['state']='released';old['released_at']=time.time();write(root/'state.json',old)
    return self.answer(200,old)
   return self.answer(404,{'error':'route_missing'})
  except VolumeUnavailable:return self.answer(503,{'error':'volume_unavailable'})
  except (KeyError,ValueError,TypeError):return self.answer(400,{'error':'invalid_request'})
  except Exception:return self.answer(500,{'error':'worker_internal_error'})
if __name__=='__main__':
 recover_orphans()
 http.server.ThreadingHTTPServer((BIND,PORT),Handler).serve_forever()
