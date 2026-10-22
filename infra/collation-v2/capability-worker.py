#!/usr/bin/env python3
"""Private MicroVM staging endpoint. Gateway authenticates requests; no master keys here."""
import base64,hashlib,http.server,json,os,pathlib,signal,subprocess,threading,time,tarfile,io,urllib.parse
import capability_export
ROOT=pathlib.Path('/opt/best14');ROOT.mkdir(mode=0o700,exist_ok=True);LOCK=threading.Lock();ACTIVE=set()
def write(path,value):
 temp=path.with_suffix(path.suffix+'.pending');fd=os.open(temp,os.O_WRONLY|os.O_CREAT|os.O_TRUNC,0o600)
 with os.fdopen(fd,'w') as f:json.dump(value,f,separators=(',',':'));f.flush();os.fsync(f.fileno())
 os.replace(temp,path)
def job_root(job):
 if len(job)!=64 or any(c not in '0123456789abcdef' for c in job):raise ValueError('invalid job identity')
 return ROOT/'jobs'/job

def execute(job,attempt):
 owner=job_root(job);root=owner/('attempt-'+str(attempt));state=owner/'state.json'
 try:
  write(state,{'job_id':job,'state':'running','attempt':attempt,'started_at':time.time()})
  with (root/'worker.log').open('ab') as log:
   child=subprocess.Popen([str(ROOT/'collation-job'),'--input',str(root/'request.json'),'--root',str(root/'staged')],stdout=log,stderr=log,start_new_session=True)
   try:code=child.wait(timeout=1830)
   except subprocess.TimeoutExpired:
    os.killpg(child.pid,signal.SIGKILL);child.wait();code=-1
  if code:write(state,{'job_id':job,'state':'failed','attempt':attempt,'failure':'media_job_failed','finished_at':time.time()})
  else:
   result=json.loads((root/'staged'/'result.json').read_text());write(state,{'job_id':job,'state':'staged','attempt':attempt,'result':result,'finished_at':time.time()})
 except Exception:write(state,{'job_id':job,'state':'failed','attempt':attempt,'failure':'worker_execution_failure','finished_at':time.time()})
 finally:
  with LOCK:ACTIVE.discard(job)

def live_runner(job,attempt):
 root=job_root(job)/('attempt-'+str(attempt));wanted=[str(ROOT/'collation-job'),'--input',str(root/'request.json'),'--root',str(root/'staged')]
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
    if os.getpgid(pid)==pid:os.killpg(pid,signal.SIGKILL)
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
  with LOCK:ACTIVE.discard(job)

def recover_orphans():
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
  receipts=capability_export.export(owner/('attempt-'+str(old['attempt']))/'staged',old['result'],capabilities)
  old.update(state='exported',export_receipts=receipts,exported_at=time.time());write(state,old)
 except Exception:
  # Keep every local artifact for reconciliation. Do not expose capability URLs
  # through error strings, and do not turn staging failure into coverage.
  old.update(state='export_failed',failure='artifact_export_failed');write(state,old)
 finally:
  with LOCK:ACTIVE.discard(job)
class Handler(http.server.BaseHTTPRequestHandler):
 def log_message(self,*args):pass
 def answer(self,status,value):
  body=json.dumps(value,separators=(',',':')).encode();self.send_response(status);self.send_header('Content-Type','application/json');self.send_header('Content-Length',str(len(body)));self.end_headers();self.wfile.write(body)
 def do_GET(self):
  route=urllib.parse.urlsplit(self.path)
  if route.path=='/ready':return self.answer(200,{'build_complete':(ROOT/'build-complete').exists(),'active_jobs':len(ACTIVE)})
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
   size=int(self.headers.get('Content-Length','0'))
   if size<=0 or size>4*1024**2:return self.answer(413,{'error':'request_size'})
   body=self.rfile.read(size)
   if len(body)!=size:return self.answer(400,{'error':'incomplete_body'})
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
     if ACTIVE:return self.answer(409,{'error':'worker_busy'})
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
      if path.exists():path.unlink()
     old['state']='released';old['released_at']=time.time();write(root/'state.json',old)
    return self.answer(200,old)
   return self.answer(404,{'error':'route_missing'})
  except (KeyError,ValueError,TypeError):return self.answer(400,{'error':'invalid_request'})
  except Exception:return self.answer(500,{'error':'worker_internal_error'})
if __name__=='__main__':
 recover_orphans()
 http.server.ThreadingHTTPServer(('127.0.0.1',8080),Handler).serve_forever()
