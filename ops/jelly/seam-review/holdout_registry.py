"""Hot-register new immutable terminal-review batches; preserve all prior bindings."""
from pathlib import Path
import hashlib,json,re,threading
from holdout_review import HoldoutReview
import review_asset_r2
def verified_asset(root,path,sha):
 p=root/path.lstrip("/")
 if p.is_file():return hashlib.sha256(p.read_bytes()).hexdigest()==sha
 return review_asset_r2.available("/"+path.lstrip("/"),sha)

ID=re.compile(r'terminal-heldout-\d{8}-batch\d{2,4}')
SHA=re.compile(r'[0-9a-f]{64}')
class HoldoutRegistry:
 def __init__(self,root,page,slide_js,frame_js,existing):
  self.root,self.page,self.slide_js,self.frame_js=root,page,slide_js,frame_js
  self.existing=existing;self.loaded={};self.rejected=set();self.lock=threading.RLock()
 def validate(self,p,c):
  key=c.get('id');assert isinstance(key,str) and ID.fullmatch(key) and p.name=='candidate-'+key+'.json'
  sha=c.get('evidence_sha256');assert isinstance(sha,str) and SHA.fullmatch(sha)
  assert c['evidence_filename']=='evidence-'+sha+'.json'
  assert hashlib.sha256((self.root/'comparison-evidence'/c['evidence_filename']).read_bytes()).hexdigest()==sha
  assert isinstance(c['cases'],list) and 0<len(c['cases'])<=64
  assert len({v['id'] for v in c['cases']})==len(c['cases'])
  for v in c['cases']:
   s=v['preview_sha256'];assert SHA.fullmatch(s)
   assert isinstance(v['video'],str) and re.fullmatch(r'vid/'+re.escape(key)+r'-case-\d{1,3}\.mp4',v['video'])
   assert verified_asset(self.root,v['video'],s)
   f=v['frame_preview'];z=f['index_sha256'];assert SHA.fullmatch(z)
   assert f['index_url']=='/frame-review/'+s+'/index-'+z+'.json'
   body=(self.root/f['index_url'].lstrip('/')).read_bytes();assert hashlib.sha256(body).hexdigest()==z
   j=json.loads(body);assert j['source_sha256']==s and j['min_frame']==f['min_frame'] and j['max_frame']==f['max_frame']
   assert 0<=j['min_frame']<=f['seam_frame_index']<=j['max_frame']<len(j['frames'])
   assert j['frames'][f['seam_frame_index']]['image_sha256']==f['seam_image_sha256']
   for frame in j['frames'][j['min_frame']:j['max_frame']+1]:
    image=frame['image_sha256'];assert SHA.fullmatch(image)
    assert verified_asset(self.root,'frame-review/'+s+'/frame-'+image+'.png',image)
   if v.get('full_preview'):
    full=v['full_preview'];assert SHA.fullmatch(full['sha256']) and re.fullmatch(r'vid/'+re.escape(key)+r'-case-\d{1,3}-full\.mp4',full['video'])
    assert verified_asset(self.root,full['video'],full['sha256'])
    native=v.get('native_frame_preview')
    if native:
     ns=native['index_sha256'];assert SHA.fullmatch(ns) and native['index_url']=='/frame-review/'+full['sha256']+'/index-'+ns+'.json'
     body=(self.root/native['index_url'].lstrip('/')).read_bytes();assert hashlib.sha256(body).hexdigest()==ns
     nj=json.loads(body);assert nj['source_sha256']==full['sha256'] and nj['frames'][native['seam_frame_index']]['image_sha256']==native['seam_image_sha256']
     for nf in nj['frames'][nj['min_frame']:nj['max_frame']+1]:
      im=nf['image_sha256'];assert SHA.fullmatch(im) and verified_asset(self.root,'frame-review/'+full['sha256']+'/frame-'+im+'.png',im)
  return key
 def refresh(self):
  with self.lock:
   paths=sorted(self.root.glob('candidate-terminal-heldout-*-batch*.json'))
   for p in paths[:256]:
    key=None
    try:
     if p.stat().st_size>2*1024*1024:continue
     body=p.read_bytes();stamp=hashlib.sha256(body).hexdigest();c=json.loads(body);key=c.get('id')
     if key in self.loaded:
      if self.loaded[key][0]!=stamp:self.rejected.add(key)
      else:self.rejected.discard(key)
      continue
     key=self.validate(p,c)
     reviewer=HoldoutReview(self.root,self.page,self.slide_js,self.frame_js,filename=p.name,expected_id=key,vote_endpoint='/api/terminal-heldout-vote/'+key)
     self.loaded[key]=(stamp,reviewer);self.rejected.discard(key)
    except (OSError,ValueError,KeyError,TypeError,AssertionError):
     if isinstance(locals().get('key'),str):self.rejected.add(key)
   return [self.loaded[k][1] for k in sorted(self.loaded,reverse=True) if k not in self.rejected]+[self.existing]
 def reviewers(self):return self.refresh()
 def get(self,h,u,q):
  if not (u.path.startswith('/candidate/terminal-heldout-') or u.path.startswith('/api/candidate-votes/terminal-heldout-') or u.path.startswith('/api/candidate-receipt/terminal-heldout-')):return False
  reviews=self.refresh();key=u.path.rsplit('/',1)[-1]
  if key in self.rejected:h.send(503,'Review batch identity changed; original labels preserved');return True
  reviewer=next((r for r in reviews if r.id==key),None)
  if not reviewer:h.send(404,'Unknown review batch');return True
  return reviewer.get(h,u,q)
 def head(self,h,u,q):
  if not u.path.startswith('/candidate/terminal-heldout-'):return False
  reviews=self.refresh();key=u.path.rsplit('/',1)[-1];reviewer=next((r for r in reviews if r.id==key),None)
  try:body=reviewer.render(q).encode() if reviewer else b'Unknown review batch';status=200 if reviewer else 404
  except KeyError:body=b'Unknown candidate item';status=404
  if key in self.rejected:body=b'Review batch identity changed';status=503
  h.send_response(status);h.send_header('Content-Type','text/html; charset=utf-8');h.send_header('Content-Length',str(len(body)));h.send_header('Cache-Control','no-store');h.end_headers();return True
 def post(self,h):
  path=h.path.split('?',1)[0];base='/api/terminal-heldout-vote/'
  if not path.startswith(base):return False
  key=path[len(base):];reviews=self.refresh()
  if key in self.rejected:h.send(503,'Review batch identity changed; original labels preserved');return True
  reviewer=next((r for r in reviews if r.id==key),None)
  if not reviewer or not ID.fullmatch(key):h.send(404,'Unknown review batch');return True
  reviewer.post(h);return True
