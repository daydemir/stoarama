"""Exact private canary preview capability; video bytes travel directly from R2."""
import json,re,subprocess,uuid,io,time,datetime
from urllib.parse import urlsplit,parse_qs,unquote
from pathlib import Path
import coverage_downloads as delivery
INDEX=Path('/Users/jelly/.stoarama/seam-review/private/review-preview-r2-index.json')
PREFIX='experimental/stitching-confident-repairs/20261009/model49-review/'
SHA=re.compile('[a-f0-9]{64}')
VIDEO=re.compile(r'/vid/terminal-heldout-20261009-batch[0-9]{2,4}-case-[0-9]{1,3}\.mp4')
GUARD='python3 /opt/collate/stitching-continuation-20261009/guard.py /opt/collate/stitching-continuation-20261009'
OLD_PHASE='model39-49-private-canary-20261009'
PHASES={OLD_PHASE:(PREFIX,GUARD),'model55-private-six-review-20261009':('experimental/stitching-confident-repairs/20261009/model55-review/','python3 /opt/collate/stitching-continuation-20261009/model55-review-guard.py /opt/collate/stitching-continuation-20261009')}
PHASES['model69-private-six-review-20261009']=('experimental/stitching-confident-repairs/20261009/model69-review/','python3 /opt/collate/stitching-continuation-20261009/model69-review-guard.py /opt/collate/stitching-continuation-20261009')
PHASES['model79-private-six-review-20261009']=('experimental/stitching-confident-repairs/20261009/model79-review/','python3 /opt/collate/stitching-continuation-20261009/model79-review-guard.py /opt/collate/stitching-continuation-20261009')

PHASES['model79-private-fourteen-review-20261009']=('experimental/stitching-confident-repairs/20261009/model79-review/','python3 /opt/collate/stitching-continuation-20261009/model79-fourteen-review-guard.py /opt/collate/stitching-continuation-20261009')
WEEKEND_START_EPOCH=1791566614.9024413
WEEKEND_GUARD='python3 /opt/collate/stitching-continuation-20261009/model79-weekend-four-delivery-guard.py /opt/collate/stitching-continuation-20261009'
WEEKEND_ASSETS={'57fe8bce7f326b76e278f5fb3817a231220a3c5502222beb74234c1d472e6cfa': {'sha256': '57fe8bce7f326b76e278f5fb3817a231220a3c5502222beb74234c1d472e6cfa', 'size_bytes': 97882, 'etag': '"e89a54be4034021fe101898b3c5791a8"', 'object_key': 'experimental/stitching-confident-repairs/20261009/model79-review/57fe8bce7f326b76e278f5fb3817a231220a3c5502222beb74234c1d472e6cfa.mp4', 'phase_id': 'model79-private-six-review-20261009', 'relative': '/vid/terminal-heldout-20261009-batch231-case-1.mp4', 'full_R2_SHA_readback_verified': True}, 'ede2cf9c05a7e1e0a017fc1bef9b39ccef5eb954f70aba60dcaf9af3c45c65a7': {'sha256': 'ede2cf9c05a7e1e0a017fc1bef9b39ccef5eb954f70aba60dcaf9af3c45c65a7', 'size_bytes': 96588, 'etag': '"713b633f8719f49fb0a45894997c63f7"', 'object_key': 'experimental/stitching-confident-repairs/20261009/model79-review/ede2cf9c05a7e1e0a017fc1bef9b39ccef5eb954f70aba60dcaf9af3c45c65a7.mp4', 'phase_id': 'model79-private-six-review-20261009', 'relative': '/vid/terminal-heldout-20261009-batch232-case-1.mp4', 'full_R2_SHA_readback_verified': True}, 'c793a7a0b06238e32fc545e1b063ff153c6c676766cc0dabdea4cca49c26fd1d': {'sha256': 'c793a7a0b06238e32fc545e1b063ff153c6c676766cc0dabdea4cca49c26fd1d', 'size_bytes': 90405, 'etag': '"ce44a7ffd76c139d466555c8c46adb56"', 'object_key': 'experimental/stitching-confident-repairs/20261009/model79-review/c793a7a0b06238e32fc545e1b063ff153c6c676766cc0dabdea4cca49c26fd1d.mp4', 'phase_id': 'model79-private-six-review-20261009', 'relative': '/vid/terminal-heldout-20261009-batch235-case-1.mp4', 'full_R2_SHA_readback_verified': True}, '192ebf31e76b8e84a7cd62a2c8eef478b04579670eff4d8771a1ea1cf65cefd2': {'sha256': '192ebf31e76b8e84a7cd62a2c8eef478b04579670eff4d8771a1ea1cf65cefd2', 'size_bytes': 562535, 'etag': '"724fd2fdbc63c07d994c59cc0e47cc05"', 'object_key': 'experimental/stitching-confident-repairs/20261009/model79-review/192ebf31e76b8e84a7cd62a2c8eef478b04579670eff4d8771a1ea1cf65cefd2.mp4', 'phase_id': 'model79-private-six-review-20261009', 'relative': '/vid/terminal-heldout-20261009-batch234-case-1.mp4', 'full_R2_SHA_readback_verified': True}}

ELEVEN_START_EPOCH=1791567586.8446355
ELEVEN_GUARD='python3 /opt/collate/stitching-continuation-20261009/model79-weekend-eleven-delivery-guard.py /opt/collate/stitching-continuation-20261009'
ELEVEN_ASSETS={'11b3f0d2075f2bc97280b9a5193ea7d31283a824202536455ca7aa1df4c8d0e4': {'etag': '"bc3c481acfa25ea805606b57d998d11c"', 'full_R2_SHA_readback_verified': True, 'object_key': 'experimental/stitching-confident-repairs/20261009/model79-review/11b3f0d2075f2bc97280b9a5193ea7d31283a824202536455ca7aa1df4c8d0e4.mp4', 'phase_id': 'model79-private-fourteen-review-20261009', 'relative': '/vid/terminal-heldout-20261009-batch236-case-1.mp4', 'sha256': '11b3f0d2075f2bc97280b9a5193ea7d31283a824202536455ca7aa1df4c8d0e4', 'size_bytes': 139680}, '47eeecb50b7615976b6e822d56de67f80a6e645dc696c96470b430b5138b2faf': {'etag': '"04d7e83a618ce49df4b5927d186c0c5b"', 'full_R2_SHA_readback_verified': True, 'object_key': 'experimental/stitching-confident-repairs/20261009/model79-review/47eeecb50b7615976b6e822d56de67f80a6e645dc696c96470b430b5138b2faf.mp4', 'phase_id': 'model79-private-fourteen-review-20261009', 'relative': '/vid/terminal-heldout-20261009-batch238-case-1.mp4', 'sha256': '47eeecb50b7615976b6e822d56de67f80a6e645dc696c96470b430b5138b2faf', 'size_bytes': 158074}, '030af38b27797e6136c632a3bc3b106139ad281709b783f4c01c236f8ea4d56c': {'etag': '"b475841f4c6b0808ac35ff8d055b4fdb"', 'full_R2_SHA_readback_verified': True, 'object_key': 'experimental/stitching-confident-repairs/20261009/model79-review/030af38b27797e6136c632a3bc3b106139ad281709b783f4c01c236f8ea4d56c.mp4', 'phase_id': 'model79-private-fourteen-review-20261009', 'relative': '/vid/terminal-heldout-20261009-batch239-case-1.mp4', 'sha256': '030af38b27797e6136c632a3bc3b106139ad281709b783f4c01c236f8ea4d56c', 'size_bytes': 499563}, '69a8eb09ec2ceee9ae7291bd3023d32df8d68c6a0949842d4cfd88b7bb5ce650': {'etag': '"0cc9129787ab032fc6e339c65875267b"', 'full_R2_SHA_readback_verified': True, 'object_key': 'experimental/stitching-confident-repairs/20261009/model79-review/69a8eb09ec2ceee9ae7291bd3023d32df8d68c6a0949842d4cfd88b7bb5ce650.mp4', 'phase_id': 'model79-private-fourteen-review-20261009', 'relative': '/vid/terminal-heldout-20261009-batch240-case-1.mp4', 'sha256': '69a8eb09ec2ceee9ae7291bd3023d32df8d68c6a0949842d4cfd88b7bb5ce650', 'size_bytes': 160684}, '2f7a8c6309daa2dd0e8d365ecaa2c564d3ec833d502b902b2001b8b81276a80a': {'etag': '"bf8582932deb747d5f2b94920d01cf4b"', 'full_R2_SHA_readback_verified': True, 'object_key': 'experimental/stitching-confident-repairs/20261009/model79-review/2f7a8c6309daa2dd0e8d365ecaa2c564d3ec833d502b902b2001b8b81276a80a.mp4', 'phase_id': 'model79-private-fourteen-review-20261009', 'relative': '/vid/terminal-heldout-20261009-batch241-case-1.mp4', 'sha256': '2f7a8c6309daa2dd0e8d365ecaa2c564d3ec833d502b902b2001b8b81276a80a', 'size_bytes': 60706}, '9badda4e35790cc564b90235682919d809af154a0ab1069ad2555399393370c3': {'etag': '"9fddfa8d8bee5013992c708cc93c76cc"', 'full_R2_SHA_readback_verified': True, 'object_key': 'experimental/stitching-confident-repairs/20261009/model79-review/9badda4e35790cc564b90235682919d809af154a0ab1069ad2555399393370c3.mp4', 'phase_id': 'model79-private-fourteen-review-20261009', 'relative': '/vid/terminal-heldout-20261009-batch242-case-1.mp4', 'sha256': '9badda4e35790cc564b90235682919d809af154a0ab1069ad2555399393370c3', 'size_bytes': 730315}, '267f9fe1a8932f619fd754deeeb56643700f4683d80814b03400194ab5e89958': {'etag': '"8e2da480f4e8af8e82f7900593c3c5d0"', 'full_R2_SHA_readback_verified': True, 'object_key': 'experimental/stitching-confident-repairs/20261009/model79-review/267f9fe1a8932f619fd754deeeb56643700f4683d80814b03400194ab5e89958.mp4', 'phase_id': 'model79-private-fourteen-review-20261009', 'relative': '/vid/terminal-heldout-20261009-batch243-case-1.mp4', 'sha256': '267f9fe1a8932f619fd754deeeb56643700f4683d80814b03400194ab5e89958', 'size_bytes': 68045}, '4633a0856bce71ef24f9da6106bdd319cc3fe3947dd07f94c944ed8fed466b7f': {'etag': '"acfb1c73c73322cd45de5fddd448e66f"', 'full_R2_SHA_readback_verified': True, 'object_key': 'experimental/stitching-confident-repairs/20261009/model79-review/4633a0856bce71ef24f9da6106bdd319cc3fe3947dd07f94c944ed8fed466b7f.mp4', 'phase_id': 'model79-private-fourteen-review-20261009', 'relative': '/vid/terminal-heldout-20261009-batch245-case-1.mp4', 'sha256': '4633a0856bce71ef24f9da6106bdd319cc3fe3947dd07f94c944ed8fed466b7f', 'size_bytes': 1356296}, '38161017df4b07ade9a49bf7883c2af10afefc602bba08017e2430f00b379ae7': {'etag': '"a2a09a0ee7349ddfff779cf667d7689b"', 'full_R2_SHA_readback_verified': True, 'object_key': 'experimental/stitching-confident-repairs/20261009/model79-review/38161017df4b07ade9a49bf7883c2af10afefc602bba08017e2430f00b379ae7.mp4', 'phase_id': 'model79-private-fourteen-review-20261009', 'relative': '/vid/terminal-heldout-20261009-batch246-case-1.mp4', 'sha256': '38161017df4b07ade9a49bf7883c2af10afefc602bba08017e2430f00b379ae7', 'size_bytes': 204932}, 'fed9bc470b58f18c8b3ef0213078b5fa86b92463f90911c22d6064b61401a170': {'etag': '"1285f4c5566722bba18a61c66fd8c83c"', 'full_R2_SHA_readback_verified': True, 'object_key': 'experimental/stitching-confident-repairs/20261009/model79-review/fed9bc470b58f18c8b3ef0213078b5fa86b92463f90911c22d6064b61401a170.mp4', 'phase_id': 'model79-private-fourteen-review-20261009', 'relative': '/vid/terminal-heldout-20261009-batch248-case-1.mp4', 'sha256': 'fed9bc470b58f18c8b3ef0213078b5fa86b92463f90911c22d6064b61401a170', 'size_bytes': 279345}, '570e4bc9a2746277724f741db4065f283752e8e390a75a0567f4c750e7e88bd5': {'etag': '"6a4d6fc27cc9ab3521707cd6868568a5"', 'full_R2_SHA_readback_verified': True, 'object_key': 'experimental/stitching-confident-repairs/20261009/model79-review/570e4bc9a2746277724f741db4065f283752e8e390a75a0567f4c750e7e88bd5.mp4', 'phase_id': 'model79-private-fourteen-review-20261009', 'relative': '/vid/terminal-heldout-20261009-batch249-case-1.mp4', 'sha256': '570e4bc9a2746277724f741db4065f283752e8e390a75a0567f4c750e7e88bd5', 'size_bytes': 580296}}

def records():
 data=json.loads(INDEX.read_text());assert data['version']==1;out={}
 for path,o in data['assets'].items():
  assert VIDEO.fullmatch(path) and SHA.fullmatch(o['sha256'])
  phase=o.get('phase_id',OLD_PHASE);assert isinstance(phase,str) and phase in PHASES
  assert o['object_key']==PHASES[phase][0]+o['sha256']+'.mp4'
  assert type(o['size_bytes'])is int and 0<o['size_bytes']<=134217728 and o['full_R2_SHA_readback_verified']is True
  assert isinstance(o['etag'],str)and re.fullmatch(r'"[A-Za-z0-9-]+"',o['etag'])
  out[path]=o
 return out
def available(path,sha):
 try:return records().get(path,{}).get('sha256')==sha
 except (OSError,ValueError,KeyError,TypeError,AssertionError):return False
def get(h,u,q):
 match=re.fullmatch('/api/review-preview-(media|ticket)/([a-f0-9]{64})',u.path)
 if not match:return False
 sha=match[2]
 if q!={'sha256':[sha]}:return delivery.send(h,400,{'error':'Exact verified preview SHA required'})
 origin=h.headers.get('Origin');host=h.headers.get('Host','')
 if origin and origin not in ('https://'+host,('http://'+host if host.split(':')[0]in('localhost','127.0.0.1','[::1]')else'')):return delivery.send(h,403,{'error':'Same-origin preview access required'})
 try:matches=[(path,o)for path,o in records().items()if o['sha256']==sha]
 except (OSError,ValueError,KeyError,TypeError,AssertionError):return delivery.send(h,503,{'error':'Verified preview registry unavailable'})
 if not matches:return delivery.send(h,404,{'error':'Verified private preview not registered'})
 path,o=matches[0];guard=PHASES[o.get('phase_id',OLD_PHASE)][1]
 try:
  sealed=WEEKEND_ASSETS.get(sha);weekend_guard=WEEKEND_GUARD;weekend_start=WEEKEND_START_EPOCH
  if sha in ELEVEN_ASSETS:
   sealed=ELEVEN_ASSETS[sha];weekend_guard=ELEVEN_GUARD;weekend_start=ELEVEN_START_EPOCH
  if time.time()>=weekend_start and sealed is not None:
   assert path==sealed['relative'] and all(o.get(k)==sealed[k] for k in ('sha256','size_bytes','etag','object_key','phase_id','full_R2_SHA_readback_verified'))
   guard=weekend_guard
   claim={'action':'reserve_ticket','ticket':'preview-weekend-'+uuid.uuid4().hex,'sha256':o['sha256'],'size_bytes':o['size_bytes'],'object_key':o['object_key']}
  else:
   claim={'action':'reserve_requests','ticket':'preview-media-head-'+uuid.uuid4().hex,'class_A':0,'class_B':1}
  result=subprocess.run(['ssh','-o','BatchMode=yes','-o','StrictHostKeyChecking=yes','root@192.81.214.16',guard],input=json.dumps(claim),capture_output=True,text=True,timeout=20)
  assert result.returncode==0 and json.loads(result.stdout)['allowed']is True
 except Exception:return delivery.send(h,503,{'error':'Finite preview request allowance unavailable'})
 # Existing signer performs a fresh exact ETag/size HEAD and issues a600s one-object
 # HTTPS capability. Browser media follows302; no video bytes transit Jelly.
 if match[1]=='media':return delivery.ticket(h,dict(o,available=True,path=path.lstrip('/')),q,browser=True)
 class Capture:
  def __init__(self):self.headers=h.headers;self.wfile=io.BytesIO();self.response_headers={};self.status=None
  def send_response(self,status):self.status=status
  def send_header(self,key,value):self.response_headers[key]=value
  def end_headers(self):pass
 response=Capture();delivery.ticket(response,dict(o,available=True,path=path.lstrip('/')),q,browser=True)
 if response.status!=302:return delivery.send(h,response.status or 503,response.wfile.getvalue() or b'{"error":"Preview ticket unavailable"}')
 try:
  url=response.response_headers['Location'];parsed=urlsplit(url);params=parse_qs(parsed.query)
  assert parsed.scheme=='https' and parsed.hostname and parsed.hostname.endswith('.r2.cloudflarestorage.com') and not parsed.username and not parsed.password
  assert unquote(parsed.path).endswith('/'+o['object_key'])
  ttl=int(params['X-Amz-Expires'][0]);assert 0<ttl<=600
  expires=datetime.datetime.strptime(params['X-Amz-Date'][0],'%Y%m%dT%H%M%SZ').replace(tzinfo=datetime.timezone.utc).timestamp()+ttl
  assert time.time()<expires<=time.time()+601
 except (AssertionError,KeyError,ValueError,TypeError):return delivery.send(h,503,{'error':'Verified preview capability unavailable'})
 return delivery.send(h,200,{'url':url,'sha256':o['sha256'],'size_bytes':o['size_bytes'],'etag':o['etag'],'expires_at':expires,'transport':'r2-direct'})
