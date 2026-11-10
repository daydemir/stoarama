"""Read-only live derivative accounting. Receipts must match private delivery identities."""
from pathlib import Path
from datetime import datetime,timezone
import json,re
PRIVATE=Path('/Users/jelly/.stoarama/seam-review/private')
ROOT=Path(__file__).resolve().parent
SHA=re.compile('[0-9a-f]{64}')
def snapshot(data,root=ROOT,private=PRIVATE):
 by_id={int(o['id']):o for o in data['outputs']};rows=[];conflicts=[];used=set()
 for kind,folder,indexfile in [('reviewed','jaworzno-reviewed-repairs-20261009','jaworzno-reviewed-repairs-20261009.json'),('strict','confident-repairs-20261009','confident-repairs-20261009.json')]:
  try:
   receipts=json.loads((root/'vid'/folder/'receipts.json').read_text());raw=json.loads((private/indexfile).read_text());index=raw['outputs'] if isinstance(raw,dict) else raw;bound={o['sha256']:o for o in index}
  except (OSError,ValueError,KeyError,TypeError):continue
  for r in receipts.get('repairs',[]):
   sha=r.get('output_sha256','');o=bound.get(sha);ids=[int(v['id']) for v in r.get('input_outputs',[])] or r.get('replacement_inputs',[])
   if not SHA.fullmatch(sha) or not o or not o.get('available') or o.get('size_bytes')!=r.get('size_bytes') or not all(r.get(v) is True for v in ('r2_verified','strict_decode_ok','packet_hash_sequence_preserved')):continue
   if not ids or any(v not in by_id for v in ids):continue
   expected=[v['sha256'] for v in r.get('input_outputs',[])] or r.get('input_output_sha256',[])
   if expected!=[by_id[v]['sha256'] for v in ids]:continue
   stream=int(r.get('recording_id',receipts.get('recording_id',0)))
   if any(by_id[v]['recording_id']!=stream for v in ids):continue
   rows.append(dict(sha256=sha,recording_id=stream,local_date=r['local_date'],hour=r['hour'],seconds=r['joined_seconds'],size_bytes=r['size_bytes'],input_ids=ids,input_parts=len(ids),parts_reduced=len(ids)-1,collections=[c for c in ('audited','best','available') if all(c in by_id[v].get('collections',[by_id[v].get('scope','audited')]) for v in ids)],kind=('reviewed' if r.get('evidence_class')=='human-reviewed' or r.get('case','').startswith('human-union-') or any(p.get('human_label')=='join' for p in r.get('seam_proofs',[])) else kind),path=o['path'],file_url='/api/'+('reviewed-repairs' if kind=='reviewed' else 'confident-repairs')+'/file/'+sha))
 candidates=rows;rows=[]
 for row in sorted(candidates,key=lambda r:(-len(r['input_ids']),r['kind']!='reviewed',r['sha256'])):
  if used.intersection(row['input_ids']):conflicts.append(row['sha256']);continue
  used.update(row['input_ids']);rows.append(row)
 return dict(verified_assembly_objects=len(candidates),updated_at=datetime.now(timezone.utc).isoformat(),repairs=rows,conflicting_replacements=len(conflicts),note='Verified assembled media; does not certify perfect stitching. Original outputs and audit remain intact.')
def presentation_paths(outputs,by_id):
 """Name only the current source-once partition; never alter stored objects."""
 groups={}
 for o in outputs:
  inputs=o.get('replacement_inputs',[o['id']]);sources=[by_id[i]for i in inputs]
  slots={(x['local_date'],x['delivery_hour'])for x in sources}
  if len(slots)!=1:raise ValueError('Replacement crosses a download hour')
  first=min(sources,key=lambda x:(x['part'],x['id']));match=re.fullmatch(r'(.*_hour_[0-9]{2})(?:_part_[0-9]+)?\.mp4',first['path'])
  if not match:raise ValueError('Original naming evidence missing')
  groups.setdefault(next(iter(slots)),[]).append((first['part'],first['id'],o,match[1]))
 for rows in groups.values():
  rows.sort(key=lambda x:(x[0],x[1]));bases={r[3]for r in rows}
  if len(bases)!=1:raise ValueError('Original hour naming evidence differs')
  for number,(_,_,o,base)in enumerate(rows,1):
   new=base+(''if len(rows)==1 else '_part_'+str(number).zfill(2))+'.mp4'
   if new!=o['path']:o['path']=new;o['ticket_presentation_path']=True
 return outputs

def ticket_alias(output,path,data=None,root=ROOT,private=PRIVATE):
 import coverage_downloads
 if not isinstance(path,str)or len(path)>1000 or any(x in path for x in ('\\','\x00','..')):raise ValueError('Unsafe presentation alias')
 data=coverage_downloads.data()if data is None else data
 stream=output.get('recording_id')
 if not stream:
  m=re.match(r'([1-9][0-9]*)_',output['path']);stream=int(m[1])if m else None
 if not stream:raise ValueError('Output stream missing')
 for scope in ('available','best','audited'):
  try:m=delivery_manifest(data,stream,scope,root,private)
  except (KeyError,ValueError):continue
  if any(o['sha256']==output['sha256']and o['size_bytes']==output['size_bytes']and o['path']==path for o in m['outputs']):return dict(output,path=path)
 raise ValueError('Unknown output presentation alias')

def delivery_manifest(data,stream,scope,root=ROOT,private=PRIVATE):
 import coverage_downloads,unicodedata
 if scope not in ('best','available','audited'):raise ValueError('Unknown delivery collection')
 period=coverage_downloads.periods(data,scope,str(stream))[str(stream)]
 originals=[coverage_downloads.public(o) for o in data['outputs'] if o['recording_id']==stream and scope in o.get('collections',[o['scope']])]
 ids={o['id'] for o in originals};repairs=[r for r in snapshot(data,root,private)['repairs'] if r['recording_id']==stream and set(r['input_ids']).issubset(ids)]
 replaced={v for r in repairs for v in r['input_ids']};outputs=[o for o in originals if o['id'] not in replaced]
 for r in repairs:outputs.append(dict(id='sha256:'+r['sha256'],sha256=r['sha256'],size_bytes=r['size_bytes'],path=r['path'],file_url=r['file_url'],available=True,local_date=r['local_date'],recording_id=stream,replacement_inputs=r['input_ids']))
 outputs=presentation_paths(outputs,{int(o['id']):o for o in data['outputs']})
 paths=[unicodedata.normalize('NFC',o['path']).casefold() for o in outputs]
 if len(paths)!=len(set(paths)):raise ValueError('Colliding delivery paths')
 return dict(schema=2,collection='verified-assembled',recording_id=stream,period=[period['start'],period['end']],original_count=len(originals),replaced_original_count=len(replaced),replacement_count=len(repairs),outputs=outputs)
def get(h,u,q):
 if u.path not in ('/api/coverage-growth','/api/coverage-delivery/manifest'):return False
 import coverage_downloads
 try:
  if u.path=='/api/coverage-delivery/manifest':
   stream=(q.get('stream') or [''])[0];scope=(q.get('scope') or ['available'])[0]
   if len(q.get('stream',[]))!=1 or not re.fullmatch('[1-9][0-9]{0,18}',stream) or any(k not in ('stream','scope') for k in q):return coverage_downloads.send(h,400,{'error':'Choose one cohort stream and collection'})
   return coverage_downloads.send(h,200,delivery_manifest(coverage_downloads.data(),int(stream),scope))
  return coverage_downloads.send(h,200,snapshot(coverage_downloads.data()))
 except (OSError,ValueError,KeyError,TypeError):return coverage_downloads.send(h,503,{'error':'Verified delivery accounting unavailable'})
