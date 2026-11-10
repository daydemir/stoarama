"""Read-only queue across version-bound candidate collections."""
import json,re,time
from pathlib import Path
from urllib.parse import urlencode
from review_comparison import validated_model_comparison

PREFERENCES = Path('/Users/jelly/.stoarama/seam-review/private/review-queue-preferences.json')

def preferences(path=PREFERENCES):
 if not path.exists():return {'defer_streams': [], 'algorithm_priority': []}
 if path.is_symlink() or not path.is_file() or path.stat().st_size>16384:raise ValueError('Invalid queue preferences')
 data=json.loads(path.read_text())
 if not isinstance(data,dict) or set(data)-{'defer_streams','algorithm_priority','reason','priority_algorithm','not_before_collections'}:raise ValueError('Invalid queue preferences')
 ids=data.get('defer_streams',[]);priority=data.get('algorithm_priority',[])
 if not isinstance(ids,list) or len(ids)>49 or any(type(i) is not int or i<=0 for i in ids):raise ValueError('Invalid deferred streams')
 if not isinstance(priority,list) or len(priority)>16 or any(not isinstance(v,str) or not v or len(v)>128 for v in priority):raise ValueError('Invalid algorithm priority')
 if 'priority_algorithm' in data and (not isinstance(data['priority_algorithm'],str) or not data['priority_algorithm'] or len(data['priority_algorithm'])>128):raise ValueError('Invalid priority algorithm')
 delayed=data.get('not_before_collections',{})
 if not isinstance(delayed,dict)or len(delayed)>64 or any(not re.fullmatch(r'terminal-heldout-20261009-batch[0-9]{2,4}',k)or type(v)is not int or not 1791504000<=v<=1791676800 for k,v in delayed.items()):raise ValueError('Invalid scheduled review collections')
 if 'reason' in data and (not isinstance(data['reason'],str) or len(data['reason'])>500):raise ValueError('Invalid queue reason')
 return data

def snapshot(reviewers, prefs=None, now=None):
 prefs=preferences() if prefs is None else prefs
 now=time.time()if now is None else now
 pending=[];deferred=[];reviewed=total=0
 priority=prefs.get('algorithm_priority',[])
 for r in reviewers:
  if not r.config:continue
  votes=r.votes()
  for c in r.config['cases']:
   total+=1;v=votes.get(c['id'],{})
   if v.get('label_source')=='explicit_human_choice' and v.get('human_label') in ('join','split','uncertain'):
    reviewed+=1;continue
   matrix=validated_model_comparison(c)
   rank=4
   if matrix and matrix.get('V7.9',{}).get('decision')=='join':
    rank=0 if matrix.get('V7.5',{}).get('decision')=='split' else 1 if matrix.get('V7',{}).get('decision')=='split' else 2 if matrix.get('V5.5',{}).get('decision')=='split' else 3
   if matrix and matrix.get('V7.99',{}).get('decision')=='join' and matrix.get('V7.9',{}).get('decision')=='split':rank=-1
   if matrix and matrix.get('Model49',{}).get('decision')=='join' and matrix.get('V7.999',{}).get('decision')=='split':rank=-3
   if matrix and matrix.get('Model59',{}).get('decision')=='join' and matrix.get('Model49',{}).get('decision')=='split':rank=-4
   if matrix and matrix.get('Model55',{}).get('decision')=='join' and matrix.get('Model49',{}).get('decision')=='split':rank=-5
   if matrix and matrix.get('Model69',{}).get('decision')=='join' and matrix.get('Model49',{}).get('decision')=='split':rank=-6
   row={'_comparison_priority':rank,'version_disagreement':bool(matrix and c.get('version_disagreement')),'collection':r.id,'id':c['id'],'title':c.get('title',c['id']),'algorithm':r.target(c)['algorithm'],'revision':r.config['revision'],'url':'/candidate/'+r.id+'?'+urlencode({'id':c['id'],'queue':1})}
   recording_id=c.get('recording_id')
   if type(recording_id) is int:row['recording_id']=recording_id
   exact_manual_delta = recording_id == 427 and matrix and ((matrix.get('V7.9',{}).get('decision')=='join' and matrix.get('V7.5',{}).get('decision')=='split') or (matrix.get('V7.99',{}).get('decision')=='join' and matrix.get('V7.9',{}).get('decision')=='split'))
   exact_manual_delta = exact_manual_delta or bool(recording_id==427 and matrix and matrix.get('Model49',{}).get('decision')=='join' and matrix.get('V7.999',{}).get('decision')=='split')
   exact_manual_delta=exact_manual_delta or bool(recording_id==427 and matrix and matrix.get('Model59',{}).get('decision')=='join' and matrix.get('Model49',{}).get('decision')=='split')
   exact_manual_delta=exact_manual_delta or bool(recording_id==427 and matrix and any(matrix.get(label,{}).get('decision')=='join' for label in ('Model55','Model69')) and matrix.get('Model49',{}).get('decision')=='split')
   release=prefs.get('not_before_collections',{}).get(r.id)
   if release is not None and now<release:deferred.append(row);continue
   morning_released=release is not None and now>=release and row['algorithm']=='v7p999-one-sided-tail-frontier-review-20261009'
   if morning_released:row['_comparison_priority']=-2
   if prefs.get('priority_algorithm') and row['algorithm']!=prefs['priority_algorithm']and not morning_released:deferred.append(row);continue
   if recording_id in prefs.get('defer_streams',[]) and not c.get('control_only',False) and not exact_manual_delta:deferred.append(row)
   else:pending.append(row)
 pending.sort(key=lambda row:(row['_comparison_priority'],0 if row['version_disagreement'] else 1,priority.index(row['algorithm']) if row['algorithm'] in priority else len(priority)))
 for row in pending+deferred:row.pop('_comparison_priority',None)
 return {'pending':pending,'deferred':deferred,'deferred_count':len(deferred),'reviewed':reviewed,'total':total,'queue_preference_reason':prefs.get('reason','')}

def get(h,u,q,reviewers,page):
 if u.path not in ('/review-queue','/api/review-queue'):return False
 try:state=snapshot(reviewers)
 except (OSError,ValueError,TypeError):h.send(503,'Queue preferences unavailable; existing reviews are preserved.');return True
 if u.path=='/api/review-queue':h.send(200,json.dumps(state),'application/json');return True
 body="""<h1>Uncertain seam review queue</h1><p>New cases come from the current look-ahead analysis. Each choice is bound to its source evidence and algorithm version.</p><p id='queue-status' role='status'>Checking for a fresh case…</p><p><a href='/vid/stitching-coverage/lookahead/'>All-stream stitching status</a> · <a href='/vid/jaworzno-reviewed-repairs-20261009/'>Reviewed Jaworzno files</a></p><button onclick='checkQueue()'>Check now</button><p>Keys: 1 Join · 2 Split · 3 Can’t tell. A confirmed saved choice advances to the next unreviewed case. Videos default to 2×; borders show blue before, red at the seam and orange after.</p>"""
 js="""let queueBusy=false;async function checkQueue(){if(queueBusy)return;queueBusy=true;try{const r=await fetch('/api/review-queue',{cache:'no-store'});if(!r.ok)throw Error('Queue check failed');const d=await r.json();if(d.pending.length){location.replace(d.pending[0].url);return}document.getElementById('queue-status').textContent=d.deferred_count?'Preparing the next fresh case. '+d.deferred_count+' cases are temporarily deferred for the selected review priority; their reviews and direct links are preserved.':'All '+d.reviewed+' published cases are reviewed. Preparing the next fresh batch; this page checks again automatically.';}catch(e){document.getElementById('queue-status').textContent=e.message+'; retrying.'}finally{queueBusy=false}}checkQueue();setInterval(checkQueue,5000);"""
 h.send(200,page('Uncertain seam queue',body,js));return True
