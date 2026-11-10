"""Isolated diagnostic canary using the existing version-bound reviewer engine."""
import hashlib,html,json,os
from candidate_review import CandidateReview,COMPACT_CSS,HUMAN_JS,PLAYBACK_JS,decision_badge
from urllib.parse import urlencode

class Window20Review(CandidateReview):
 def __init__(self,root,page,slide_js,frame_js,filename="candidate-window20-427-20261008.json",expected_id="window20-427-20261008",vote_endpoint="/api/window20-vote"):
  self.root,self.page,self.frame_js=root,page,frame_js
  self.path=root/filename
  self.config=json.loads(self.path.read_text()) if self.path.exists() else None
  self.slide_js=slide_js[:slide_js.index('async function vote(verdict){')]
  self.slide_js=self.slide_js.replace('function votePayload(verdict){const body={id:ITEM,verdict,','function votePayload(label){const body={id:ITEM,label,')+HUMAN_JS.replace("fetch('/api/candidate-vote'","fetch('"+vote_endpoint+"'")
  if self.config:
   self.id=self.config['id'];assert self.id==expected_id
   self.cases={c['id']:c for c in self.config['cases']}
   evidence=root/'comparison-evidence'/self.config['evidence_filename']
   assert hashlib.sha256(evidence.read_bytes()).hexdigest()==self.config['evidence_sha256']
   for c in self.cases.values():
    media=root/c['video']
    if media.is_file():assert hashlib.sha256(media.read_bytes()).hexdigest()==c['preview_sha256']
    else:
     import review_asset_r2
     assert review_asset_r2.available('/'+c['video'].lstrip('/'),c['preview_sha256']),'Verified preview unavailable'
   self.database=root/'candidate-reviews'/self.id/'review.db'
   self.database.parent.mkdir(parents=True,exist_ok=True)
   with self.db():pass
   os.chmod(self.database.parent,0o700);os.chmod(self.database,0o600)
 def target(self,c):
  return dict(super().target(c),algorithm='collation-v3-window20-diagnostic')
 def post(self,h):
  import coverage_reviews
  if not coverage_reviews.same_origin(h):return h.send(403,'Same-origin review required')
  return super().post(h)
 def sampling_summary(self,votes):
  groups=self.config['sampling']['groups'];total=sum(groups.values());parts=[];low=high=0;complete=True;estimated=0;proposed=0;proposal_count=0
  eager=conservative=agreement=unknown_count=0
  for case in self.config['cases']:
   label=votes.get(case['id'],{}).get('human_label');proposal='join' if case['claim']=='continuous' else 'split'
   if label not in ('join','split'):unknown_count+=1
   elif label==proposal:agreement+=1
   elif proposal=='join':eager+=1
   else:conservative+=1
  for group,population in groups.items():
   cases=[c for c in self.config['cases'] if c.get('sampling_group')==group]
   labels=[votes.get(c['id'],{}).get('human_label') for c in cases]
   yes=sum(c['claim']=='continuous' for c in cases);proposed+=population*yes/len(cases);proposal_count+=yes
   joins=labels.count('join');splits=labels.count('split');unknown=len(labels)-joins-splits
   parts.append(f"{html.escape(group.replace('_',' '))}: {joins} Join / {splits} Split / {unknown} unknown")
   low+=population*joins/len(labels);high+=population*(joins+unknown)/len(labels);estimated+=population*joins/len(labels);complete=complete and unknown==0
  result=f'<p>The20s diagnostic proposes {proposal_count} joins in12 sampled cases. Weighted by the group sizes, that suggests roughly {round(proposed)} additional proposals among {total} boundaries. Sampling uncertainty is large; proposals are not verified safe joins.</p>'
  result+=f'<p><strong>20s diagnostic vs your checks:</strong> {eager} overly eager join proposals · {conservative} overly conservative splits · {agreement} agreements · {unknown_count} uncertain/unreviewed.</p>'
  result+='<p><strong>Population estimate from this12-case audit</strong><br>'+'<br>'.join(parts)+'</p>'
  if complete:result+=f'<p>Weighted preliminary estimate: about {round(estimated)} of {total} unreviewed visual splits acceptable to join. Only three cases per group; sampling uncertainty is large. This is a planning estimate, not an automatic join list.</p>'
  else:result+=f'<p>Sample-implied range so far: {round(low)}–{round(high)} of {total}, because unreviewed/uncertain samples remain unknown. This is not a confidence interval or permission to join those boundaries.</p>'
  return result
 def render(self,q):
  esc=html.escape;votes=self.votes();all_cases=self.config['cases'];current=q.get('id',[''])[0] or all_cases[0]['id'];c=self.cases[current];pos=next(i for i,v in enumerate(all_cases) if v['id']==current)
  route='/candidate/'+self.id
  next_canary="<a class='navb' href='/candidate/jaworzno-uncertain-20261009'>Next uncertain seams</a>" if self.id!='jaworzno-uncertain-20261009' else "<a class='navb' href='/candidate/window20-427-20261008'>Prior checks</a>"
  repair_link="<a class='navb' href='/vid/jaworzno-reviewed-repairs-20261009/'>Reviewed repairs</a>"
  def link(i='',**kw):return route+'?'+urlencode({'id':i,**kw})
  prev=link(all_cases[pos-1]['id']) if pos else '';nxt=link(all_cases[pos+1]['id']) if pos+1<len(all_cases) else ''
  speed={'0.5':.5,'1':1,'2':2}.get(q.get('speed',['2'])[0],2);target=self.target(c);f=c['frame_preview'];v= votes.get(current,{});label=v.get('human_label','')
  nav=(f"<a class='navb' href='{prev}'>← Prev</a>" if prev else '')+(f"<a class='navb' href='{nxt}'>Next →</a>" if nxt else '')
  rows=''.join(f"<tr><th>{title}</th><td>{decision_badge(r['decision'])}</td><td>{esc(r['reason'])}</td></tr>" for title,r in [('V2 · recorded',c['v2']),('V3 · fresh',c['current_v3']),('20 s · diagnostic',c['candidate'])])
  choices=''.join(f"<button class='human-choice choice-{k}{' on' if label==k else ''}' data-label='{k}' onclick=\"vote('{k}')\" aria-pressed='{str(label==k).lower()}'>{t}</button>" for k,t in [('join','Join'),('split','Split'),('uncertain','I can’t tell')])
  warning=''
  if c['capture_gap_seconds']>0:warning=f"<p><strong>Real capture gap: {c['capture_gap_seconds']:.3f} s.</strong> Preview places boundary frames together; it is not a continuous timeline.</p>"
  if c['id'].endswith('accepted_control'):warning+='<p>V2 accepted this seam; current V3 and the diagnostic split it. This is a regression control, not a proposed replacement.</p>'
  joined=self.config.get('joined_candidate',{}).get('relative')
  joined_link=f" · <a href='/{joined}' download>Experimental packet-copy candidate (514093 → 514132)</a>" if joined else ''
  joined_note='<p>The candidate preserves all encoded source packets in order, strictly decodes, and has an R2 receipt. It is not part of the production download collection.</p>' if joined else ''
  batch_title=esc(self.config.get('title','Jaworzno · 20 s window canary'))
  sampling=''
  if self.config.get('sampling'):
   group=c['sampling_group'];sampling=f"<p>Sample group: {esc(group.replace('_',' '))} · {c['population']} unreviewed boundaries in this group. Three were selected uniformly at random. Your labels will inform a weighted estimate; this does not certify the rest.</p>"
   sampling+=self.sampling_summary(votes)
  body=f"""<style>{COMPACT_CSS}.candidate-header a{{min-height:44px;display:inline-flex;align-items:center}}.compact-sidebar p{{overflow-wrap:anywhere}}.candidate-comparison table{{width:100%}}.note{{width:100%}}.compact-sidebar input.note{{height:44px}}</style>
<header class='candidate-header'><h1>{batch_title}</h1><div class='bar'><span>{pos+1} / {len(all_cases)} · {len(votes)} reviewed</span>{nav}{next_canary}{repair_link}<a class='navb' href='/vid/stitching-coverage/?order=delivery&amp;stream=427'>Stream coverage</a></div></header>
<article class='card candidate-workspace' data-human-label='{esc(label)}'><section class='media-column'><div class='vwrap'><video data-preload-src='/{esc(c['video'])}' muted autoplay loop playsinline preload='auto' aria-label='Source seam preview'></video><img class='frame-still' hidden alt='Exact decoded preview frame'></div>
<div class='playback-controls'><button id='video-play' disabled>Play</button><input id='video-seek' type='range' min='0' max='10' step='0.01' disabled aria-label='Seek preview'><output id='video-time'>0:00 / 0:10</output><button id='video-mute'>Unmute</button></div>
<div class='phasebar'><span class='phase'>BEFORE</span></div><section class='frame-controls' aria-label='Exact frame review'><div class='frame-toolbar'><button id='frame-previous' disabled>Previous frame</button><button id='frame-next' disabled>Next frame</button><button id='frame-seam' disabled>Jump to seam</button><button id='frame-resume' disabled>Resume video</button></div><button id='frame-retry' hidden>Retry preload</button><progress id='frame-progress' value='0' max='{f['max_frame']-f['min_frame']+1}' aria-label='Exact frames loaded'></progress><p id='frame-status' class='frame-status' role='status'>Loading clip and seam frames…</p><div class='frame-range-row'><label for='frame-range'>Frames</label><input id='frame-range' type='range' disabled min='{f['min_frame']}' max='{f['max_frame']}' value='{f['seam_frame_index']}' aria-label='Browse exact seam frames'></div></section></section>
<section class='meta compact-sidebar'><p class='title'>{esc(c['title'])}</p><p>Sources {' → '.join(map(str,c['clip_ids']))} · seam {esc(c['local_seam'])} local.</p><p>Preview seam at 5 s. Check motion, then exact frames. Keys: 1 Join · 2 Split · 3 Can’t tell · , / . frames · j seam.</p>{warning}{sampling}<p class='human-question'><strong>Your seam decision</strong></p><div class='btns human-choices'>{choices}</div><label for='review-note'>Comment (optional)</label><input id='review-note' class='note' maxlength='1000' value='{esc(v.get('note',''),quote=True)}' onchange='saveNote()'><span id='saved' class='save-message' role='status'></span><button id='save-retry' hidden onclick='retryReview()'>Retry same choice</button><button class='undo-choice' onclick=\"vote('clear')\">Undo choice</button>
<p>Experimental review. Your choice is bound to source hashes, preview, evidence and revision. Existing outputs and votes stay intact.</p><div class='comparison candidate-comparison'><table><thead><tr><th>Algorithm</th><th>Decision</th><th>Reason</th></tr></thead><tbody>{rows}</tbody></table></div>
<details><summary>Evidence and limitations</summary><p>Both originals were fetched from R2 and checked against their recorded SHA-256. This 10 s HD preview is re-encoded for inspection; frames are exact decoded frames of that preview. It is not a lossless source-frame proof. No H.264 HRD continuity proof is available.</p><p>Only the visual window changed to 20 s. Codec, timing, identity and ambiguity gates were preserved. The wider window changes the motion mask as well as its baseline; it can still reject valid joins. No false-positive guarantee is claimed.</p><p><a href='/evidence/{self.config['evidence_filename']}'>Hash-bound evidence</a>{joined_link}</p>{joined_note}<p>Preview speed: {' · '.join(f"<a href='{link(current,speed=s)}'>{s}×</a>" for s in [.5,1,2])}</p></details></section></article>"""
  js=f"const ITEM={json.dumps(current)},PREV={json.dumps(prev)},NEXT={json.dumps(nxt)},SPEED={speed},PRIORITY=false,RESUME={json.dumps(route)},REVIEW_TARGET={json.dumps(target)},FRAME_PREVIEW={json.dumps(f)},REFERENCE_ONLY=false;"+self.slide_js+self.frame_js+PLAYBACK_JS
  return self.page('Jaworzno seam canary',body,js)
