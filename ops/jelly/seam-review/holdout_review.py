"""Separate version-bound reviewer for a review-only terminal rescue candidate."""
from window20_review import Window20Review
import html,re,json
from review_comparison import validated_model_comparison
from review_aliases import algorithm_label,model_label
EXACT_PHASE_JS="""
function updateSourcePhase(time){
 if(typeof frameViewIsActive==='function'&&frameViewIsActive())return;
 if(typeof frameManifest==='undefined'||!frameManifest){p.textContent='VERIFYING SEAM';p.style.background='var(--blue)';return;}
 const i=frameManifest.seam_frame_index,at=Number(frameManifest.frames[i].pts),next=frameManifest.frames[i+1]?Number(frameManifest.frames[i+1].pts):Infinity;
 const label=time<at?'BEFORE':time<next?'SEAM':'AFTER';
 p.textContent=label;p.style.background=label==='BEFORE'?'var(--blue)':label==='SEAM'?'var(--red)':'var(--orange)';
}
if(typeof v.requestVideoFrameCallback==='function'){
 const phaseFrame=(now,metadata)=>{updateSourcePhase(metadata.mediaTime);v.requestVideoFrameCallback(phaseFrame)};
 v.requestVideoFrameCallback(phaseFrame);
}else v.addEventListener('timeupdate',()=>updateSourcePhase(v.currentTime));
"""
class HoldoutReview(Window20Review):
 def __init__(self,root,page,slide_js,frame_js,filename='candidate-terminal-heldout-20261009.json',expected_id='terminal-heldout-20261009',vote_endpoint='/api/terminal-heldout-vote'):
  super().__init__(root,page,slide_js,frame_js,filename=filename,expected_id=expected_id,vote_endpoint=vote_endpoint)
  self.slide_js=self.slide_js.replace("preview_variant:'hd',preview_sha256:FRAME_PREVIEW.source_sha256","preview_variant:REVIEW_TARGET.preview_variant,preview_sha256:FRAME_PREVIEW.source_sha256")
 def target(self,c):
  target=dict(super().target(c),algorithm=self.config.get('algorithm','terminal-only-review-v1'))
  for field in ('parameter_variant','parameters','blind_review','preview_variant'):
   if field in self.config:target[field]=self.config[field]
  for field in ('control_only','builder_eligible'):
   if field in c:target[field]=c[field]
  return target
 def render(self,q):
  page=super().render(q)
  current=q.get('id',[''])[0] or self.config['cases'][0]['id'];c=self.cases[current]
  import review_preview_r2
  media_path='/'+c['video'].lstrip('/')
  if review_preview_r2.available(media_path,c['preview_sha256']):
   ticket='/api/review-preview-ticket/'+c['preview_sha256']+'?sha256='+c['preview_sha256']
   page=page.replace("data-preload-src='"+html.escape(media_path,quote=True)+"'","data-r2-direct='true' data-preload-src='"+ticket+"'")
   page=page.replace("<div class='playback-controls'>","<p><a id='review-open-video' aria-disabled='true' target='_blank' rel='noopener noreferrer'>Open R2 video</a></p><div class='playback-controls'>")
  elif self.config.get('preview_transport')=='r2-direct':
   raise ValueError('Required R2 preview receipt unavailable')
  alias=algorithm_label(self.config.get('algorithm'),c)
  if alias:
   page=page.replace('<h1>'+html.escape(self.config.get('title',''))+'</h1>','<h1>'+html.escape(alias)+' · seam audit</h1>')
  page=page.replace('Preview seam at 5 s.', 'Preview seam at '+format(c.get('preview_seam_seconds',5),'.3f')+' s; exact seam frame is verified from source trim counts.')
  page=re.sub(r"v.addEventListener\('timeupdate',\(\)=>\{if\(typeof frameViewIsActive.*?\}\);",EXACT_PHASE_JS,page,count=1,flags=re.S)
  page=page.replace('V3 · fresh','Strict 20 s · fresh').replace('20 s · diagnostic','Terminal rule · review only')
  page=page.replace('Only the visual window changed to 20 s. Codec, timing, identity and ambiguity gates were preserved. The wider window changes the motion mask as well as its baseline; it can still reject valid joins. No false-positive guarantee is claimed.','This review-only candidate replays fresh 20-second evidence with a relaxed terminal-frame rule. Its thresholds were tuned on prior Jaworzno checks; these other-stream examples are unseen checks. It is not enabled for automatic joins. All source hashes and strict comparator evidence are preserved. Matcher offsets are not measured lost frames. No false-positive guarantee is claimed.')
  page=page.replace("href='/vid/stitching-coverage/?order=delivery&amp;stream=427'", "href='/vid/stitching-coverage/?order=delivery&amp;stream="+str(c['recording_id'])+"'")
  page=page.replace("<p class='human-question'>", "<p>Targeted challenge: "+html.escape(c['selection_reason'])+". Please check for a real jump or replay; a few lost frames may be acceptable.</p><p class='human-question'>")
  page=page.replace('<title>Jaworzno seam canary</title>','<title>Other streams · terminal rule audit</title>')
  if self.config.get('algorithm','terminal-only-review-v1')!='terminal-only-review-v1':
   match=re.match(r'^v(\d+(?:p\d+)?)(?:-|$)',self.config.get('algorithm',''));version=((alias or 'V'+match[1].replace('p','.'))+(' head-only stress' if match[1]=='5' else ' tuning')) if match else (alias+' tuning' if alias else 'Experimental tuning')
   page=page.replace('Terminal rule · review only',version+' · review only').replace('Other streams · terminal rule audit','Other streams · '+version+' audit')
   page=page.replace('This review-only candidate replays fresh 20-second evidence with a relaxed terminal-frame rule. Its thresholds were tuned on prior Jaworzno checks; these other-stream examples are unseen checks. It is not enabled for automatic joins. All source hashes and strict comparator evidence are preserved. Matcher offsets are not measured lost frames. No false-positive guarantee is claimed.',html.escape(self.config.get('review_description','This is a separate review-only V4 parameter experiment. It is not enabled for automatic joins. Source hashes, native frame proofs and experiment parameters are bound to your review.')))
  if self.config.get('blind_review') and not self.votes().get(current,{}).get('human_label'):
   page=re.sub(r"<div class='comparison candidate-comparison'>.*?</div>","<p class='blind-review'>Blind review: predictions appear after saving. Check motion and exact frames.</p>",page,flags=re.S)
   page=re.sub(r'<p>Targeted challenge: .*?</p>','',page,flags=re.S)
   page=re.sub(r'<p><strong>Real capture gap: .*?</p>','',page,flags=re.S)
  elif c.get('control_explanation_after_vote') and self.votes().get(current,{}).get('human_label'):
   explanation="<p class='control-explanation'>"+html.escape(str(c['control_explanation_after_vote']))+"</p>"
   page=page.replace("<p class='human-question'>",explanation+"<p class='human-question'>")
  if self.config.get('default_preview_quality')=='480p':page=page.replace('This 10 s HD preview is re-encoded for inspection; frames are exact decoded frames of that preview.','The default 480p preview retains source frame cadence with resized pixels. Frame controls show exact decoded frames of this preview. Source-resolution evidence is retained in the private proof.')
  if c.get('full_preview'):
   full=c['full_preview'];link="<p><a href='/"+html.escape(full['video'],quote=True)+"' target='_blank' rel='noopener'>Full-resolution video</a> (loads on demand; review stays bound to the default preview).</p>"
   native=c.get('native_frame_preview')
   if native:link+="<p><a href='/frame-review/"+full['sha256']+"/frame-"+native['seam_image_sha256']+".png' target='_blank' rel='noopener'>Native seam frame</a></p>"
   page=page.replace("<p class='human-question'>",link+"<p class='human-question'>")
  matrix=validated_model_comparison(c)
  if matrix and self.votes().get(current,{}).get('human_label'):
   rows=''.join('<tr><th>'+html.escape(model_label(label,matrix[label]))+'</th><td>'+html.escape(matrix[label]['decision'].capitalize())+'</td><td><code>'+html.escape(matrix[label]['implementation_sha256'][:12])+'</code></td></tr>' for label in tuple(label for label in ('V7','V7.5','V7.9','V5.5','V7.99','V7.999','Model49','Model55','Model59','Model69','Model79','Model89')if label in matrix))
   comparison="<details class='same-source-comparison'><summary>Algorithm comparison</summary><p>Same SHA-verified sources and diagnostic. Predictions appear after saving; they do not certify joins.</p><table><tr><th>Version</th><th>Prediction</th><th>Implementation SHA</th></tr>"+rows+"</table></details>"
   page=page.replace("<p class='human-question'>",comparison+"<p class='human-question'>")
  return page
