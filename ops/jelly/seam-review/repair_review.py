"""Separate, opt-in overlap-repair review. Original candidate rounds are immutable here."""
import hashlib
import html
import json
from candidate_review import CandidateReview, COMPACT_CSS, HUMAN_JS, PLAYBACK_JS, decision_badge

REPAIR_CSS = """
.candidate-workspace{grid-template-columns:minmax(0,1fr) 300px}.comparison-panes{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:8px;flex:1;min-height:0}.comparison-pane{display:flex;flex-direction:column;min-width:0;min-height:0}.pane-label{font-size:13px;margin:0 0 4px}.reference-still{position:absolute;inset:0;width:100%;height:100%;object-fit:contain}.reference-status{font-size:12px;min-height:16px;margin:3px 0}.candidate-workspace .frame-toolbar button{min-height:44px}.repair-limits{padding:6px;border:1px solid var(--line);border-radius:5px}.comparison-pane .vwrap{background:#101010}.candidate-workspace [hidden]{display:none!important}
@media(max-width:900px){.candidate-workspace{grid-template-columns:minmax(0,1fr)}.comparison-panes{flex:none}.comparison-pane .vwrap{height:clamp(110px,28vw,220px)}.pane-label{font-size:12px;min-height:32px}.reference-status{font-size:11px}.playback-controls{grid-template-columns:65px minmax(0,1fr) 74px 65px}.candidate-workspace .frame-toolbar{grid-template-columns:repeat(2,minmax(0,1fr))}}
"""
REPAIR_JS = """
const originalVideo=document.getElementById('original-video'),originalPicture=document.getElementById('original-still'),originalStatus=document.getElementById('original-status');
let originalManifest=null,referenceReady=false,referenceURL=null,referenceStillURL=null,referenceSerial=0;
const referenceDelta=ORIGINAL_SEAM-REPAIR_SEAM;
originalVideo.muted=true;originalVideo.src=new URL('/'+ORIGINAL_VIDEO,location.href).href;originalVideo.addEventListener('loadedmetadata',()=>syncOriginal(true));
function syncOriginal(force=false){
 if(originalVideo.readyState<1 || frameViewIsActive())return;
 originalPicture.hidden=true;originalVideo.hidden=false;
 const time=Math.max(0,Math.min(originalVideo.duration||10,v.currentTime+referenceDelta));
 if(force || Math.abs(originalVideo.currentTime-time)>.08)originalVideo.currentTime=time;
 originalVideo.playbackRate=v.playbackRate;originalVideo.muted=v.muted;
 if(v.paused)originalVideo.pause();else originalVideo.play().catch(()=>{});
}
const referencePreparation=(async()=>{
 await prepareExactFrames();
 const response=await fetch(ORIGINAL_FRAME.index_url,{signal:frameAbort.signal});
 if(!response.ok)throw Error('Original frame index could not load');
 const bytes=await response.arrayBuffer();
 if(await frameHash(bytes)!==ORIGINAL_FRAME.index_sha256)throw Error('Original frame index identity differs');
 originalManifest=JSON.parse(new TextDecoder().decode(bytes));
 if(originalManifest.source_sha256!==ORIGINAL_FRAME.source_sha256 || originalManifest.frames.length!==ORIGINAL_FRAME.frame_count)throw Error('Original preview identity differs');
 const videoURL=new URL('/'+ORIGINAL_VIDEO,location.href).href;
 await cacheFrameAsset(videoURL,ORIGINAL_FRAME.source_sha256,'video/mp4');
 referenceURL=URL.createObjectURL(await cachedFrameBlob(videoURL,frameAbort));originalVideo.src=referenceURL;
 let cursor=originalManifest.min_frame;
 const worker=async()=>{while(cursor<=originalManifest.max_frame){const frame=originalManifest.frames[cursor++];await cacheFrameAsset(referenceFrameURL(frame),frame.image_sha256,'image/png');}};
 await Promise.all(Array.from({length:3},worker));
 referenceReady=true;originalStatus.textContent='Original verified · old Split preserved';syncOriginal(true);
})().catch(error=>{originalStatus.textContent='Original reference unavailable: '+error.message+'. Reload to retry.';throw error;});
referencePreparation.catch(()=>{});
function referenceFrameURL(frame){return new URL(`/frame-review/${ORIGINAL_FRAME.source_sha256}/frame-${frame.image_sha256}.png`,location.href).href;}
const repairedShowExactFrame=showExactFrame;
showExactFrame=async function(index){
 if(!frameReady)return;
 const serial=++referenceSerial;
 index=Math.max(frameManifest.min_frame,Math.min(frameManifest.max_frame,index));requestedFrame=index;
 v.pause();originalVideo.pause();
 try{
  await referencePreparation;
  const pts=Number(frameManifest.frames[index].pts)+referenceDelta;
  const choices=originalManifest.frames.slice(originalManifest.min_frame,originalManifest.max_frame+1);
  const frame=choices.reduce((best,f)=>Math.abs(Number(f.pts)-pts)<Math.abs(Number(best.pts)-pts)?f:best);
  const blob=await cachedFrameBlob(referenceFrameURL(frame),frameAbort);
  const url=URL.createObjectURL(blob);const picture=new Image();picture.src=url;await picture.decode();
  if(serial!==referenceSerial || !frameAlive){URL.revokeObjectURL(url);return;}
  await repairedShowExactFrame(index);
  if(serial!==referenceSerial || !frameViewIsActive()){URL.revokeObjectURL(url);return;}
  if(referenceStillURL)URL.revokeObjectURL(referenceStillURL);referenceStillURL=url;
  originalPicture.src=url;originalPicture.hidden=false;originalVideo.hidden=true;
  originalStatus.textContent=`Original frame ${frame.index+1} / ${originalManifest.decoded_frame_count} · ${Number(frame.pts).toFixed(6)} s · seam-aligned`;
 }catch(error){originalStatus.textContent='Comparison frame unavailable: '+error.message;}
};
new MutationObserver(()=>{if(!frameViewIsActive()){++referenceSerial;syncOriginal(true);}}).observe(framePicture,{attributes:true,attributeFilter:['hidden']});
['timeupdate','play','pause','seeked','ratechange','volumechange','loadedmetadata'].forEach(event=>v.addEventListener(event,()=>syncOriginal(event==='seeked')));
originalVideo.addEventListener('loadedmetadata',()=>syncOriginal(true));
const repairedVote=vote;
vote=async function(label){
 if(label!=='clear' && (!referenceReady || !frameReady)){showSaveMessage('Wait for both verified previews before grading.');return;}
 await repairedVote(label);
 document.querySelector('.candidate-progress').textContent=(document.querySelector('.card').dataset.humanLabel?'1':'0')+' / 1 repaired case graded';
};
window.addEventListener('pageshow',event=>{if(event.persisted)location.reload();});
window.addEventListener('pagehide',()=>{++referenceSerial;referenceReady=false;originalVideo.pause();if(referenceURL)URL.revokeObjectURL(referenceURL);if(referenceStillURL)URL.revokeObjectURL(referenceStillURL);});
"""

class RepairReview(CandidateReview):
    def __init__(self, root, page, slide_js, frame_js):
        self.root, self.page, self.frame_js = root, page, frame_js
        self.path = root / 'candidate-v3-ae50164-repair.json'
        self.config = json.loads(self.path.read_text()) if self.path.exists() else None
        self.slide_js = slide_js[:slide_js.index('async function vote(verdict){')]
        self.slide_js = self.slide_js.replace('function votePayload(verdict){const body={id:ITEM,verdict,', 'function votePayload(label){const body={id:ITEM,label,')
        self.slide_js = self.slide_js.replace("document.querySelector('video'),p=document.querySelector('.phase')", "document.getElementById('repaired-video'),p=document.getElementById('repair-phase')")
        self.slide_js += HUMAN_JS.replace('/api/candidate-vote', '/api/repair-vote')
        if self.config:
            self.cases = {c['id']: c for c in self.config['cases']}
            self.id = self.config['id']
            assert self.id == 'v3-ae50164-repair'
            assert self.config['revision'] == 'ae501645a91b9a40320f53808f7391ea5fc00e1a'
            evidence = root / 'comparison-evidence' / self.config['evidence_filename']
            assert hashlib.sha256(evidence.read_bytes()).hexdigest() == self.config['evidence_sha256']
            self.database = root / 'candidate-reviews' / self.id / 'review.db'
            self.database.parent.mkdir(parents=True, exist_ok=True)
            with self.db():
                pass

    def target(self, case):
        target = super().target(case)
        target.update(evaluation_basis='repaired', transformation=case['transformation'],
                      original_revision=case['original_revision'], original_item_id=case['original_item_id'],
                      original_preview_sha256=case['original_preview_sha256'],
                      repair_output_sha256=case['repair_output_sha256'], repair_proof_sha256=case['repair_proof_sha256'])
        return target

    def render(self, query):
        current = query.get('id', [next(iter(self.cases))])[0]
        case = self.cases[current]
        frame = case['frame_preview']
        vv = self.votes().get(current, {})
        selected = vv.get('human_label', '') if vv.get('label_source') == 'explicit_human_choice' else ''
        esc = html.escape
        speed = {'0.5': .5, '1': 1, '2': 2}.get(query.get('speed', ['2'])[0], 2)
        buttons = ''.join(f"<button class='human-choice choice-{label}{' on' if selected==label else ''}' data-label='{label}' onclick=\"vote('{label}')\" aria-pressed='{str(selected==label).lower()}' aria-keyshortcuts='{key}'>{text}</button>" for label,text,key in [('join','Join','1'),('split','Split','2'),('uncertain','I can’t tell','3')])
        body = f"""<style>{COMPACT_CSS}{REPAIR_CSS}</style><header class='candidate-header'><h1>Overlap repair · ae50164 · 348af7</h1><div class='bar'><span class='candidate-progress'>{int(bool(selected))} / 1 repaired case graded</span><details class='candidate-menu'><summary>More</summary><div><button class='undo-choice' onclick=\"vote('clear')\">Undo repaired choice</button><p>I can’t tell records uncertainty and is excluded from training.</p><p><a href='/candidate/v3-1f31b3d?all=1'>Prior round: all four grades</a></p><p>1d7374 remains separately unresolved.</p><p>Production V2 and all original rounds are preserved.</p></div></details></div></header>
<aside class='shortcut-sheet' aria-label='Keyboard shortcuts'><strong>Keys:</strong><span><kbd>1</kbd> Join</span><span><kbd>2</kbd> Split</span><span><kbd>3</kbd> Can’t tell</span><span><kbd>,</kbd> / <kbd>.</kbd> frames</span><span><kbd>j</kbd> seam</span><span><kbd>Space</kbd> play / pause</span><span><kbd>←</kbd> / <kbd>→</kbd> cases</span><span>More → Undo</span></aside>
<article class='card candidate-workspace' data-human-label='{selected}'><section class='media-column' aria-label='Original and repaired previews'><div class='comparison-panes'>
<section class='comparison-pane'><h2 class='pane-label'>Original · 1f31b3d · old Split</h2><div class='vwrap'><video id='original-video' muted loop playsinline preload='auto' aria-label='Original untrimmed seam reference'></video><img id='original-still' class='reference-still' hidden alt='Original decoded frame reference'></div><p id='original-status' class='reference-status' role='status'>Loading original reference…</p></section>
<section class='comparison-pane'><h2 class='pane-label'>Repaired · ae50164 · grade this</h2><div class='vwrap'><video id='repaired-video' data-preload-src='/{case['video']}' muted autoplay loop playsinline preload='auto' aria-label='Repaired seam preview to grade'></video><img class='frame-still' hidden alt='Repaired decoded HD frame'></div><div class='phasebar'><span id='repair-phase' class='phase'>BEFORE</span></div></section></div>
<div class='playback-controls' role='group' aria-label='Both preview playback controls'><button id='video-play' type='button' disabled>Play</button><input id='video-seek' type='range' min='0' max='10' step='0.01' value='0' disabled aria-label='Seek both review videos'><output id='video-time' aria-label='Repaired video time'>0:00 / 0:10</output><button id='video-mute' type='button'>Unmute</button></div>
<section class='frame-controls' aria-label='Exact frame review of both previews'><div class='frame-toolbar'><button id='frame-previous' disabled>Previous frame</button><button id='frame-next' disabled>Next frame</button><button id='frame-seam' disabled>Jump to seam</button><button id='frame-resume' disabled>Resume video</button></div><button id='frame-retry' hidden>Retry preload</button><progress id='frame-progress' value='0' max='27' aria-label='Exact frames loaded'></progress><p id='frame-status' class='frame-status' role='status' aria-live='polite'>Loading repaired video and exact frames…</p><div class='frame-range-row'><label for='frame-range'>Frames</label><input id='frame-range' type='range' disabled min='{frame['min_frame']}' max='{frame['max_frame']}' value='{frame['seam_frame_index']}' aria-label='Browse repaired exact frames with original reference'></div></section></section>
<section class='meta compact-sidebar' aria-label='Repaired decision and grading'><p class='title'>Punta Arenas · sources 484918 → 484959</p><p class='human-question'><strong>What should happen at this repaired seam?</strong></p><div class='btns human-choices' role='group' aria-label='Your repaired seam decision'>{buttons}</div><label class='comment-label' for='review-note'>Comment (optional)</label><input class='note' id='review-note' placeholder='Add a comment before grading' value='{esc(vv.get('note',''),quote=True)}' onchange='saveNote()'><span id='saved' class='save-message' role='status' aria-live='polite'></span><button id='save-retry' class='undo-choice' hidden onclick='retryReview()'>Check / retry same choice</button>
<p class='claim model-context'>Repair proposes: {decision_badge('join')}</p><div class='comparison candidate-comparison'><table aria-label='Original and repaired outcomes'><thead><tr><th>Version</th><th>Outcome</th><th>Evidence</th></tr></thead><tbody><tr><th>Original 1f31b3d</th><td>{decision_badge('split')}</td><td>Your Split preserved; repeated footage.</td></tr><tr><th>Repair ae50164</th><td>{decision_badge('join')}</td><td>60 repeated frames removed from the first clip’s tail.</td></tr></tbody></table></div>
<p>The old Split applies to the untrimmed seam. Only this repair needs a new grade.</p><p class='repair-limits'><strong>Offline, opt-in, video-only.</strong> Audio and reordered video fail closed. Production is unchanged.</p><p class='speed-links'>Speed: <a href='?speed=0.5'>0.5×</a> · <a href='?speed=1'>1×</a> · <a href='?speed=2'>2×</a></p><details><summary>Repair proof and version</summary><p>Exact frames align by time relative to each seam. Original and repaired preview resolutions differ; judge continuity.</p><p>ae501645a91b9a40320f53808f7391ea5fc00e1a</p><p>Retained packet ranges: 484918 [0,720), 484959 [0,780). 1,500 retained video packets; payload conservation and source-relative timestamps verified.</p><p><a href='/evidence/{self.config['evidence_filename']}'>Immutable evidence</a></p><p>Human approval is still required. 1d7374 remains separately unresolved.</p></details></section></article>"""
        constants = {'ITEM': current, 'PREV': '', 'NEXT': '', 'SPEED': speed, 'PRIORITY': False, 'RESUME': '/candidate/'+self.id, 'REVIEW_TARGET': self.target(case), 'FRAME_PREVIEW': frame, 'REFERENCE_ONLY': False, 'ORIGINAL_FRAME': case['original_frame_preview'], 'ORIGINAL_VIDEO': case['original_video'], 'ORIGINAL_SEAM': case['original_seam_seconds'], 'REPAIR_SEAM': case['seam_seconds']}
        js = 'const '+','.join(k+'='+json.dumps(v) for k,v in constants.items())+';'+self.slide_js+self.frame_js+PLAYBACK_JS+REPAIR_JS
        return self.page('Overlap repair ae50164 · 348af7', body, js)
