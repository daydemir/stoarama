"""Version-scoped private candidate grading; original review DB stays separate."""
import hashlib
import html
import json
import re
import sqlite3
import sys
import time
import uuid
from pathlib import Path
from urllib.parse import urlencode

COMPACT_CSS = """
main{width:100%;max-width:none;height:100vh;height:100dvh;min-height:0;padding:8px;display:flex;flex-direction:column;gap:6px}
.candidate-header{display:flex;align-items:center;justify-content:space-between;gap:12px;min-height:34px;flex:none}
.candidate-header h1{font-size:18px;line-height:1.2;margin:0}.candidate-header .bar{gap:6px;margin:0}
.candidate-header .navb{padding:5px 9px;margin:0;font-size:13px}.candidate-progress{font-size:12px;white-space:nowrap}
.shortcut-sheet{display:flex;flex-wrap:wrap;align-items:center;gap:3px 12px;flex:none;padding:3px 6px;border:1px solid var(--line);border-radius:6px;font-size:12px;line-height:1.35}.shortcut-sheet span{white-space:nowrap}.shortcut-sheet kbd{font:600 11px system-ui;padding:1px 4px;border:1px solid var(--line);border-radius:3px;background:var(--card)}
.playback-controls{display:grid;grid-template-columns:76px minmax(0,1fr) 78px 76px;align-items:center;gap:6px;flex:none;margin-top:4px}.playback-controls button{min-height:44px;border:1px solid var(--line);border-radius:6px;background:var(--card);color:var(--fg);font:600 13px system-ui;cursor:pointer}.playback-controls input{min-width:0;width:100%;min-height:44px;margin:0;accent-color:var(--blue)}.playback-controls output{font-size:12px;white-space:nowrap;text-align:center;font-variant-numeric:tabular-nums}.playback-controls button:disabled{opacity:.5;cursor:default}.playback-controls input:focus-visible{outline:3px solid var(--blue);outline-offset:2px}.save-message{display:block;font-size:12px;line-height:1.25;margin-top:2px}.save-message:empty{display:none}
.candidate-menu{position:relative;font-size:13px}.candidate-menu summary{cursor:pointer;padding:5px}
.candidate-menu[open]>div{position:absolute;right:0;top:30px;z-index:5;width:260px;padding:12px;background:var(--card);border:1px solid var(--line);border-radius:8px;box-shadow:0 3px 12px #0002}
.candidate-menu p{margin:7px 0}.candidate-workspace{display:grid;grid-template-columns:minmax(0,1fr) 340px;gap:10px;padding:8px;flex:1;min-height:0;overflow:visible}
.media-column{display:flex;flex-direction:column;min-width:0;min-height:0}.candidate-workspace .vwrap{position:relative;flex:1;min-height:0;border-radius:6px;overflow:hidden}
.candidate-workspace .vwrap video,.candidate-workspace .vwrap .frame-still{position:absolute;inset:0;width:100%;height:100%;max-height:100%;object-fit:contain}
.candidate-workspace .phasebar{height:22px;min-height:22px;border:0;margin-top:3px}.candidate-workspace .phase{font-size:12px}
.candidate-workspace .frame-controls{padding:4px 0 0;border:0;flex:none}.candidate-workspace .frame-toolbar{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:5px;margin:0 0 3px}
.candidate-workspace .frame-toolbar button{min-height:36px;padding:5px 7px;font-size:13px}.candidate-workspace .frame-status{font-size:12px;line-height:1.25;min-height:16px;margin:3px 0}
.frame-range-row{display:flex;align-items:center;gap:8px;font-size:12px}.frame-range-row input{flex:1;min-width:0;margin:0;min-height:26px}.candidate-workspace .frame-controls progress{height:5px}
.compact-sidebar{min-width:0;min-height:0;overflow:auto;padding:0 2px 0 0}.compact-sidebar .claim{font-size:13px;line-height:1.3;padding:3px 8px;margin:0 0 5px;max-width:100%}
.compact-sidebar p{margin:3px 0;font-size:12px;line-height:1.3}.compact-sidebar .title{font-size:13px}.compact-sidebar .comparison{margin:6px 0}
.candidate-comparison table{table-layout:fixed;font-size:12px}.candidate-comparison th,.candidate-comparison td{font-size:12px;line-height:1.25;padding:4px 4px;overflow-wrap:anywhere}
.candidate-comparison th:first-child{width:35%}.candidate-comparison td:nth-child(2){width:20%}.candidate-comparison td:nth-child(3){width:45%}
.decision-badge{display:inline-flex;align-items:center;gap:3px;white-space:nowrap;font-size:11px;font-weight:800;line-height:1.25;padding:2px 4px;border:1px solid;border-radius:4px;vertical-align:middle}
.decision-join{color:#123b75;background:#dbeafe;border-color:#245ca6}.decision-split{color:#713f12;background:#ffedd5;border-color:#9a520f}
.decision-legend{display:flex;align-items:center;gap:6px;font-size:11px;line-height:1.25;margin-top:5px}.compact-sidebar .claim .decision-badge{font-size:12px}
@media(prefers-color-scheme:dark){.decision-join{color:#b9dcff;background:#193655;border-color:#77b8ff}.decision-split{color:#ffddb0;background:#4a2f13;border-color:#eeb366}}
.compact-sidebar .btns{gap:6px;margin:5px 0}.compact-sidebar .btns button{font-size:15px;min-height:44px;padding:7px}
.compact-sidebar .human-question{font-size:15px;margin:6px 0 3px}.human-choices .human-choice{font-size:13px;border-width:2px;font-weight:750}
.human-choice.choice-join{color:#123b75;background:#dbeafe;border-color:#245ca6}.human-choice.choice-split{color:#713f12;background:#ffedd5;border-color:#9a520f}.human-choice.choice-uncertain{color:var(--fg);background:var(--bg);border-color:var(--mut)}
.human-choice.on{outline:3px solid currentColor;outline-offset:1px}.human-choice:disabled{opacity:1}.human-choice[data-save-state="pending"]{outline-style:dashed}.human-choice[data-save-state="pending"]::after{content:" …"}.human-choice[data-save-state="saved"]::after{content:" ✓"}.human-choice[data-save-state="error"]::after{content:" !"}body[data-review-saving="true"] .candidate-header .navb{pointer-events:none;opacity:.65}.compact-sidebar .model-context{font-size:12px;margin:5px 0 0;padding:2px 6px;border:1px solid var(--line);font-weight:400}.undo-choice{min-height:44px;border:1px solid var(--line);border-radius:5px;background:var(--bg);color:var(--fg);padding:6px;cursor:pointer}
@media(prefers-color-scheme:dark){.human-choice.choice-join{color:#b9dcff;background:#193655;border-color:#77b8ff}.human-choice.choice-split{color:#ffddb0;background:#4a2f13;border-color:#eeb366}}
.compact-sidebar .note{margin:3px 0 0;padding:7px 8px;height:36px}.comment-label{display:block;font-size:12px;margin-top:6px}
.compact-sidebar details{font-size:12px;margin:5px 0}.compact-sidebar details summary{cursor:pointer}.compact-sidebar code{overflow-wrap:anywhere;font-size:11px}
.compact-sidebar .speed-links{margin-top:6px}.candidate-workspace button:focus-visible,.candidate-header a:focus-visible,.candidate-header summary:focus-visible,.compact-sidebar input:focus-visible{outline:3px solid var(--blue);outline-offset:2px}
@media(max-width:900px){main{height:auto;min-height:100vh;min-height:100dvh}.candidate-header{flex-wrap:wrap;gap:4px}.candidate-header h1{font-size:16px}.candidate-header .bar{gap:4px}.candidate-progress{font-size:11px}
.candidate-workspace{grid-template-columns:minmax(0,1fr);flex:none;padding:6px;gap:8px}.media-column{min-height:0}.candidate-workspace .vwrap{flex:none;height:clamp(150px,56.25vw,300px)}
.candidate-workspace .frame-toolbar{grid-template-columns:repeat(2,minmax(0,1fr))}.candidate-workspace .frame-toolbar button{min-height:44px}.compact-sidebar{overflow:visible;padding:0}.compact-sidebar .claim{font-size:13px}
.compact-sidebar .comparison{margin:6px 0}.candidate-comparison th,.candidate-comparison td{padding:5px 4px}.candidate-menu[open]>div{max-width:calc(100vw - 20px)}}
"""

HUMAN_JS = """
let pendingReview=null;
function markReviewChoice(label,state){
 document.querySelectorAll('.human-choice').forEach(b=>{const active=b.dataset.label===label;
  b.classList.toggle('on',active);b.setAttribute('aria-pressed',String(active));
  if(active){b.dataset.saveState=state;b.setAttribute('aria-busy',String(state==='pending'));b.setAttribute('aria-label',({join:'Join',split:'Split',uncertain:"I can’t tell"}[label]||label)+', '+({pending:'saving',saved:'confirmed saved',error:'save not confirmed'}[state]||state));}
  else{delete b.dataset.saveState;b.removeAttribute('aria-busy');b.removeAttribute('aria-label');}
 });
}
const initialChoice=document.querySelector('.card')?.dataset.humanLabel;
if(initialChoice)markReviewChoice(initialChoice,'saved');
function showSaveMessage(message,error=false){const node=document.getElementById('saved');node.textContent=message;node.setAttribute('role',error?'alert':'status');document.getElementById('save-retry').hidden=!error;}
async function requestReceipt(requestID){
 const response=await fetch('/api/candidate-receipt/'+REVIEW_TARGET.candidate_id+'?request_id='+encodeURIComponent(requestID),{cache:'no-store'});
 if(!response.ok)return null;const receipt=await response.json();return receipt.ok?receipt:null;
}
async function vote(label){
 if(REFERENCE_ONLY || saving)return;
 const next=JSON.parse(votePayload(label));
 const signature=JSON.stringify(next);
 if(!pendingReview || pendingReview.signature!==signature)pendingReview={signature,body:{...next,request_id:crypto.randomUUID()}};
 await submitReview();
}
async function retryReview(){if(pendingReview&&!saving)await submitReview();}
async function submitReview(){
 const attempt=pendingReview,label=attempt.body.label;
 saving=true;document.body.dataset.reviewSaving='true';markReviewChoice(label,'pending');showSaveMessage((label==='clear'?'Clearing choice':({join:'Join',split:'Split',uncertain:"I can’t tell"}[label])+' selected')+' — saving…');document.querySelectorAll('.human-choice,.undo-choice').forEach(b=>b.disabled=true);
 try{
  let receipt;
  try{
   const r=await fetch('/api/candidate-vote',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(attempt.body)});
   const result=await r.json();if(!r.ok)throw Error('HTTP '+r.status+': '+(result.error||'save rejected'));if(!result.ok)throw Error('Invalid save response');receipt=result;
  }catch(error){
   receipt=await requestReceipt(attempt.body.request_id).catch(()=>null);
   if(!receipt)throw error;
  }
  if(receipt.request_id!==attempt.body.request_id||receipt.item_id!==ITEM||receipt.human_label!==label)throw Error('Save receipt differs from this choice');
  const card=document.querySelector('.card');card.dataset.humanLabel=label==='clear'?'':label;
  markReviewChoice(label,'saved');
  showSaveMessage(label==='clear'?'Choice cleared':label==='uncertain'?"Saved ✓: I can’t tell — excluded from training":"Saved ✓: "+(label==='join'?'Join':'Split'));pendingReview=null;
  if((PRIORITY || new URLSearchParams(location.search).get('queue')==='1') && label!=='clear'){await new Promise(resolve=>setTimeout(resolve,180));location.href=new URLSearchParams(location.search).get('queue')==='1'?'/review-queue':RESUME;}
 }catch(e){markReviewChoice(label,'error');showSaveMessage('Save unconfirmed. '+e.message+'. Retry keeps this request.',true);}
 finally{saving=false;delete document.body.dataset.reviewSaving;document.querySelectorAll('.human-choice,.undo-choice').forEach(b=>b.disabled=false);}
}
async function saveNote(){const label=document.querySelector('.card').dataset.humanLabel;if(!label || REFERENCE_ONLY)return;await vote(label);}
document.addEventListener('keydown',e=>{if(e.ctrlKey||e.metaKey||e.altKey||e.target.closest('input,textarea,select,[contenteditable="true"]')||saving)return;
 if(e.key==='1')vote('join');else if(e.key==='2')vote('split');else if(e.key==='3')vote('uncertain');
 else if(e.key==='ArrowRight'||e.key==='n'){if(NEXT)location.href=NEXT}else if(e.key==='ArrowLeft'||e.key==='p'){if(PREV)location.href=PREV}
 else if(e.key===' ' && e.target.tagName!=='BUTTON'){e.preventDefault();togglePlayback()}});
"""

PLAYBACK_JS = """
const playButton=document.getElementById('video-play'),seekVideo=document.getElementById('video-seek'),muteButton=document.getElementById('video-mute'),videoTime=document.getElementById('video-time');
v.controls=false;v.removeAttribute('controls');
const clockTime=t=>{t=Math.max(0,Number.isFinite(t)?t:0);return Math.floor(t/60)+':'+String(Math.floor(t%60)).padStart(2,'0');};
function updatePlayback(){
 const exact=typeof frameViewIsActive==='function' && frameViewIsActive();
 const loaded=Number.isFinite(v.duration)&&v.duration>0;
 playButton.disabled=seekVideo.disabled=!loaded;
 playButton.textContent=exact?'Resume':v.paused?'Play':'Pause';playButton.setAttribute('aria-label',exact?'Resume video':v.paused?'Play video':'Pause video');
 if(loaded){seekVideo.max=String(v.duration);seekVideo.value=String(exact?Number(frameManifest.frames[displayedFrame].pts):v.currentTime);}
 videoTime.textContent=clockTime(Number(seekVideo.value))+' / '+clockTime(loaded?v.duration:0);
 muteButton.textContent=v.muted?'Unmute':'Mute';muteButton.setAttribute('aria-label',v.muted?'Unmute video':'Mute video');
}
function togglePlayback(){
 if(typeof frameViewIsActive==='function' && frameViewIsActive())resumeFrameVideo();
 else if(v.paused)v.play().catch(()=>{frameStatus.textContent='Video is paused. Press Play to start.';updatePlayback();});
 else v.pause();
 updatePlayback();
}
playButton.addEventListener('click',togglePlayback);
seekVideo.addEventListener('input',()=>{const target=Number(seekVideo.value);if(frameViewIsActive()){resumeFrameVideo();v.pause();}v.currentTime=target;updatePlayback();});
muteButton.addEventListener('click',()=>{v.muted=!v.muted;updatePlayback();});
['loadedmetadata','durationchange','timeupdate','play','pause','volumechange','seeked','ended'].forEach(event=>v.addEventListener(event,updatePlayback));
new MutationObserver(updatePlayback).observe(framePicture,{attributes:true,attributeFilter:['hidden']});
updatePlayback();
"""


def decision_badge(decision):
    kind = {'continuous': 'join', 'jump': 'split'}.get(decision, decision)
    if kind not in ('join', 'split'):
        return html.escape(decision.upper())
    icon = '↔' if kind == 'join' else '∥'
    return f"<span class='decision-badge decision-{kind}'><span aria-hidden='true'>{icon}</span>{kind.upper()}</span>"


class CandidateReview:
    def __init__(self, root, page, slide_js, frame_js):
        self.root, self.page, self.frame_js = root, page, frame_js
        self.path = root / 'candidate-v3-1f31b3d.json'
        self.config = json.loads(self.path.read_text()) if self.path.exists() else None
        self.slide_js = slide_js[:slide_js.index('async function vote(verdict){')]
        self.slide_js = self.slide_js.replace('function votePayload(verdict){const body={id:ITEM,verdict,', 'function votePayload(label){const body={id:ITEM,label,') + HUMAN_JS
        if self.config:
            self.cases = {c['id']: c for c in self.config['cases']}
            self.id = self.config['id']
            assert self.id == 'v3-1f31b3d'
            assert self.config['revision'] == '1f31b3d1b0cb53a7ef95e64987132c3e99e9c147'
            evidence = root / 'comparison-evidence' / self.config['evidence_filename']
            assert hashlib.sha256(evidence.read_bytes()).hexdigest() == self.config['evidence_sha256']
            self.database = root / 'candidate-reviews' / self.id / 'review.db'
            self.database.parent.mkdir(parents=True, exist_ok=True)
            with self.db():
                pass

    def db(self):
        con = sqlite3.connect(self.database, timeout=10)
        con.execute('CREATE TABLE IF NOT EXISTS votes(id TEXT PRIMARY KEY,verdict TEXT NOT NULL,note TEXT,updated_at REAL NOT NULL)')
        con.execute('CREATE TABLE IF NOT EXISTS history(id TEXT,verdict TEXT,note TEXT,at REAL,context TEXT NOT NULL)')
        con.execute("CREATE TABLE IF NOT EXISTS human_labels(id TEXT PRIMARY KEY,label TEXT NOT NULL CHECK(label IN ('join','split','uncertain')),note TEXT,updated_at REAL NOT NULL,context TEXT NOT NULL)")
        con.execute("CREATE TABLE IF NOT EXISTS human_history(id TEXT,label TEXT NOT NULL,note TEXT,at REAL,context TEXT NOT NULL)")
        con.execute("CREATE TABLE IF NOT EXISTS review_requests(request_id TEXT PRIMARY KEY,payload_sha256 TEXT NOT NULL,response TEXT NOT NULL)")
        return con

    def votes(self):
        with self.db() as con:
            votes = {i: {'verdict': v, 'note': n or ''} for i, v, n in con.execute('SELECT id,verdict,note FROM votes')}
            for i, vote in votes.items():
                case = self.cases.get(i)
                if case and vote['verdict'] in ('good', 'bad') and case['claim'] in ('continuous', 'jump'):
                    model = 'join' if case['claim'] == 'continuous' else 'split'
                    vote.update(human_label=model if vote['verdict'] == 'good' else ('split' if model == 'join' else 'join'), label_source='inferred_legacy_agreement')
            for i, label, note, stamp, context in con.execute('SELECT * FROM human_labels'):
                ctx = json.loads(context)
                votes[i] = {'human_label': label, 'label_source': 'explicit_human_choice', 'note': note or '',
                            'updated_at': stamp, 'derived_agreement': ctx['derived_agreement'],
                            'verdict': ctx['derived_agreement'] or '', 'training_eligible': ctx.get('training_eligible', label != 'uncertain'), 'context': ctx}
            return votes

    def target(self, case):
        return {'algorithm': 'collation-v3', 'candidate_id': self.id, 'revision': self.config['revision'],
                'item_id': case['id'], 'claim': case['claim'], 'evaluation_basis': 'fresh',
                'preview_variant': 'hd', 'preview_sha256': case['preview_sha256'],
                'evidence_sha256': self.config['evidence_sha256']}

    def context(self, case, supplied):
        expected = self.target(case)
        if not isinstance(supplied, dict) or any(supplied.get(k) != v for k, v in expected.items()):
            raise ValueError('candidate version or claim differs')
        if set(supplied) - set(expected) - {'frame_review'}:
            raise ValueError('unknown candidate context')
        frame_context = supplied.get('frame_review')
        if frame_context is not None:
            preview = case.get('frame_preview')
            if not isinstance(frame_context, dict) or not preview or frame_context.get('index_sha256') != preview['index_sha256']:
                raise ValueError('candidate frame index differs')
            index_path = self.root / preview['index_url'].lstrip('/')
            body = index_path.read_bytes()
            if hashlib.sha256(body).hexdigest() != preview['index_sha256']:
                raise ValueError('frame index identity differs')
            index = json.loads(body)
            n = frame_context.get('frame_index')
            if type(n) is not int or not index['min_frame'] <= n <= index['max_frame'] or index['source_sha256'] != expected['preview_sha256']:
                raise ValueError('invalid candidate frame selection')
            frame = index['frames'][n]
            if frame_context != {'index_sha256': preview['index_sha256'], 'frame_index': n, 'pts': frame['pts'], 'image_sha256': frame['image_sha256']}:
                raise ValueError('candidate frame identity differs')
            expected['frame_review'] = frame_context
        expected['binding_source'] = 'explicit_candidate_page'
        return expected

    def get(self, handler, url, query):
        if not self.config:
            return False
        if url.path == '/api/candidate-votes/' + self.id:
            handler.send(200, json.dumps(self.votes()), 'application/json')
            return True
        if url.path == '/api/candidate-receipt/' + self.id:
            request_id = query.get('request_id', [''])[0]
            with self.db() as con:
                receipt = con.execute('SELECT response FROM review_requests WHERE request_id=?', (request_id,)).fetchone()
            handler.send(200 if receipt else 404, receipt[0] if receipt else '{"ok":false,"error":"No saved receipt found"}', 'application/json')
            return True
        if url.path != '/candidate/' + self.id:
            return False
        try:
            handler.send(200, self.render(query))
        except KeyError:
            handler.send(404, 'unknown candidate item')
        return True

    def post(self, handler):
        if not self.config:
            return handler.send(404, 'candidate unavailable')
        try:
            length = int(handler.headers.get('Content-Length', '0'))
            if not 0 < length <= 8192:
                raise ValueError('invalid body size')
            request = json.loads(handler.rfile.read(length))
            case = self.cases[request['id']]
            label = request.get('label')
            if label is None and request.get('verdict') == 'clear':
                label = 'clear'
            note = str(request.get('note', ''))[:1000]
            if case['reference_only'] or label not in ('join', 'split', 'uncertain', 'clear'):
                raise ValueError('candidate item is read-only or verdict invalid')
            context = self.context(case, request.get('review_target'))
            request_id = request.get('request_id') or str(uuid.uuid4())
            if not isinstance(request_id, str) or not re.fullmatch(r'[a-f0-9-]{36}', request_id):
                raise ValueError('invalid save request identifier')
        except (KeyError, ValueError, TypeError) as error:
            detail = str(error) if isinstance(error, ValueError) else 'invalid candidate request'
            if isinstance(locals().get('request'), dict) and request.get('verdict') in ('good', 'bad'):
                detail = 'This tab uses the old grading format. Reload for Join / Split / I can’t tell.'
            print('candidate save rejected at '+str(time.time())+': '+detail, file=sys.stderr, flush=True)
            return handler.send(400, json.dumps({'ok': False, 'error': detail}), 'application/json')
        stamp = time.time()
        model = 'join' if case['claim'] == 'continuous' else 'split'
        agreement = None if label in ('uncertain', 'clear') else 'good' if label == model else 'bad'
        context.update(human_label=label, label_source='explicit_human_choice', derived_agreement=agreement, training_eligible=label in ('join', 'split') and not case.get('control_only',False) and case.get('builder_eligible',True))
        context['request_id'] = request_id
        encoded = json.dumps(context, sort_keys=True)
        fingerprint = hashlib.sha256(json.dumps({'id': case['id'], 'label': label, 'note': note, 'context': context}, sort_keys=True).encode()).hexdigest()
        response = json.dumps({'ok': True, 'item_id': case['id'], 'human_label': label, 'derived_agreement': agreement, 'request_id': request_id, 'saved_at': stamp})
        try:
            with self.db() as con:
                con.execute('BEGIN IMMEDIATE')
                prior = con.execute('SELECT payload_sha256,response FROM review_requests WHERE request_id=?', (request_id,)).fetchone()
                if prior:
                    if prior[0] != fingerprint:
                        return handler.send(409, '{"ok":false,"error":"Save identifier already belongs to a different choice"}', 'application/json')
                    response = prior[1]
                else:
                    if label in ('uncertain', 'clear'):
                        con.execute('DELETE FROM votes WHERE id=?', (case['id'],))
                    else:
                        con.execute('INSERT INTO votes VALUES(?,?,?,?) ON CONFLICT(id) DO UPDATE SET verdict=excluded.verdict,note=excluded.note,updated_at=excluded.updated_at',
                                    (case['id'], agreement, note, stamp))
                    if label == 'clear':
                        con.execute('DELETE FROM human_labels WHERE id=?', (case['id'],))
                    else:
                        con.execute('INSERT INTO human_labels VALUES(?,?,?,?,?) ON CONFLICT(id) DO UPDATE SET label=excluded.label,note=excluded.note,updated_at=excluded.updated_at,context=excluded.context',
                                    (case['id'], label, note, stamp, encoded))
                    con.execute('INSERT INTO human_history VALUES(?,?,?,?,?)', (case['id'], label, note, stamp, encoded))
                    con.execute('INSERT INTO history VALUES(?,?,?,?,?)', (case['id'], agreement or label, note, stamp, encoded))
                    con.execute('INSERT INTO review_requests VALUES(?,?,?)', (request_id, fingerprint, response))
        except sqlite3.Error:
            print('candidate save database error at '+str(time.time())+' request '+request_id, file=sys.stderr, flush=True)
            return handler.send(503, '{"ok":false,"error":"Save temporarily unavailable; check receipt before retrying the same request"}', 'application/json')
        return handler.send(200, response, 'application/json')

    def render(self, query):
        esc = html.escape
        votes = self.votes()
        show_all = query.get('all', ['0'])[0] == '1'
        current = query.get('id', [''])[0]
        reference = current in self.cases and self.cases[current]['reference_only']
        full = [c for c in self.config['cases'] if not c['reference_only']]
        pending = [c for c in full if c['id'] not in votes]
        route = '/candidate/' + self.id
        def link(i='', **options):
            params = {'id': i, **options}
            if show_all:
                params.setdefault('all', 1)
            return route + ('?' + urlencode(params) if any(str(v) for v in params.values()) else '')
        if current and current not in self.cases:
            raise KeyError(current)
        if not reference and not show_all and (not current or current in votes):
            if not pending:
                return self.page('Candidate review complete', f"<h1>Candidate 1f31b3d review complete</h1><p>All {len(full)} priority cases are graded for this revision.</p><p><a href='{route}?all=1'>Review candidate grades</a> · <a href='/compare/version-review-20261006'>Original V3 comparison</a></p>")
            current = pending[0]['id']
        if not current:
            current = full[0]['id']
        case = self.cases[current]
        queue = [case] if reference else full if show_all else pending
        pos = next(n for n, c in enumerate(queue) if c['id'] == current)
        previous = link(queue[pos-1]['id']) if pos else ''
        following = link(queue[pos+1]['id']) if pos+1 < len(queue) else ''
        speed_value = query.get('speed', ['2'])[0]
        speed = {'0.5': .5, '1': 1, '2': 2}.get(speed_value, 2)
        target = self.target(case)
        vv = votes.get(current, {})
        verdict = vv.get('verdict', '')
        selected_label = vv.get('human_label', '') if vv.get('label_source') == 'explicit_human_choice' else ''
        frame = case['frame_preview']
        rows = ''
        v1_basis = 'historical' if case['v1'].get('basis') == 'historical' else 'unavailable'
        previous_basis = 'unavailable' if case['previous_candidate']['decision'] == 'unavailable' else 'fresh'
        for title, result in [('V1 · '+v1_basis, case['v1']), ('V2 · fresh', case['v2']), ('V3 d50ce5c · '+case['old_v3_basis'], case['old_v3']), ('Candidate 01668b8 · '+previous_basis, case['previous_candidate']), ('Candidate 1f31b3d · fresh', case['candidate'])]:
            rows += f"<tr><th scope='row'>{esc(title)}</th><td>{decision_badge(result['decision'])}</td><td>{esc(result.get('reason',''))}</td></tr>"
        old_vote = case.get('historical_vote')
        human = f"<p class='mut'>Historical d50ce5c vote: {esc(old_vote['verdict'].title())} on {decision_badge(case['old_claim'])}. It remains attached to that version.</p>" if old_vote else "<p class='mut'>No prior human grade for this pair.</p>"
        claim = decision_badge(case['claim']) + (' continuous' if case['claim'] == 'continuous' else ' jump')
        refs = ' · '.join(f"<a href='{link(c['id'])}'>{esc(c['short_label'])}</a>" for c in self.config['cases'] if c['reference_only'])
        nav = (f"<a class='navb' href='{previous}' aria-label='Previous case'>← Prev</a>" if previous else "<span class='navb off' aria-disabled='true'>← Prev</span>") + (f"<a class='navb' href='{following}' aria-label='Next case'>Next →</a>" if following else "<span class='navb off' aria-disabled='true'>Next →</span>")
        controls = f"""<section class='frame-controls' aria-label='Exact frame review'><div class='frame-toolbar'>
<button type='button' id='frame-previous' disabled aria-keyshortcuts=','>Previous frame</button><button type='button' id='frame-next' disabled aria-keyshortcuts='.'>Next frame</button>
<button type='button' id='frame-seam' disabled aria-keyshortcuts='j'>Jump to seam</button><button type='button' id='frame-resume' disabled>Resume video</button></div>
<button type='button' id='frame-retry' hidden>Retry preload</button><progress id='frame-progress' value='0' max='{frame['max_frame']-frame['min_frame']+1}' aria-label='Exact frames loaded'></progress>
<p id='frame-status' class='frame-status' role='status' aria-live='polite'>Loading the review clip and exact frames…</p>
<div class='frame-range-row'><label for='frame-range'>Frames</label><input id='frame-range' type='range' disabled min='{frame['min_frame']}' max='{frame['max_frame']}' value='{frame['seam_frame_index']}' aria-label='Browse decoded HD frames'></div></section>"""
        choice_buttons = ''.join(f"<button class='human-choice choice-{label}{' on' if selected_label==label else ''}' data-label='{label}' onclick=\"vote('{label}')\" aria-pressed='{str(selected_label==label).lower()}' aria-keyshortcuts='{key}' title='{text} (key {key})'>{text}</button>" for label,text,key in [('join','Join','1'),('split','Split','2'),('uncertain','I can’t tell','3')])
        undo_markup = '' if reference else "<button class='undo-choice' onclick=\"vote('clear')\">Undo choice on this case</button>"
        grades = "<p><strong>Previously approved Seoul join is preserved.</strong> No repeat grade requested.</p><input class='note' hidden disabled><span id='saved'></span>" if reference else f"""<p class='human-question'><strong>Your seam decision</strong></p>
<div class='btns human-choices' role='group' aria-label='Your decision at this seam'>{choice_buttons}</div>
<label class='comment-label' for='review-note'>Comment (optional)</label><input id='review-note' class='note' placeholder='Add a note before saving' value='{esc(vv.get('note',''),quote=True)}' onchange='saveNote()'><span id='saved' class='save-message' role='status' aria-live='polite'></span><button id='save-retry' class='undo-choice' hidden onclick='retryReview()'>Retry same choice</button>"""
        body = f"""<style>{COMPACT_CSS}</style><header class='candidate-header'><h1>{'Reference' if reference else 'Candidate'} · 1f31b3d</h1>
<div class='bar'><span class='candidate-progress'>{'Reference' if reference else str(pos+1)+' / '+str(len(queue))} · {sum(c['id'] in votes for c in full)} / {len(full)} graded</span>{nav}
<details class='candidate-menu'><summary>More</summary><div><p><a href='{route}'>Priority queue</a> · <a href='{route}?all=1'>All reviews</a></p>{undo_markup}<p>Keys: 1 Join · 2 Split · 3 I can’t tell.</p><p>Can’t tell = uncertain; excluded from training.</p><p><a href='/compare/version-review-20261006'>Original V3 comparison</a></p><p>Seoul references: {refs}. Already reviewed.</p><p>Production V2 is unchanged.</p></div></details></div></header>
<aside class='shortcut-sheet' aria-label='Keyboard shortcuts'><strong>Keys:</strong><span><kbd>1</kbd> Join</span><span><kbd>2</kbd> Split</span><span><kbd>3</kbd> Can’t tell</span><span><kbd>,</kbd> / <kbd>.</kbd> frames</span><span><kbd>j</kbd> seam</span><span><kbd>Space</kbd> play / pause</span><span><kbd>←</kbd> / <kbd>→</kbd> cases</span><span>More → Undo</span></aside>
<article class='card candidate-workspace' data-verdict='{verdict}' data-human-label='{selected_label}'>
<section class='media-column' aria-label='Seam preview and exact frames'><div class='vwrap'><video data-preload-src='/{esc(case['video'],quote=True)}' muted autoplay loop playsinline preload='auto' aria-label='Seam preview'></video><img class='frame-still' hidden alt='Decoded HD frame'></div>
<div class='playback-controls' role='group' aria-label='Video playback controls'><button id='video-play' type='button' disabled>Play</button><input id='video-seek' type='range' min='0' max='10' step='0.01' value='0' disabled aria-label='Seek review video'><output id='video-time' aria-label='Video time'>0:00 / 0:10</output><button id='video-mute' type='button' aria-label='Unmute video'>Unmute</button></div>
<div class='phasebar'><span class='phase'>BEFORE</span></div>{controls}</section><section class='meta compact-sidebar' aria-label='Candidate decision and grading'>
<p class='title'>{esc(case['title'])} · sources {' → '.join(map(str,case['clip_ids']))}</p>
<p class='mut'>Seam at 5 s · case {esc(current.rsplit('-',1)[1])}</p>
{grades}
<p class='claim model-context'>Model 1f31b3d: {claim}</p>
<div class='comparison candidate-comparison'><table aria-label='Algorithm comparison for this source pair'><thead><tr><th>Version</th><th>Outcome</th><th>Reason</th></tr></thead><tbody>{rows}</tbody></table><div class='decision-legend' aria-label='Algorithm decision legend'><span>Algorithm:</span>{decision_badge('join')}{decision_badge('split')}</div></div>
<p class='mut speed-links'>HD · Speed: {' · '.join(f"<a href='{link(current,speed=s)}'>{s}×</a>" for s in [.5,1,2])} · Keys: , / . frames · j seam</p>
<details><summary>Prior votes and evidence</summary>{human}<p>Candidate <code>{self.config['revision']}</code></p><p>Exact frames cover {float(frame['min_pts']):.3f}–{float(frame['max_pts']):.3f} s. Original HD preview reused unchanged; grades are bound to this revision.</p><p><a href='/evidence/{self.config['evidence_filename']}'>Immutable candidate evidence</a></p><p>Seven real false splits remain; s25 is case-specifically inconclusive and needs no repeat grade.</p></details></section></article>"""
        js = f"const ITEM={json.dumps(current)},PREV={json.dumps(previous)},NEXT={json.dumps(following)},SPEED={speed},PRIORITY={json.dumps(not reference and not show_all)},RESUME={json.dumps(route)},REVIEW_TARGET={json.dumps(target)},FRAME_PREVIEW={json.dumps(frame)},REFERENCE_ONLY={json.dumps(reference)};" + self.slide_js + self.frame_js + PLAYBACK_JS
        return self.page('Candidate 1f31b3d · '+case['short_label'], body, js)
