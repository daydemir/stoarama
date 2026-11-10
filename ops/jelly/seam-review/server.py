#!/usr/bin/env python3
"""Seam review app: Good/Bad verdicts per seam, stored in a local SQLite DB."""
import html
import json
import sqlite3
import sys
import time
import re
from datetime import datetime, timezone
from http.server import ThreadingHTTPServer, SimpleHTTPRequestHandler
from pathlib import Path
from urllib.parse import urlparse, parse_qs

ROOT = Path(__file__).resolve().parent
DB = ROOT / "review.db"
ITEMS = json.loads((ROOT / "items.json").read_text())
BY_ID = {it["id"]: it for it in ITEMS}
SETS = {
    "canary": "Collation v2 canary (gates the backfill)",
    "proof": "Seam proof: real + deliberately broken cases",
    "canary-v3-20261002": "Collation v3 (2026-10-02): source-proven joins, prior misses, blind test cases",
}

# Stable priority ids only; no expected verdicts are sent to reviewers.
PRIORITY_SET = "canary-v3-20261002"
PRIORITY_ENTRY = "canary-v3-20261002-103f7e"
PRIORITY_IDS = ["canary-v3-20261002-" + suffix for suffix in (
    "996a77", "b94cbc", "4893bf", "037c9d", "370a36",
    "6fe971", "290b88", "3aa8b4", "838492", "9fac61",
    "103f7e", "de49fb", "5e44dd",
    "1d7374", "348af7", "bba188", "3bf2a4",
)]

FRAME_MAP_PATH = ROOT / "frame-review-map.json"
FRAME_MAP = json.loads(FRAME_MAP_PATH.read_text()) if FRAME_MAP_PATH.exists() else {}
def frame_preview_for(video):
    return json.loads(FRAME_MAP_PATH.read_text()).get(video) if FRAME_MAP_PATH.exists() else None
FRAME_JS = (ROOT / "frame-controls.js").read_text() if (ROOT / "frame-controls.js").exists() else ""
COMPARISON_PATH = ROOT / "comparison.json"
COMPARISON = json.loads(COMPARISON_PATH.read_text()) if COMPARISON_PATH.exists() else None
COMPARISON_CASES = {case["item_id"]: case for case in COMPARISON["cases"]} if COMPARISON else {}


def priority_items(items):
    rank = {iid: n for n, iid in enumerate(PRIORITY_IDS)}
    return sorted(items, key=lambda it: rank.get(it["id"], len(rank)))


def db():
    con = sqlite3.connect(DB)
    con.execute("create table if not exists votes (id text primary key, verdict text not null, note text, updated_at real not null)")
    con.execute("create table if not exists history (id text, verdict text, note text, at real)")
    return con


def votes():
    with db() as con:
        return {r[0]: {"verdict": r[1], "note": r[2] or ""} for r in con.execute("select id, verdict, note from votes")}


def review_target(case, variant):
    target = dict(case["grading_target"])
    target.update(preview_variant=variant,
                  preview_sha256=case["preview_hashes"].get(variant),
                  evidence_sha256=COMPARISON["evidence_sha256"])
    return target


def vote_context(iid, supplied, referer):
    """Bind new events only; old V2 votes are never assigned a V3 target."""
    case = COMPARISON_CASES.get(iid)
    if not case:
        if supplied is not None:
            raise ValueError("item has no V3 grading target")
        return None
    if supplied is not None:
        if not isinstance(supplied, dict):
            raise ValueError("invalid grading target")
        variant = supplied.get("preview_variant")
        if variant not in ("hd", "light"):
            raise ValueError("invalid preview variant")
        expected = review_target(case, variant)
        # Immutable older pages remain usable after a priority-only update.
        for key, value in expected.items():
            if key != "evidence_sha256" and supplied.get(key) != value:
                raise ValueError("grading target changed")
        evidence_sha = supplied.get("evidence_sha256", "")
        if not isinstance(evidence_sha, str) or not re.fullmatch(r"[0-9a-f]{64}", evidence_sha):
            raise ValueError("invalid evidence identity")
        if not (ROOT / "comparison-evidence" / ("evidence-" + evidence_sha + ".json")).is_file():
            raise ValueError("unknown evidence identity")
        frame_context = supplied.get("frame_review")
        if frame_context is not None:
            if variant != "hd" or not isinstance(frame_context, dict):
                raise ValueError("frame view requires HD identity")
            index_sha = frame_context.get("index_sha256", "")
            if not isinstance(index_sha, str) or not re.fullmatch(r"[0-9a-f]{64}", index_sha):
                raise ValueError("invalid frame index identity")
            index_path = ROOT / "frame-review" / expected["preview_sha256"] / ("index-" + index_sha + ".json")
            if not index_path.is_file():
                raise ValueError("unknown frame index")
            index = json.loads(index_path.read_text())
            n = frame_context.get("frame_index")
            if type(n) is not int or n < 0 or n >= len(index["frames"]) or index["source_sha256"] != expected["preview_sha256"]:
                raise ValueError("invalid frame selection")
            frame = index["frames"][n]
            if not frame["image_sha256"] or frame_context != {"index_sha256": index_sha, "frame_index": n, "pts": frame["pts"], "image_sha256": frame["image_sha256"]}:
                raise ValueError("frame identity differs")
            expected["frame_review"] = frame_context
        expected.update(evidence_sha256=evidence_sha, binding_source="explicit_page")
        return expected
    # Already-open original V3 pages do not send a target. Their immutable item
    # identity still defines V3; record only the preview variant we can observe.
    u = urlparse(referer or "")
    variant = "legacy_unknown"
    if u.path in ("/compare/" + COMPARISON["id"], "/slide/" + PRIORITY_SET):
        variant = "hd" if (parse_qs(u.query).get("hq") or ["0"])[0] == "1" else "light"
    context = review_target(case, variant)
    context["binding_source"] = "legacy_item_identity"
    return context


CSS = """
.navb{display:inline-block;padding:8px 14px;border:1px solid var(--line);border-radius:8px;text-decoration:none;margin-right:6px}.navb.off{opacity:.35}
:root{--bg:#fafaf8;--fg:#1c1c1a;--mut:#6b6b66;--card:#fff;--line:#e2e1dc;--good:#1f7a4d;--bad:#c0392b;--blue:#2f6fed;--orange:#f08c00;--red:#e53935}
@media (prefers-color-scheme:dark){:root{--bg:#141413;--fg:#ecebe6;--mut:#9d9c96;--card:#1e1e1c;--line:#34332f;--good:#4fbf86;--bad:#ff6b5b}}
*{box-sizing:border-box}body{margin:0;background:var(--bg);color:var(--fg);font:15px/1.45 -apple-system,system-ui,sans-serif}
main{max-width:1500px;margin:0 auto;padding:16px}a{color:var(--blue)}
h1{font-size:22px;margin:6px 0}h2{font-size:18px;margin:26px 0 8px;padding-top:14px;border-top:1px solid var(--line)}
.mut{color:var(--mut)}.bar{display:flex;gap:10px;flex-wrap:wrap;align-items:center;margin:8px 0 4px}
.legend span{display:inline-block;padding:2px 10px;border-radius:6px;color:#fff;font-weight:600;font-size:13px;margin-right:6px}
.grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(520px,1fr));gap:16px}
@media (max-width:600px){.grid{grid-template-columns:1fr}}
.card{background:var(--card);border:1px solid var(--line);border-radius:12px;overflow:hidden}
.card.good{outline:3px solid var(--good)}.card.bad{outline:3px solid var(--bad)}
.vwrap{background:#000}.vwrap video{width:100%;display:block}
.phasebar{height:26px;display:flex;align-items:center;justify-content:center;background:var(--card);border-top:1px solid var(--line)}
.phase{padding:2px 12px;border-radius:6px;font-weight:700;color:#fff;font-size:13px;letter-spacing:.04em}
.meta{padding:10px 12px}.meta p{margin:3px 0;font-size:13px}.title{font-weight:600}
.claim{display:inline-block;font-weight:700;font-size:13px;padding:2px 10px;border-radius:99px;border:2px solid currentColor;margin-bottom:4px}
.btns{display:flex;gap:8px;margin-top:8px}.btns button{flex:1;font:600 15px system-ui;padding:10px;border-radius:8px;border:2px solid var(--line);background:var(--card);color:var(--fg);cursor:pointer}
.btns button.g.on{background:var(--good);border-color:var(--good);color:#fff}.btns button.b.on{background:var(--bad);border-color:var(--bad);color:#fff}
.note{width:100%;margin-top:6px;padding:6px 8px;border:1px solid var(--line);border-radius:6px;background:var(--bg);color:var(--fg);font:13px system-ui}
.stat{font-weight:600}
.vwrap .frame-still{display:block;width:100%;height:auto}.vwrap video[hidden],.vwrap img[hidden]{display:none}
.frame-controls{padding:12px 16px;border-bottom:1px solid var(--line)}
.frame-toolbar{display:flex;gap:8px;flex-wrap:wrap;margin:0 0 10px}
.frame-toolbar button{min-height:44px;padding:8px 12px;border:1px solid var(--line);border-radius:6px;background:var(--card);color:var(--fg);font:inherit;cursor:pointer}
.frame-toolbar button:hover:not(:disabled){background:var(--bg)}.frame-toolbar button:disabled{opacity:.5;cursor:default}
.frame-toolbar button:focus-visible,.frame-controls input:focus-visible{outline:3px solid var(--blue);outline-offset:2px}
.frame-controls progress{width:100%;height:8px;accent-color:var(--blue)}.frame-controls progress[hidden]{display:none}
.frame-controls p{margin:6px 0}.frame-status{font-variant-numeric:tabular-nums;min-height:1.5em}
.frame-controls input[type=range]{width:100%;min-height:36px;accent-color:var(--blue)}
@media(max-width:600px){.frame-controls{padding:12px}.frame-toolbar{display:grid;grid-template-columns:1fr 1fr}.frame-toolbar button{width:100%}}
.comparison{margin:12px 0}.comparison table{border-collapse:collapse;width:100%;font-size:13px;text-align:left}
.comparison th,.comparison td{padding:7px 8px;border-bottom:1px solid var(--line);vertical-align:top;overflow-wrap:anywhere}
.comparison th:first-child{width:25%}.comparison td:nth-child(2){width:18%;font-weight:700}
.comparison small{display:block;color:var(--mut);font-weight:400}.comparison code{font-size:12px;overflow-wrap:anywhere}
.comparison details{margin:10px 0}.comparison summary{cursor:pointer}.comparison .prior{margin:8px 0}
.comparison-note-label{display:block;margin-top:8px;font-size:13px;font-weight:600}
@media(max-width:600px){.comparison table{table-layout:fixed}.comparison th,.comparison td{padding:6px 4px}.comparison th:first-child{width:22%}.comparison th:nth-child(2),.comparison td:nth-child(2){width:28%}}
"""

JS = """
const rate=v=>{v.defaultPlaybackRate=2;v.playbackRate=2;};
document.querySelectorAll('.vwrap').forEach(w=>{const v=w.querySelector('video'),p=w.closest('.card').querySelector('.phase');
 rate(v);['loadedmetadata','play'].forEach(e=>v.addEventListener(e,()=>rate(v)));
 v.addEventListener('timeupdate',()=>{const t=v.currentTime;
  if(t<4.6){p.textContent='BEFORE';p.style.background='var(--blue)'}else if(t<=5.4){p.textContent='SEAM';p.style.background='var(--red)'}else{p.textContent='AFTER';p.style.background='var(--orange)'}});});
async function vote(id,verdict){const card=document.getElementById(id);const note=card.querySelector('.note').value;
 const cur=card.dataset.verdict; if(cur===verdict) verdict='clear';
 const r=await fetch('/api/vote',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({id,verdict,note})});
 if(!r.ok){alert('save failed');return}
 card.dataset.verdict=verdict==='clear'?'':verdict; card.classList.remove('good','bad'); if(verdict!=='clear') card.classList.add(verdict);
 card.querySelector('.g').classList.toggle('on',verdict==='good'); card.querySelector('.b').classList.toggle('on',verdict==='bad'); tally();}
async function saveNote(id){const card=document.getElementById(id);const v=card.dataset.verdict;if(!v)return;
 await fetch('/api/vote',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({id,verdict:v,note:card.querySelector('.note').value})});}
function tally(){const cs=[...document.querySelectorAll('.card')];const g=cs.filter(c=>c.dataset.verdict==='good').length,b=cs.filter(c=>c.dataset.verdict==='bad').length;
 document.getElementById('tally').textContent=`${g} good · ${b} bad · ${cs.length-g-b} left of ${cs.length}`;}
tally();
"""



PHASE_BORDER_CSS = """
.vwrap[data-review-phase]{position:relative}
.vwrap[data-review-phase]::after{content:"";position:absolute;inset:0;border:4px solid var(--review-phase-color,var(--blue));border-radius:inherit;pointer-events:none;z-index:10}
"""
PHASE_BORDER_JS = """
(()=>{document.querySelectorAll('.vwrap').forEach(w=>{
 const phase=w.closest('.card')?.querySelector('.phase');
 if(!phase)return;
 const sync=()=>{const name=phase.textContent.trim().toLowerCase();
  const color={before:'blue',seam:'red',after:'orange'}[name];
  if(!color)return;w.dataset.reviewPhase=name;w.style.setProperty('--review-phase-color','var(--'+color+')');};
 sync();new MutationObserver(sync).observe(phase,{childList:true,subtree:true,characterData:true});
});})();
"""

def page(title, body, extra_js=""):
    phase_css = PHASE_BORDER_CSS if "<video" in body else ""
    phase_js = "<script>" + PHASE_BORDER_JS + "</script>" if "<video" in body else ""
    return f"""<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>{html.escape(title)}</title><style>{CSS}{phase_css}</style></head><body><main>{body}</main><script>{extra_js}</script>{phase_js}</body></html>"""


def index():
    v = votes()
    rows = []
    for key, label in SETS.items():
        its = [i for i in ITEMS if i["set"] == key]
        g = sum(1 for i in its if v.get(i["id"], {}).get("verdict") == "good")
        b = sum(1 for i in its if v.get(i["id"], {}).get("verdict") == "bad")
        rows.append(f"<h2><a href='slide/{key}'>{html.escape(label)}</a></h2><p class='stat'>{g} good · {b} bad · {len(its)-g-b} left of {len(its)}</p>"
                    f"<p><a href='slide/{key}'><b>▶ Review one at a time</b></a> (loads one video) · <a href='set/{key}'>Grid view (loads all)</a> · <a href='set/{key}?only=left'>Show only unreviewed</a> · <a href='set/{key}?only=bad'>Show only bad</a></p>")
    comparison_link = (f"<h2><a href='/compare/{COMPARISON['id']}'>V1 / V2 / V3 comparison</a></h2>"
                       f"<p>{COMPARISON['summary']['cases']} unique seams · disagreements and safety checks first · existing votes retained</p>") if COMPARISON else ""
    body = ("<h1>Seam review</h1>" + comparison_link + "<p class='mut'>Each video plays the last 5 s before a seam and the first 5 s after it, at 2×. "
            "The label below each video marks BEFORE, SEAM, and AFTER without altering the frames. Mark <b>Good</b> if our claim is right, <b>Bad</b> if it's wrong. Click again to clear. Votes save instantly.</p>"
            + "".join(rows) + "<p class='mut'><a href='export.csv'>Download votes (CSV)</a></p>")
    return page("Seam Review", body)


def set_page(key, only):
    v = votes()
    its = [i for i in ITEMS if i["set"] == key]
    if only == "left":
        its = [i for i in its if i["id"] not in v]
    elif only == "bad":
        its = [i for i in its if v.get(i["id"], {}).get("verdict") == "bad"]

    def card(it):
        vv = v.get(it["id"], {})
        verdict = vv.get("verdict", "")
        claim = "We claim: CONTINUOUS (join)" if it["claim"] == "continuous" else "We claim: JUMP (split)"
        color = "var(--good)" if it["claim"] == "continuous" else "var(--bad)"
        lines = "".join(f"<p class='mut'>{html.escape(l)}</p>" for l in it.get("lines", []))
        return (f"<article class='card {verdict}' id='{it['id']}' data-verdict='{verdict}'>"
                f"<div class='vwrap'><video src='../{it['video']}' muted autoplay loop playsinline controls preload='auto'></video></div><div class='phasebar'><span class='phase'>BEFORE</span></div>"
                f"<div class='meta'><span class='claim' style='color:{color}'>{claim}</span><p class='title'>{html.escape(it.get('title',''))}</p>{lines}"
                f"<div class='btns'><button class='g{' on' if verdict=='good' else ''}' onclick=\"vote('{it['id']}','good')\">Good</button>"
                f"<button class='b{' on' if verdict=='bad' else ''}' onclick=\"vote('{it['id']}','bad')\">Bad</button></div>"
                f"<input class='note' placeholder='Optional note' value='{html.escape(vv.get('note',''))}' onchange=\"saveNote('{it['id']}')\"></div></article>")

    cont = [i for i in its if i["claim"] == "continuous"]
    jump = [i for i in its if i["claim"] == "jump"]
    body = (f"<p><a href='../'>← All sets</a></p><h1>{html.escape(SETS[key])}</h1>"
            "<div class='bar legend'><span style='background:var(--blue)'>BEFORE</span><span style='background:var(--red)'>SEAM</span><span style='background:var(--orange)'>AFTER</span>"
            "<span class='stat' id='tally' style='color:var(--fg);padding:0'></span></div>"
            f"<h2>We claim CONTINUOUS: these get joined ({len(cont)})</h2><div class='grid'>{''.join(card(i) for i in cont)}</div>"
            f"<h2>We claim JUMP: these get split ({len(jump)})</h2><div class='grid'>{''.join(card(i) for i in jump)}</div>")
    return page(SETS[key], body, JS)


SLIDE_JS = """
let saving=false;
function votePayload(verdict){const body={id:ITEM,verdict,note:document.querySelector('.note').value};
 if(typeof REVIEW_TARGET!=='undefined' && REVIEW_TARGET){
  body.review_target=REVIEW_TARGET;
  const frame=typeof activeFrameContext==='function'?activeFrameContext():null;
  if(frame)body.review_target={...REVIEW_TARGET,preview_variant:'hd',preview_sha256:FRAME_PREVIEW.source_sha256,frame_review:frame};
 }return JSON.stringify(body);}
const v=document.querySelector('video'),p=document.querySelector('.phase');
const rate=()=>{v.defaultPlaybackRate=SPEED;v.playbackRate=SPEED;};
rate();['loadedmetadata','play'].forEach(e=>v.addEventListener(e,rate));
v.addEventListener('timeupdate',()=>{if(typeof frameViewIsActive==='function' && frameViewIsActive())return;const t=v.currentTime;
 if(t<4.6){p.textContent='BEFORE';p.style.background='var(--blue)'}else if(t<=5.4){p.textContent='SEAM';p.style.background='var(--red)'}else{p.textContent='AFTER';p.style.background='var(--orange)'}});
async function vote(verdict){const card=document.querySelector('.card');const cur=card.dataset.verdict;
 if(saving)return;
 if(cur===verdict) verdict='clear';
 saving=true;document.querySelectorAll('.btns button').forEach(b=>b.disabled=true);
 let r;try{r=await fetch('/api/vote',{method:'POST',headers:{'Content-Type':'application/json'},body:votePayload(verdict)});}
 catch(e){alert('save failed - check connection and try again');return}
 finally{saving=false;document.querySelectorAll('.btns button').forEach(b=>b.disabled=false);}
 if(!r.ok){alert('save failed - check connection and try again');return}
 card.dataset.verdict=verdict==='clear'?'':verdict;card.classList.remove('good','bad');if(verdict!=='clear')card.classList.add(verdict);
 document.querySelector('.g').classList.toggle('on',verdict==='good');document.querySelector('.b').classList.toggle('on',verdict==='bad');
 document.getElementById('saved').textContent=verdict==='clear'?'cleared':'saved: '+verdict;
 if(PRIORITY && verdict!=='clear')location.href=RESUME;}
async function saveNote(){const v2=document.querySelector('.card').dataset.verdict;if(!v2)return;
 await fetch('/api/vote',{method:'POST',headers:{'Content-Type':'application/json'},body:votePayload(v2)});
 document.getElementById('saved').textContent='note saved';}
document.addEventListener('keydown',e=>{if(e.ctrlKey||e.metaKey||e.altKey||e.target.closest('input,textarea,select,[contenteditable="true"]')||saving)return;
 if(e.key==='g')vote('good');else if(e.key==='b')vote('bad');
 else if(e.key==='ArrowRight'||e.key==='n'){if(NEXT)location.href=NEXT}else if(e.key==='ArrowLeft'||e.key==='p'){if(PREV)location.href=PREV}
 else if(e.key===' ' && e.target.tagName!=='BUTTON'){e.preventDefault();if(typeof frameViewIsActive==='function' && frameViewIsActive())resumeFrameVideo();else v.paused?v.play():v.pause()}});
"""


def comparison_panel(case):
    reasons = {
        "source_adjacent": "HRD timing supports adjacent source frames",
        "frame_jump": "Frame comparison rejects continuity",
        "continuous": "Frame comparison accepts continuity",
        "prev_clip_cut_short": "Previous clip ends early",
        "content_span_mismatch": "Media duration differs from the recorded span",
        "gop_truncated": "Previous clip ends inside a GOP",
        "packet_replay": "Repeated source packets",
        "source_discontinuity": "Source timing contradicts continuity",
        "same_verified_output": "Both sources are consecutive in one verified output",
        "separate_verified_outputs": "Sources are in separate verified outputs",
        "signed_presentation_gap": "Historical timing boundary",
        "source_quarantined": "Historical source quarantine",
    }
    rows = []
    for version in ("v1", "v2", "v3"):
        result = case[version]
        basis = result.get("basis", "unavailable")
        label = version.upper()
        qualifier = "Historical output" if basis == "historical" else "Fresh run" if basis == "fresh" else "Recorded Oct 2" if basis == "recorded" else "Unavailable"
        reason = result.get("reason", "")
        friendly = reasons.get(reason, reason.replace("_", " "))
        code = f"<small><code>{html.escape(reason)}</code></small>" if reason in reasons else ""
        rows.append(f"<tr><th scope='row'>{label}<small>{qualifier}</small></th><td>{html.escape(result['decision'].upper())}</td><td>{html.escape(friendly)}{code}</td></tr>")
    with db() as con:
        related = case.get("related_vote_ids", [case["item_id"]])
        marks = ",".join("?" for _ in related)
        history = list(con.execute(f"select id,verdict,note,at from history where id in ({marks}) order by at", related))
    previous = []
    for iid, verdict, note, at in history:
        old = BY_ID.get(iid, {})
        claim = "JOIN" if old.get("claim") == "continuous" else "SPLIT"
        stamp = datetime.fromtimestamp(at, timezone.utc).strftime("%b %d %H:%M UTC")
        version = "V2 canary" if old.get("set") == "canary" else "V3 canary" if old.get("set") == PRIORITY_SET else "V3 comparison"
        detail = f"{stamp}: {version} · {verdict.title()} on {claim} · {iid}"
        if note:
            detail += " · " + note
        previous.append(f"<li>{html.escape(detail)}</li>")
    prior = (f"<details><summary>Previous votes · {len(history)} saved events</summary><p class='mut'>Good confirms that version's claim; Bad rejects it. V2 votes are historical references, not V3 approval. These records are unchanged.</p><ul>{''.join(previous)}</ul></details>") if history else "<p class='mut prior'>No previous vote for this seam.</p>"
    source = "Fresh V2 and V3 runs used identical, hash-verified original clips." if case["basis"] == "fresh" else "These are retained October 2 decisions, not a fresh rerun."
    limitation = case.get("limitation", "")
    v1_note = "V1 is an observed historical output, not a reproduced worker run."
    if case["v1"].get("basis") == "historical" and not case["v1"].get("same_metadata", True):
        v1_note += " Its frozen metadata differs from the fresh-run metadata."
    evidence_url = f"/evidence/{COMPARISON['evidence_filename']}"
    return (f"<div class='comparison'><table><thead><tr><th scope='col'>Algorithm</th><th scope='col'>Outcome</th><th scope='col'>Reason</th></tr></thead><tbody>{''.join(rows)}</tbody></table>"
            f"<p class='mut'>{source} {html.escape(limitation)}</p>{prior}"
            f"<details><summary>Versions and immutable evidence</summary><p>{v1_note} Excluded means V1 published no media for one or both sources; it is not a split verdict.</p>"
            f"<p>V2: collation-v2 · V3: collation-v3 · code {COMPARISON['versions']['code_commit'][:12]}. Fresh decoder: {html.escape(COMPARISON['versions']['tools']['ffmpeg']['version'])}.</p>"
            f"<p>One preview represents this boundary; matching algorithm decisions do not create duplicate review items.</p>"
            f"<p><a href='{evidence_url}'>Read immutable comparison evidence</a> · SHA-256 <code>{COMPARISON['evidence_sha256']}</code></p></details></div>")


def slide_page(key, cur_id, speed, show_all, hq, priority=False, priority_entry=False, comparison=False):
    v = votes()
    full = [BY_ID[case["item_id"]] for case in COMPARISON["cases"] if show_all or case.get("default_queue", True)] if comparison else [i for i in ITEMS if i["set"] == key]
    priority = (comparison or (priority and key == PRIORITY_SET)) and not show_all
    if priority:
        if not comparison:
            full = priority_items(full)
        pending = [i for i in full if i["id"] not in v]
        if not pending:
            all_url = f"/compare/{key}?all=1" if comparison else f"?id={PRIORITY_ENTRY}&all=1"
            return page("Priority review complete", "<h1>Priority review complete</h1>"
                        f"<p>All {len(full)} items are graded. Your votes and notes are saved.</p>"
                        f"<p><a href='{all_url}'>Review all grades</a> · <a href='/'>All sets</a></p>")
        if priority_entry or cur_id in v:
            cur_id = pending[0]["id"]
    ids = [i["id"] for i in full]
    # The queue hides items already graded Good (unless show_all); the item on
    # screen always stays so a fresh vote doesn't yank it away.
    def in_queue(i):
        if priority:
            return i["id"] not in v
        return show_all or v.get(i["id"], {}).get("verdict") != "good" or i["id"] == cur_id
    if cur_id not in ids:
        cur_id = next((i["id"] for i in full if i["id"] not in v), None) or next((i["id"] for i in full if in_queue(i)), ids[0])
    pos = ids.index(cur_id)
    it = full[pos]
    prev_it = next((i for i in reversed(full[:pos]) if in_queue(i)), None)
    next_it = next((i for i in full[pos + 1:] if in_queue(i)), None)
    queue = [i for i in full if in_queue(i)]
    vv = v.get(it["id"], {})
    verdict = vv.get("verdict", "")
    g = sum(1 for i in full if v.get(i["id"], {}).get("verdict") == "good")
    bd = sum(1 for i in full if v.get(i["id"], {}).get("verdict") == "bad")
    owner = "V3 (d50ce5c) claims" if comparison else "We claim"
    claim = owner + (": CONTINUOUS (join)" if it["claim"] == "continuous" else ": JUMP (split)")
    color = "var(--good)" if it["claim"] == "continuous" else "var(--bad)"
    lines = "".join(f"<p class='mut'>{html.escape(l)}</p>" for l in it.get("lines", []))

    def link(item_id, **over):
        params = {"id": item_id, "speed": speed, "all": 1 if show_all else 0, "hq": 1 if hq else 0}
        if priority:
            params.update(priority=1, item=1)
        params.update(over)
        prefix = f"/compare/{key}?" if comparison else "?"
        return prefix + "&".join(f"{k}={v2}" for k, v2 in params.items() if not (k == "all" and not v2) and not (k == "speed" and v2 == 2))

    prev_url = link(prev_it["id"]) if prev_it else ""
    next_url = link(next_it["id"]) if next_it else ""
    nav = ("<div class='btns'>"
           + (f"<a class='navb' href='{prev_url}'>← Prev</a>" if prev_url else "<span class='navb off'>← Prev</span>")
           + (f"<a class='navb' href='{next_url}'>Next →</a>" if next_url else "<span class='navb off'>Next →</span>")
           + "</div>")
    speeds = " · ".join((f"<b>{sp}×</b>" if sp == speed else f"<a href='{link(it['id'], speed=sp)}'>{sp}×</a>") for sp in (0.5, 1, 2))
    qual = (f"<b>HD</b> · <a href='{link(it['id'], hq=0)}'>light</a>" if hq else f"<b>light</b> · <a href='{link(it['id'], hq=1)}'>HD</a>")
    filt = (f"<a href='{link(it['id'], all=0)}'>hide Good-graded</a>" if show_all else f"Good-graded hidden · <a href='{link(it['id'], all=1)}'>show all</a>")
    if priority:
        filt = f"Priority order · Graded items hidden · <a href='{link(it['id'], all=1)}'>review all</a>"
    src = it["video"] if hq else it["video"].replace("vid/", "vid-lo/", 1)
    qn = next((n for n, q in enumerate(queue) if q["id"] == it["id"]), 0) + 1
    priority_hint = "<p class='mut'>Add any note before grading. Good or Bad saves your vote and opens the highest-priority remaining case.</p>" if priority else ""
    case = COMPARISON_CASES.get(it["id"]) if comparison else None
    if case:
        lines = f"<details class='comparison'><summary>Original item annotations (unchanged)</summary>{lines}</details>"
    panel = comparison_panel(case) if case else ""
    heading = (f"<h1>Grade V3 · d50ce5c</h1><p>{html.escape(case['group'])}</p>"
               f"<p class='mut'>You are grading V3's join or split. V2 and V1 are comparison references. V3/V2 differences come first, with the known false-join regression pinned above them. "
               f"<a href='/slide/{PRIORITY_SET}?legacy=1'>Original V3 set</a> · <a href='/slide/canary?all=1'>Original V2 set</a></p>") if case else ""
    frame_preview = frame_preview_for(it["video"])
    frame_controls = ("<section class='frame-controls' aria-label='Exact frame review'>"
        "<div class='frame-toolbar'><button type='button' id='frame-previous' disabled aria-keyshortcuts=','>Previous frame</button>"
        "<button type='button' id='frame-next' disabled aria-keyshortcuts='.'>Next frame</button>"
        "<button type='button' id='frame-seam' disabled aria-keyshortcuts='j'>Jump to seam</button>"
        "<button type='button' id='frame-resume' disabled>Resume video</button></div>"
        "<button type='button' id='frame-retry' hidden>Retry preload</button>"
        f"<progress id='frame-progress' value='0' max='{frame_preview['max_frame']-frame_preview['min_frame']+1}' aria-label='Exact frames loaded'></progress>"
        "<p id='frame-status' class='frame-status' role='status' aria-live='polite'>Loading the review clip and exact frames…</p>"
        "<label for='frame-range'>Browse decoded HD frames</label>"
        f"<input id='frame-range' type='range' disabled min='{frame_preview['min_frame']}' max='{frame_preview['max_frame']}' value='{frame_preview['seam_frame_index']}'>"
        f"<p class='mut'>Exact frames cover {float(frame_preview['min_pts']):.3f}–{float(frame_preview['max_pts']):.3f} s. Keys: , previous · . next · j seam. The seam starts at 5 s.</p></section>") if frame_preview else ""
    note_label = "<label class='comparison-note-label' for='review-note'>Note (optional)</label>" if comparison else ""
    good_label = "V3 is right (g)" if comparison else "Good (g)"
    bad_label = "V3 is wrong (b)" if comparison else "Bad (b)"
    video_source = f"data-preload-src='../{src}'" if frame_preview and hq else f"src='../{src}'"
    body = (heading + f"<p><a href='/'>← All sets</a> · <span class='stat'>{qn} / {len(queue)} in queue</span> · {g} good · {bd} bad · {len(full)-g-bd} left of {len(full)} · {filt}</p>"
            f"{priority_hint}"
            f"<article class='card {verdict}' data-verdict='{verdict}' style='max-width:900px;margin:0 auto'>"
            f"<div class='vwrap'><video {video_source} muted autoplay loop playsinline controls preload='auto'></video>" + (f"<img class='frame-still' src='data:image/gif;base64,R0lGODlhAQABAIAAAAAAAP///yH5BAEAAAAALAAAAAABAAEAAAIBRAA7' hidden alt='Decoded HD frame'>" if frame_preview else "") + "</div>"
            f"<div class='phasebar'><span class='phase'>BEFORE</span></div>{frame_controls}"
            f"<div class='meta'><span class='claim' style='color:{color}'>{claim}</span><p class='title'>{html.escape(it.get('title',''))} <span class='mut'>({it['id']})</span></p>{panel}{lines}"
            f"<p class='mut'>{'Good confirms V3’s claim above; Bad rejects it. A Good on JUMP confirms a correct split.' if comparison else ''}</p>"
            f"<div class='btns'><button class='g{' on' if verdict=='good' else ''}' onclick=\"vote('good')\">{good_label}</button>"
            f"<button class='b{' on' if verdict=='bad' else ''}' onclick=\"vote('bad')\">{bad_label}</button> <span id='saved' class='mut'></span></div>"
            f"{note_label}<input id='review-note' class='note' placeholder='Optional note' value='{html.escape(vv.get('note',''))}' onchange='saveNote()'>"
            f"{nav}<p class='mut'>Video: {qual} · Speed: {speeds} · keys: g/b vote, ←/→ prev/next, space pause</p></div></article>")
    resume = f"?id={PRIORITY_ENTRY}&priority=1&speed={speed}&hq={int(hq)}"
    if comparison:
        resume = f"/compare/{key}?speed={speed}&hq={int(hq)}"
    target = review_target(case, "hd" if hq else "light") if case else None
    js = f"const ITEM={json.dumps(it['id'])},PREV={json.dumps(prev_url)},NEXT={json.dumps(next_url)},SPEED={speed},PRIORITY={json.dumps(priority)},RESUME={json.dumps(resume)},REVIEW_TARGET={json.dumps(target)},FRAME_PREVIEW={json.dumps(frame_preview)};" + SLIDE_JS + FRAME_JS
    title = COMPARISON["title"] if comparison else SETS[key]
    return page(f"{qn}/{len(queue)} · {title}", body, js)

from candidate_review import CandidateReview
CANDIDATE_REVIEW = CandidateReview(ROOT, page, SLIDE_JS, FRAME_JS)
from repair_review import RepairReview
REPAIR_REVIEW = RepairReview(ROOT, page, SLIDE_JS, FRAME_JS)

import coverage_downloads
import review_asset_r2
import coverage_reviews
from window20_review import Window20Review
WINDOW20_REVIEW = Window20Review(ROOT, page, SLIDE_JS, FRAME_JS)
UNCERTAIN_REVIEW = Window20Review(ROOT, page, SLIDE_JS, FRAME_JS,filename="candidate-jaworzno-uncertain-20261009.json",expected_id="jaworzno-uncertain-20261009",vote_endpoint="/api/jaworzno-uncertain-vote")
import reviewed_repairs
import confident_repairs
import review_queue
from holdout_review import HoldoutReview
HOLDOUT_REVIEW = HoldoutReview(ROOT, page, SLIDE_JS, FRAME_JS)
from holdout_registry import HoldoutRegistry
HOLDOUT_BATCHES = HoldoutRegistry(ROOT, page, SLIDE_JS, FRAME_JS, HOLDOUT_REVIEW)

class H(SimpleHTTPRequestHandler):
    def __init__(self, *a, **k):
        super().__init__(*a, directory=str(ROOT), **k)

    def log_message(self, *a):
        pass

    def send(self, code, body, ctype="text/html; charset=utf-8"):
        data = body.encode() if isinstance(body, str) else body
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(data)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self):
        u = urlparse(self.path)
        q = parse_qs(u.query)
        if review_asset_r2.get(self, u, q):
            return
        if review_queue.get(self, u, q, HOLDOUT_BATCHES.reviewers() + [UNCERTAIN_REVIEW, WINDOW20_REVIEW, CANDIDATE_REVIEW], page):
            return
        if HOLDOUT_BATCHES.get(self, u, q):
            return
        if coverage_reviews.get(self, u, q):
            return
        if UNCERTAIN_REVIEW.get(self, u, q):
            return
        if confident_repairs.get(self, u, q):
            return
        if reviewed_repairs.get(self, u, q):
            return
        if WINDOW20_REVIEW.get(self, u, q):
            return
        if coverage_downloads.get(self, u, q):
            return
        if CANDIDATE_REVIEW.get(self, u, q):
            return
        if REPAIR_REVIEW.get(self, u, q):
            return
        if u.path in ("/", "/index.html"):
            return self.send(200, index())
        if re.fullmatch(r"/frame-review/[0-9a-f]{64}/(index-[0-9a-f]{64}\.json|frame-[0-9a-f]{64}\.png)", u.path):
            asset = ROOT / u.path.lstrip("/")
            if asset.is_file():
                return self.send(200, asset.read_bytes(), "application/json" if asset.suffix == ".json" else "image/png")
            return self.send(404, "unknown frame asset")
        if COMPARISON and re.fullmatch(r"/evidence/evidence-[0-9a-f]{64}\.json", u.path):
            evidence = ROOT / "comparison-evidence" / u.path.rsplit("/", 1)[1]
            if evidence.is_file():
                return self.send(200, evidence.read_bytes(), "application/json")
            return self.send(404, "unknown evidence")
        comparison = COMPARISON and u.path == f"/compare/{COMPARISON['id']}"
        if u.path.startswith("/slide/") or comparison:
            key = u.path.split("/")[2]
            if not comparison and key not in SETS:
                return self.send(404, "unknown set")
            try:
                speed = float((q.get("speed") or ["2"])[0])
            except ValueError:
                speed = 2
            speed = speed if speed in (0.5, 1, 2) else 2
            speed = int(speed) if speed == int(speed) else speed
            cur = (q.get("id") or [""])[0]
            if not cur and "i" in q:
                full = [i["id"] for i in ITEMS if i["set"] == key]
                try:
                    cur = full[max(0, min(int(q["i"][0]), len(full) - 1))]
                except ValueError:
                    cur = ""
            show_all = (q.get("all") or ["0"])[0] == "1"
            entry = key == PRIORITY_SET and not show_all and (not cur or cur == PRIORITY_ENTRY) and (q.get("item") or ["0"])[0] != "1"
            if comparison or (COMPARISON and entry and (q.get("legacy") or ["0"])[0] != "1"):
                return self.send(200, slide_page(COMPARISON["id"], cur if comparison else "", speed, show_all,
                                               (q.get("hq") or ["1"])[0] == "1", True, not cur or not comparison, True))
            priority = entry or (q.get("priority") or ["0"])[0] == "1"
            return self.send(200, slide_page(key, cur, speed, show_all, (q.get("hq") or ["0"])[0] == "1", priority, entry))
        if u.path.startswith("/set/"):
            key = u.path.split("/")[2]
            if key not in SETS:
                return self.send(404, "unknown set")
            return self.send(200, set_page(key, (q.get("only") or [""])[0]))
        if u.path == "/api/votes":
            return self.send(200, json.dumps(votes()), "application/json")
        if u.path == "/export.csv":
            v = votes()
            lines = ["id,set,claim,verdict,note"] + [
                ",".join([i["id"], i["set"], i["claim"], v.get(i["id"], {}).get("verdict", ""), '"' + v.get(i["id"], {}).get("note", "").replace('"', "'") + '"'])
                for i in ITEMS]
            return self.send(200, "\n".join(lines), "text/csv")
        if u.path.startswith("/vid/") or u.path.startswith("/vid-lo/"):
            return super().do_GET()
        return self.send(404, "not found")

    def do_HEAD(self):
        u = urlparse(self.path)
        q = parse_qs(u.query)
        if review_asset_r2.head(self, u, q):
            return
        if HOLDOUT_BATCHES.head(self, u, q):
            return
        if confident_repairs.head(self, u, q):
            return
        if reviewed_repairs.head(self, u, q):
            return
        if coverage_downloads.head(self, u, q):
            return
        return super().do_HEAD()

    def do_POST(self):
        if HOLDOUT_BATCHES.post(self):
            return
        if urlparse(self.path).path == "/api/terminal-heldout-vote":
            return HOLDOUT_REVIEW.post(self)
        if urlparse(self.path).path == "/api/jaworzno-uncertain-vote":
            return UNCERTAIN_REVIEW.post(self)
        if urlparse(self.path).path == "/api/coverage-reviews":
            return coverage_reviews.post(self)
        if urlparse(self.path).path == "/api/window20-vote":
            return WINDOW20_REVIEW.post(self)
        if urlparse(self.path).path == "/api/repair-vote":
            return REPAIR_REVIEW.post(self)
        if urlparse(self.path).path == "/api/candidate-vote":
            return CANDIDATE_REVIEW.post(self)
        if urlparse(self.path).path != "/api/vote":
            return self.send(404, "not found")
        n = int(self.headers.get("Content-Length", "0"))
        try:
            req = json.loads(self.rfile.read(n))
            iid, verdict, note = req["id"], req["verdict"], str(req.get("note", ""))[:1000]
        except (ValueError, KeyError):
            return self.send(400, "bad request")
        if iid not in BY_ID or verdict not in ("good", "bad", "clear"):
            return self.send(400, "bad id or verdict")
        try:
            context = vote_context(iid, req.get("review_target"), self.headers.get("Referer"))
        except ValueError:
            return self.send(400, "invalid review target")
        now = time.time()
        with db() as con:
            if verdict == "clear":
                con.execute("delete from votes where id=?", (iid,))
            else:
                con.execute("insert into votes(id,verdict,note,updated_at) values(?,?,?,?) on conflict(id) do update set verdict=excluded.verdict, note=excluded.note, updated_at=excluded.updated_at", (iid, verdict, note, now))
            event = con.execute("insert into history values(?,?,?,?)", (iid, verdict, note, now))
            if context is not None:
                con.execute("create table if not exists vote_contexts (history_rowid integer primary key, item_id text not null, context text not null)")
                con.execute("insert into vote_contexts values(?,?,?)", (event.lastrowid, iid, json.dumps(context, sort_keys=True)))
        return self.send(200, '{"ok":true}', "application/json")


if __name__ == "__main__":
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 8475
    ThreadingHTTPServer(("127.0.0.1", port), H).serve_forever()
