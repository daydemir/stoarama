/* Selected-period weekly exports. Filename weeks remain calendar-month weeks. */
(()=>{'use strict';
const esc=s=>String(s??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
const choices=new Map(),kinds=new Map();let catalogPromise,reviewedPromise;
const add=(s,n)=>new Date(Date.parse(s+'T12:00:00Z')+n*86400000).toISOString().slice(0,10);
const label=s=>new Intl.DateTimeFormat('en-GB',{timeZone:'UTC',day:'numeric',month:'short'}).format(new Date(s+'T12:00:00Z'));
const range=(r,w)=>w?label(add(r.start,(w-1)*7))+'–'+label(add(r.start,w*7-1)):label(r.start)+'–'+label(r.end);
const reviewed=(r,scope)=>(kinds.get(r.id)||'reviewed')==='reviewed';
const command=(r,scope,w)=>'python3 '+(reviewed(r,scope)?'reviewed_downloader.py --stream '+r.id+' --scope '+scope:'coverage_downloader.py --stream '+r.id+' --scope '+scope)+(w?' --week '+w:'')+' --jobs 4 --destination ./stitched --server '+location.origin;
const script=(r,scope)=>reviewed(r,scope)?'/vid/jaworzno-reviewed-repairs-20261009/reviewed_downloader.py':'/api/coverage-downloads/downloader.py';
const help=(r,context)=>'<span class="weekly-help"><button type="button" class="weekly-help-toggle" aria-expanded="false" aria-describedby="weekly-help-'+r.id+'-'+context+'">ⓘ Setup</button><span id="weekly-help-'+r.id+'-'+context+'" role="tooltip" hidden>Python 3.9+ only; no extra packages. Download the script, then run the command in Terminal. Choose a destination with room for the GB shown. Rerun to resume; existing files are verified and kept. On Windows, <code>py -3</code> can replace <code>python3</code>.</span></span>';
function rowMarkup(r,scope,comparison){
 if(comparison)return '<p class="summary">Weekly export uses the original audited period; comparison-day files remain in Browse files.</p>';
 return '<section class="weekly-entry"><strong>Download dataset</strong><div class="weekly-row-actions">'+[1,2,0].map(w=>'<button type="button" class="weekly-open" data-stream="'+r.id+'" data-week="'+w+'">'+(w?'Week '+w:'All 14 days')+'<small>'+esc(range(r,w))+'</small></button>').join('')+'</div><a href="'+script(r,scope)+'" download>Get folder download script</a>'+help(r,'row')+'</section>';
}
function detailMarkup(r,scope,comparison){
 if(comparison)return '';
 const w=choices.has(r.id)?choices.get(r.id):1,repair=reviewed(r,scope);
 return '<section class="weekly-detail" data-stream="'+r.id+'" data-scope="'+scope+'"><h3>Download dataset by week</h3><p>Selected Week 1 = days 1–7; Week 2 = days 8–14. '+esc(r.timezone)+' local dates.</p>'+(false?'<label class="weekly-version">Files<select class="weekly-kind"><option value="reviewed"'+(repair?' selected':'')+'>Use verified assembled replacements</option><option value="original"'+(!repair?' selected':'')+'>Keep original V2 parts</option></select></label>':'')+'<div class="weekly-periods">'+[1,2,0].map(n=>'<button type="button" class="weekly-pick" data-week="'+n+'" aria-pressed="'+(n===w)+'">'+(n?'Week '+n:'All 14 days')+'<small>'+esc(range(r,n))+'</small></button>').join('')+'</div><p class="weekly-selection" role="status">Checking available files…</p><p><a class="weekly-script" href="'+script(r,scope)+'" download>1. Download the folder script</a>'+help(r,'script')+'</p><p>2. Copy this command and run it beside the saved script:</p><pre class="weekly-code"><code>'+esc(command(r,scope,w))+'</code></pre><button type="button" class="weekly-copy">Copy command</button>'+help(r,'command')+'<span class="weekly-copy-status" role="status"></span><p class="weekly-meaning">Files keep the actual month / weekday folders. Filename W1–W5 means the week within the calendar month: days 1–7, 8–14, 15–21, 22–28, 29–31. It differs from the selected download week. Hours are the 12 scheduled slots; multiple parts stay separate.</p><p class="weekly-example-label">Example from an available file:</p><code class="weekly-path">Loading verified path…</code><p class="weekly-missing detail-meta">Resumable downloads verify SHA-256 and preserve existing files. Missing capture and slots without registered output contribute no fabricated files.</p></section>';
}
async function catalog(){return catalogPromise??=(async()=>{const r=await fetch('/api/coverage-downloads/catalog?weekly=1',{cache:'no-store'});if(!r.ok)throw Error('Weekly download metadata unavailable');return r.json()})().catch(e=>{catalogPromise=undefined;throw e;});}
async function hydrate(r,scope){
 const section=document.querySelector('.weekly-detail');if(!section||Number(section.dataset.stream)!==r.id)return;
 const chosen=choices.has(r.id)?choices.get(r.id):1;
 try{
  const d=await catalog(),s=d.streams.find(s=>s.id===r.id);if(!s)throw Error('Unknown download stream');
  const info=s.weekly_exports[scope];if(info.period.start!==r.start||info.period.end!==r.end)throw Error('Selected period differs from download catalog');
  let weeks=info.weeks,repair=reviewed(r,scope);
  if(repair){
   const resp=await fetch('/api/coverage-delivery/manifest?stream='+r.id+'&scope='+scope,{cache:'no-store'});if(!resp.ok)throw Error('Verified replacement manifest unavailable');const m=await resp.json();if(m.period[0]!==r.start||m.period[1]!==r.end)throw Error('Replacement period differs');
   weeks=info.weeks.map(w=>{const outputs=m.outputs.filter(o=>o.local_date>=w.start&&o.local_date<=w.end);if(m.outputs.some(o=>!o.local_date))throw Error('Reviewed output dates unavailable');return{...w,parts:outputs.length,available:outputs.filter(o=>o.available).length,size_bytes:outputs.filter(o=>o.available).reduce((n,o)=>n+o.size_bytes,0),sample_path:outputs.find(o=>o.available)?.path||null}});
  }
  if(!section.isConnected||Number(section.dataset.stream)!==r.id)return;
  const set=chosen?weeks.filter(w=>w.number===chosen):weeks,parts=set.reduce((n,w)=>n+w.parts,0),available=set.reduce((n,w)=>n+w.available,0),bytes=set.reduce((n,w)=>n+w.size_bytes,0),sample=set.find(w=>w.sample_path)?.sample_path;
  section.querySelector('.weekly-selection').textContent=available.toLocaleString()+' available files · '+(bytes/1e9).toFixed(2)+' GB'+(available<parts?' · '+(parts-available)+' unavailable':'')+(repair?' · includes verified assembled replacements':' · original registered V2 parts');
  section.querySelector('.weekly-path').textContent=sample||'No registered downloadable files in this selected week.';
  if(!s.metadata_complete)section.querySelector('.weekly-missing').textContent='Location metadata is incomplete; the shown folder preserves the selected recording ID without inventing geography. Missing capture and slots without output remain absent.';
 }catch(e){if(section.isConnected){section.querySelector('.weekly-selection').textContent=e.message;section.querySelector('.weekly-path').textContent='No path invented. Browse files for individual registered outputs.';}}
}
function rerender(section){const app=window.coverageApp,r=app?.meta.streams.find(r=>r.id===Number(section.dataset.stream));if(!r)return;const scope=section.dataset.scope;section.outerHTML=detailMarkup(r,scope,false);hydrate(r,scope);}
document.addEventListener('click',async e=>{
 const toggle=e.target.closest('.weekly-help-toggle');if(toggle){const h=toggle.closest('.weekly-help');h.dataset.pinned=String(h.dataset.pinned!=='true');setHelp(h,h.dataset.pinned==='true');return;}
 const open=e.target.closest('.weekly-open');if(open){const app=window.coverageApp,r=app?.meta.streams.find(r=>r.id===Number(open.dataset.stream));if(!r)return;const w=Number(open.dataset.week);choices.set(r.id,w);await app.openDetail(r,w===2?7:0,0);document.querySelector('.weekly-detail')?.scrollIntoView({block:'nearest'});return;}
 const pick=e.target.closest('.weekly-pick');if(pick){const section=pick.closest('.weekly-detail');choices.set(Number(section.dataset.stream),Number(pick.dataset.week));rerender(section);return;}
 const copy=e.target.closest('.weekly-copy');if(copy){const section=copy.closest('.weekly-detail'),code=section.querySelector('.weekly-code').textContent,status=section.querySelector('.weekly-copy-status');try{await navigator.clipboard.writeText(code);status.textContent='Command copied';}catch(e){status.textContent='Select and copy the command above';}}
});
document.addEventListener('change',e=>{if(e.target.matches('.weekly-kind')){const section=e.target.closest('.weekly-detail');kinds.set(Number(section.dataset.stream),e.target.value);rerender(section);}});
function setHelp(h,show){h.querySelector('[role=tooltip]').hidden=!show;h.querySelector('button').setAttribute('aria-expanded',String(show));}
document.addEventListener('pointerover',e=>{const h=e.target.closest('.weekly-help');if(h)setHelp(h,true);});
document.addEventListener('focusin',e=>{const h=e.target.closest('.weekly-help');if(h)setHelp(h,true);});
for(const event of ['pointerout','focusout'])document.addEventListener(event,e=>{const h=e.target.closest('.weekly-help');if(h&&!h.contains(e.relatedTarget)&&h.dataset.pinned!=='true')setHelp(h,false);});
document.addEventListener('keydown',e=>{if(e.key==='Escape')document.querySelectorAll('.weekly-help').forEach(h=>{h.dataset.pinned='false';setHelp(h,false);});});
window.weeklyDownloads={rowMarkup,detailMarkup,hydrate,resetCatalog:()=>{catalogPromise=undefined}};
})();
