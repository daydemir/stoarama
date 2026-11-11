'use strict';
const $=id=>document.getElementById(id);
const RATE=15/1e12;
const TB=b=>(b/1e12).toFixed(b<1e11?3:2)+' TB';
const USD=b=>'$'+(b*RATE).toFixed(b*RATE<10?2:0);
const escapeHTML=v=>String(v??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
const fmtD=d=>d?new Date(d+'T00:00:00Z').toLocaleDateString(undefined,{month:'short',day:'numeric',year:'numeric',timeZone:'UTC'}):'—';
const addDays=(d,n)=>new Date(Date.parse(d+'T00:00:00Z')+n*864e5).toISOString().slice(0,10);
const sizeOf=(r,c)=>c==='none'?0:c==='st'?r.r2_st_keep:c==='keep'?r.r2_raw_keep+r.r2_st_keep:nowOf(r);
const nowOf=r=>r.r2_raw_keep+r.r2_raw_out+r.r2_st_keep+r.r2_st_out;
const grade=s=>s===0?'zero':s>90?'green':s>=70?'yellow':'red';
let rows=[],savedDocument={},choices={},changed=new Set(),saving=false;
function status(text,ok=false){$('st').textContent=text;$('st').className=ok?'saved':'';}
function kpis(){let now=0,next=0,decided=0;for(const r of rows){now+=nowOf(r);next+=choices[r.stream_id]?sizeOf(r,choices[r.stream_id]):nowOf(r);if(choices[r.stream_id])decided++;}
$('k-now').textContent=USD(now)+'/mo';$('k-now-tb').textContent=TB(now);
$('k-new').textContent=USD(next)+'/mo';$('k-new-tb').textContent=TB(next)+' · undecided stay kept';
$('k-save').textContent=USD(now-next)+'/mo';$('k-dec').textContent=decided+' / '+rows.length;
$('save').disabled=saving||changed.size===0;
}
function visibleRows(){const q=$('q').value.toLowerCase().trim(),filter=$('filter').value,key=$('sort').value;
return rows.filter(r=>(!q||[r.name,r.city,r.country,r.stream_id].join(' ').toLowerCase().includes(q))&&
(filter==='all'||filter==='undecided'&&!choices[r.stream_id]||filter==='decided'&&choices[r.stream_id]||filter==='gaps'&&r.seconds.includes(0)||filter==='strong'&&r.days90>0))
.sort((a,b)=>key==='name'?a.name.localeCompare(b.name):((key==='all'?nowOf(b)-nowOf(a):b[key]-a[key])||a.stream_id-b.stream_id));}
function calendar(r){if(!r.first_day)return '<p class="muted">No raw clips remain in the R2 catalog. Coverage is unavailable; stitched footage may still exist.</p>';
const months=new Map();r.seconds.forEach((seconds,i)=>{const date=addDays(r.first_day,i),month=date.slice(0,7);if(!months.has(month))months.set(month,[]);months.get(month).push({date,seconds,percent:seconds/r.scheduled_seconds[i]*100});});
let out='';for(const [month,days]of months){const first=new Date(days[0].date+'T00:00:00Z'),offset=(first.getUTCDay()+6)%7;
out+=`<section class="month"><h3>${first.toLocaleDateString(undefined,{month:'long',year:'numeric',timeZone:'UTC'})}</h3><div class="days"><span class="weekday">M</span><span class="weekday">T</span><span class="weekday">W</span><span class="weekday">T</span><span class="weekday">F</span><span class="weekday">S</span><span class="weekday">S</span>${'<span></span>'.repeat(offset)}`;
for(const {date,seconds,percent} of days){const keep=r.keep_start&&date>=r.keep_start&&date<r.keep_end_excl,best=r.best_start&&date>=r.best_start&&date<=r.best_end_incl;
const label=`${date} · ${(percent).toFixed(2)}% coverage · ${(seconds/3600).toFixed(2)} hours${keep?' · pinned 28-day window':''}${best?' · best 14':''}`;
out+=`<button type="button" class="day ${grade(percent)}${keep?' keep':''}${best?' best':''}" title="${label}" aria-label="${label}" data-day="${label}">${Number(date.slice(8))}</button>`;}
out+='</div></section>';}
return out;}
function render(){const visible=visibleRows();$('count').textContent=visible.length+' of '+rows.length+' streams';const fragment=document.createDocumentFragment();
for(const r of visible){const id=r.stream_id,c=choices[id]||'',article=document.createElement('article');article.className='stream';article.dataset.stream=id;
const option=(value,label)=>{const unavailable=!r.keep_start&&['st','keep'].includes(value);return `<label class="${c===value?'on':''}"><span class="choice-label"><input type="radio" name="s${id}" value="${value}" ${c===value?'checked':''} ${unavailable?'disabled':''}>${label}</span><small>${unavailable?'No pinned window':TB(sizeOf(r,value))+'<br>'+USD(sizeOf(r,value))+'/mo'}</small></label>`;};
article.innerHTML=`<div class="stream-head"><div class="identity"><h2>${escapeHTML(r.name)}</h2><div class="loc">${escapeHTML([r.city,r.country].filter(Boolean).join(', '))} · #${id} · ${escapeHTML(r.tz)}${!c?' · Undecided':''}</div>
<p class="schedule">Records ${escapeHTML(r.windows.map(([a,b])=>a+'–'+b).join(', '))} local</p>
<p class="span">${r.first_day?fmtD(r.first_day)+' – '+fmtD(r.last_day):'No raw recording span in R2'}</p>
<div class="stream-meta"><span><b>${r.total_hours.toLocaleString()}</b> recorded hours in R2</span><span><b>${r.days_recorded}</b> days recorded</span><span><b>${r.avg_coverage.toFixed(1)}%</b> average · gaps included</span><span><b>${r.days90}</b> days ≥90%</span></div>
${r.thumbs?.length?`<div class="thumbs">${r.thumbs.map((t,i)=>`<button type="button" class="thumb" data-src="${escapeHTML(t)}" aria-label="Enlarge ${['morning','midday','evening'][i]||'sample'} preview"><img loading="lazy" src="${escapeHTML(t)}" width="160" height="90" alt="${['Morning','Midday','Evening'][i]||'Sample'} frame from the pinned best 14 days"></button>`).join('')}</div>`:'<p class="nothumb">No preview: no playable clip found for this stream</p>'}
</div><div><fieldset class="opts"><legend>Keep in R2</legend>${option('none','Nothing')}${option('st','Kept period, stitched')}${option('keep','Kept period, raw + stitched')}${option('all','Everything · all days')}</fieldset><p class="cost">Currently ${TB(nowOf(r))} · ${USD(nowOf(r))}/mo</p></div></div>
<p class="window">${r.keep_start?`<b>Pinned 28:</b> ${fmtD(r.keep_start)} – ${fmtD(addDays(r.keep_end_excl,-1))} &nbsp; <b>Best 14:</b> ${fmtD(r.best_start)} – ${fmtD(r.best_end_incl)}`:'No pinned keep28 window for this stream.'}</p>
<div class="calendar" aria-label="Daily coverage for stream ${id}">${calendar(r)}</div><p class="day-status" aria-live="polite">Tap or focus a day to inspect its coverage.</p>`;
article.querySelectorAll('.day').forEach(button=>{const show=()=>article.querySelector('.day-status').textContent=button.dataset.day;button.onclick=show;button.onfocus=show;});
article.querySelectorAll('.thumb').forEach(button=>button.onclick=()=>{$('lightbox').querySelector('img').src=button.dataset.src;$('lightbox').querySelector('img').hidden=false;$('lightbox').querySelector('img').alt=r.name;$('lightbox').showModal();});
article.querySelectorAll('.opts input').forEach(input=>input.onchange=()=>{choices[id]=input.value;changed.add(String(id));status('Unsaved changes');const y=window.scrollY;render();window.scrollTo(0,y);});
fragment.appendChild(article);}
$('streams').replaceChildren(fragment);if(!visible.length){const p=document.createElement('p');p.className='empty';p.textContent='No streams match these filters. Try another search or show all streams.';$('streams').appendChild(p);}kpis();}
async function getJSON(url){const res=await fetch(url,{cache:'no-store'});if(!res.ok)throw new Error('Could not load '+url+' ('+res.status+')');return res.json();}
$('q').oninput=render;$('sort').onchange=render;$('filter').onchange=render;
$('bulk').onchange=event=>{const value=event.target.value;if(!value)return;for(const r of visibleRows()){if(!r.keep_start&&['st','keep'].includes(value))continue;choices[r.stream_id]=value;changed.add(String(r.stream_id));}event.target.value='';status('Unsaved changes');render();};
$('save').onclick=async()=>{saving=true;kpis();status('Saving…');try{
const latest=await getJSON('api/selection');if(JSON.stringify(latest)!==JSON.stringify(savedDocument))throw new Error('Saved picks changed in another session. Reload before saving.');
// Preserve every existing pick (including streams absent from this snapshot) and its metadata.
const document={...savedDocument,choices:{...(savedDocument.choices||{})}};
for(const id of changed){const r=rows.find(row=>String(row.stream_id)===id);document.choices[id]={...(document.choices[id]||{}),choice:choices[id],bytes:sizeOf(r,choices[id]),...(r.keep_start?{keep_start_utc:r.keep_start_utc,keep_end_utc:r.keep_end_utc}:{})};}
const res=await fetch('api/selection',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(document)});
if(!res.ok)throw new Error(res.status===403?'Only Deniz can save picks. Your changes remain unsaved.':'Save failed ('+res.status+'). Try again.');
// Read back the server's exact document, including its saved_at/saved_by fields.
savedDocument=await getJSON('api/selection');for(const id of changed)if(savedDocument.choices?.[id]?.choice!==choices[id])throw new Error('Save verification failed. Reload to inspect saved picks.');
changed.clear();status('Saved '+new Date(savedDocument.saved_at).toLocaleString(),true);
}catch(error){status(error.message);}finally{saving=false;kpis();}};
$('close-preview').onclick=()=>$('lightbox').close();$('lightbox').onclick=event=>{if(event.target===$('lightbox'))$('lightbox').close();};
window.onbeforeunload=()=>changed.size?true:undefined;
Promise.all([getJSON('coverage.json'),getJSON('api/selection')]).then(([data,saved])=>{rows=data.streams;savedDocument=saved;for(const[k,v]of Object.entries(saved.choices||{}))choices[k]=v.choice;
$('definition').textContent=data.definition;$('snapshot').textContent='R2 catalog snapshot · '+new Date(data.generated_at).toLocaleString()+' · Raw R2 coverage in each stream’s local timezone · Full historical span';
status(saved.saved_at?'Last saved '+new Date(saved.saved_at).toLocaleString():'No saved picks yet',Boolean(saved.saved_at));render();}).catch(error=>{$('count').textContent='Recording history could not load.';status(error.message+'. Reload to retry.');});
