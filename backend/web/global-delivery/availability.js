/* Best-14 availability filter. ?show=all includes partial streams; the default lists available ones. */
(function(root){'use strict';
const STATES=['available','partial'],WINDOWS=['original','best14'];
function annotated(streams){
 const marked=streams.filter(s=>s.availability!==undefined);
 if(marked.length&&marked.length!==streams.length)throw Error('Stream availability is incomplete');
 for(const s of marked)if(!STATES.includes(s.availability))throw Error('Unknown stream availability');
 for(const s of marked)if(s.window!==undefined&&!WINDOWS.includes(s.window))throw Error('Unknown stream window');
 return marked.length>0;
}
const show=search=>new URLSearchParams(search).get('show')==='all'?'all':'available';
function visible(streams,search){return annotated(streams)&&show(search)==='available'?streams.filter(s=>s.availability==='available'):streams}
function counts(streams){return{available:streams.filter(s=>s.availability==='available').length,partial:streams.filter(s=>s.availability==='partial').length}}
const countLabel=c=>`${c.available} streams fully available now · ${c.partial} more partial`;
const day=s=>new Date(s+'T12:00:00Z'),month=s=>day(s).toLocaleString('en-US',{timeZone:'UTC',month:'short'});
function period(start,end){return month(start)===month(end)?`${month(start)} ${day(start).getUTCDate()}–${day(end).getUTCDate()}`:`${month(start)} ${day(start).getUTCDate()}–${month(end)} ${day(end).getUTCDate()}`}
// A partial stream still on its original period names its best-14 window (best14_dates) beside it.
function reason(s){
 if(s.availability!=='partial')return '';
 const original=s.window==='original',[start,end]=original?s.best14_dates:[s.best14_start,s.best14_end];
 return `${original?'Showing original period; best':'Best'} 14 days (${period(start,end)}) in progress: ${s.best14_days_ready} of 14 ready`;
}
function label(s){const text=reason(s);return text?`<p class="availability-note">${text}</p>`:''}
// A link to one partial stream (?stream=<id>) opens the full list so the stream is shown.
function linkedSearch(streams,search){
 const q=new URLSearchParams(search),linked=streams.find(s=>s.id===Number(q.get('stream')));
 if(linked?.availability!=='partial'||show(search)==='all')return null;
 q.set('show','all');return '?'+q;
}
function mount(streams,render){
 if(!annotated(streams))return;
 const loc=root.location,linked=linkedSearch(streams,loc.search);
 if(linked)root.history.replaceState(null,'',loc.pathname+linked+loc.hash);
 const el=root.document.querySelector('#show-filter'),c=counts(streams);el.hidden=false;
 const sync=()=>{const v=show(loc.search),u=new URL(loc.href);v==='all'?u.searchParams.delete('show'):u.searchParams.set('show','all');
  for(const b of el.querySelectorAll('button'))b.setAttribute('aria-pressed',String(b.dataset.show===v));
  el.querySelector('#show-counts').innerHTML=`<strong>${countLabel(c)}</strong> <a href="${u.search||loc.pathname}" data-show="${v==='all'?'available':'all'}">(${v==='all'?'available only':'show all'})</a>`};
 el.onclick=e=>{const b=e.target.closest('[data-show]');if(!b)return;e.preventDefault();const u=new URL(loc.href);if(b.dataset.show==='all')u.searchParams.set('show','all');else u.searchParams.delete('show');root.history.replaceState(null,'',u);sync();render()};
 sync();
}
const api={annotated,show,visible,counts,countLabel,period,reason,label,linkedSearch,mount};
if(typeof module==='object'&&module.exports)module.exports=api;else root.deliveryAvailability=api;
})(typeof window==='object'?window:this);
