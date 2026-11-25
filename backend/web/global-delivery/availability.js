/* Best-14 availability filter. ?show=all includes partial streams; the default lists available ones. */
(function(root){'use strict';
const STATES=['available','partial'];
function annotated(streams){
 const marked=streams.filter(s=>s.availability!==undefined);
 if(marked.length&&marked.length!==streams.length)throw Error('Stream availability is incomplete');
 for(const s of marked)if(!STATES.includes(s.availability))throw Error('Unknown stream availability');
 return marked.length>0;
}
const show=search=>new URLSearchParams(search).get('show')==='all'?'all':'available';
function visible(streams,search){return annotated(streams)&&show(search)==='available'?streams.filter(s=>s.availability==='available'):streams}
function counts(streams){return{available:streams.filter(s=>s.availability==='available').length,partial:streams.filter(s=>s.availability==='partial').length}}
const countLabel=c=>`${c.available} available now · ${c.partial} partial`;
const reason=s=>s.availability==='partial'?`Best 14 days in progress: ${s.best14_days_ready} of 14 days ready`:'';
function label(s){const text=reason(s);return text?`<p class="availability-note">${text}</p>`:''}
function mount(streams,render){
 if(!annotated(streams))return;
 const el=root.document.querySelector('#show-filter');el.hidden=false;
 el.querySelector('#show-counts').textContent=countLabel(counts(streams));
 const sync=()=>{const v=show(root.location.search);for(const b of el.querySelectorAll('button'))b.setAttribute('aria-pressed',String(b.dataset.show===v))};
 el.onclick=e=>{const b=e.target.closest('button[data-show]');if(!b)return;const u=new URL(root.location.href);if(b.dataset.show==='all')u.searchParams.set('show','all');else u.searchParams.delete('show');root.history.replaceState(null,'',u);sync();render()};
 sync();
}
const api={annotated,show,visible,counts,countLabel,reason,label,mount};
if(typeof module==='object'&&module.exports)module.exports=api;else root.deliveryAvailability=api;
})(typeof window==='object'?window:this);
