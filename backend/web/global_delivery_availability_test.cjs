const test=require('node:test');
const assert=require('node:assert/strict');
const fs=require('node:fs');
const a=require('./global-delivery/availability.js');

const streams=[
 {id:416,availability:'available',best14_start:'2026-08-07',best14_end:'2026-08-20',best14_days_ready:14},
 {id:335,availability:'partial',best14_start:'2026-09-25',best14_end:'2026-10-08',best14_days_ready:0},
 {id:427,availability:'partial',best14_start:'2026-08-13',best14_end:'2026-08-26',best14_days_ready:6},
];

test('default view lists only available streams; ?show=all includes partial',()=>{
 assert.equal(a.show(''),'available');
 assert.equal(a.show('?show=available'),'available');
 assert.equal(a.show('?stream=416&show=all'),'all');
 assert.deepEqual(a.visible(streams,'').map(s=>s.id),[416]);
 assert.deepEqual(a.visible(streams,'?show=all').map(s=>s.id),[416,335,427]);
});

test('counts and the partial reason label',()=>{
 assert.equal(a.countLabel(a.counts(streams)),'1 streams fully available now · 2 more partial');
 assert.equal(a.label(streams[0]),'');
 assert.equal(a.reason(streams[2]),'Best 14 days (Aug 13–26) in progress: 6 of 14 ready');
 assert.match(a.label(streams[2]),/^<p class="availability-note">/);
});

test('best-14 periods across months',()=>{
 assert.equal(a.reason(streams[1]),'Best 14 days (Sep 25–Oct 8) in progress: 0 of 14 ready');
});

test('a deep link to a partial stream opens the full list',()=>{
 assert.equal(a.linkedSearch(streams,'?stream=427&day=2026-08-14'),'?stream=427&day=2026-08-14&show=all');
 assert.equal(a.linkedSearch(streams,'?stream=427&show=all'),null);
 assert.equal(a.linkedSearch(streams,'?stream=416'),null);
 assert.equal(a.linkedSearch(streams,'?stream=999'),null);
 assert.equal(a.linkedSearch(streams,''),null);
});

test('generations without availability list every stream; mixed or unknown values fail',()=>{
 const old=[{id:1},{id:2}];
 assert.equal(a.annotated(old),false);
 assert.deepEqual(a.visible(old,''),old);
 assert.throws(()=>a.visible([{id:1,availability:'available'},{id:2}],''),/incomplete/);
 assert.throws(()=>a.visible([{id:1,availability:'soon'}],''),/Unknown/);
});

test('the page loads the filter before app.js and app.js uses it',()=>{
 const html=fs.readFileSync(__dirname+'/global-delivery/index.html','utf8'),app=fs.readFileSync(__dirname+'/global-delivery/app.js','utf8');
 assert.ok(html.indexOf('availability.js')>0&&html.indexOf('availability.js')<html.indexOf('/app.js'));
 assert.match(html,/id="show-filter"[^>]*hidden/);
 assert.match(app,/records=window\.deliveryAvailability\.visible\(meta\.streams,location\.search\)/);
 assert.match(app,/window\.deliveryAvailability\.mount\(meta\.streams,renderList\);renderList\(\)/);
});
