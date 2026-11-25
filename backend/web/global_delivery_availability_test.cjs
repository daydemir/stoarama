const test=require('node:test');
const assert=require('node:assert/strict');
const fs=require('node:fs');
const a=require('./global-delivery/availability.js');

const streams=[
 {id:416,availability:'available',best14_days_ready:14},
 {id:335,availability:'partial',best14_days_ready:0},
 {id:339,availability:'partial',best14_days_ready:6},
];

test('default view lists only available streams; ?show=all includes partial',()=>{
 assert.equal(a.show(''),'available');
 assert.equal(a.show('?show=available'),'available');
 assert.equal(a.show('?stream=416&show=all'),'all');
 assert.deepEqual(a.visible(streams,'').map(s=>s.id),[416]);
 assert.deepEqual(a.visible(streams,'?show=all').map(s=>s.id),[416,335,339]);
});

test('counts and the partial reason label',()=>{
 assert.equal(a.countLabel(a.counts(streams)),'1 available now · 2 partial');
 assert.equal(a.label(streams[0]),'');
 assert.equal(a.reason(streams[2]),'Best 14 days in progress: 6 of 14 days ready');
 assert.match(a.label(streams[2]),/^<p class="availability-note">/);
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
