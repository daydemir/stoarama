const test=require('node:test');
const assert=require('node:assert/strict');
const fs=require('node:fs');
const vm=require('node:vm');
const project=require('./global-delivery/current-parts.js');
const fixture=require('./testdata/global_delivery_clock_crosshour.json');
const all=fixture.hours.flat();

test('actual CLOCK427 10+3 Originals become one 120-minute current file',()=>{
 const before=JSON.stringify(fixture),a=fixture.assembly;
 assert.deepEqual(fixture.hours.map(h=>h.length),[10,3]);
 assert.equal(a.input_ids.length,13);
 const first=project(fixture.hours[0],[a],all),second=project(fixture.hours[1],[a],all);
 assert.equal(first.length,1);assert.equal(second.length,0);
 assert.equal(first[0][2],7201.251);assert.equal(first[0].currentAssembly,a);
 assert.equal(first[0].sourceIntervals.length,13);
 assert.equal(first[0][3],Math.min(...all.map(p=>p[3])));
 assert.equal(first[0][4],Math.max(...all.map(p=>p[4])));
 assert.equal(a.original_source_audio_present,null);
 assert.deepEqual(a.clock_boundary_annotations[0].source_input_ids,[90766,90767]);
 assert.equal(JSON.stringify(fixture),before);
});

test('same-hour projection and missing membership stay unchanged',()=>{
 const {parts,assembly:a}=fixture.sameHour419;
 const projected=project(parts,[a]);assert.equal(projected.length,1);assert.equal(projected[0][2],240.03);assert.equal(projected[0].currentAssembly,a);assert.equal(a.original_source_audio_present,null);
 assert.deepEqual(project(fixture.hours[0],[fixture.assembly],all.slice(0,-1)),fixture.hours[0]);
 assert.throws(()=>project(parts,[a,{...a,sha256:'another'}]),/Overlapping/);
});

test('real coverage caller supplies only the selected day’s complete inventory',()=>{
 const h0=Array(10).fill(null),h1=Array(10).fill(null);h0[9]=fixture.hours[0];h1[9]=fixture.hours[1];
 const day={hours:[h0,h1]},window={coverageApp:{cache:new Map([[427,{days:[day]}]])},projectCurrentDeliveryParts:project};
 const source=fs.readFileSync(__dirname+'/global-delivery/coverage-growth.js','utf8').match(/^function currentParts\(r,h\).*$/m)[0];
 const current=vm.runInNewContext('('+source+')',{window,registered:()=>true,selected:()=>[fixture.assembly]});
 assert.equal(current({id:427},h0).length,1);assert.equal(current({id:427},h1).length,0);
});

test('actual CLOCK badges remain boundary-only after projection',()=>{
 const source=fs.readFileSync(__dirname+'/global-delivery/coverage-growth.js','utf8');
 const start=source.indexOf('function clockBoundaryLabel('),end=source.indexOf('function audioLabel(',start);
 const code=source.slice(start,end),context={measured:n=>Number.isFinite(n)};
 const label=vm.runInNewContext(code+';clockBoundaryLabel',context);
 for(const a of [fixture.assembly,fixture.sameHour419.assembly])assert.equal(label(a),'CLOCK_20 · 1 boundary verified');
});
