const assert=require('node:assert/strict');
const fs=require('node:fs');
const vm=require('node:vm');
const path=require('node:path');
const window={addEventListener(){}};
const context={window,document:{addEventListener(){},querySelector(){return{}}},MutationObserver:class{observe(){}},setInterval(){},setTimeout(){},URL,location:{origin:'https://stoarama.com'}};
vm.runInNewContext(fs.readFileSync(path.join(__dirname,'../web/global-delivery/coverage-growth.js'),'utf8'),context);
const ui=window.coverageGrowth;
const sha='a'.repeat(64);
const parent={sha256:sha,production_version:'79',input_ids:[30031,30032,30033,30034]};
const annotation={scope:'boundary',method:'CLOCK_20',status:'verified',artifact_sha256:sha,proof_sha256:'b'.repeat(64),source_input_ids:[30032,30033],maximum_relative_fps_error:1/134};
const labeled={...parent,clock_boundary_annotations:[annotation]};
assert.equal(ui.modelLabel(labeled),'V79','Existing native provenance is retained');
assert.equal(ui.clockBoundaryLabel(labeled),'CLOCK_20 · 1 boundary verified');
assert.equal(ui.clockBoundaryLabel({...labeled,clock_boundary_annotations:[annotation,annotation]}),'CLOCK_20 · 1 boundary verified','One boundary is counted once');
assert.equal(ui.clockBoundaryLabel(parent),'');
assert.equal(ui.clockBoundaryLabel({...parent,clock_boundary_annotations:{}}),'');
assert.equal(ui.clockBoundaryLabel({...labeled,input_ids:null}),'');
for(const invalid of [{status:'pending'},{scope:'file'},{artifact_sha256:'c'.repeat(64)},{proof_sha256:'not-verified'},{source_input_ids:[30033,30032]},{source_input_ids:[30031,30034]},{maximum_relative_fps_error:.2001}]){
 assert.equal(ui.clockBoundaryLabel({...parent,clock_boundary_annotations:[{...annotation,...invalid}]}),'','Unsupported or unrelated evidence earns no label');
}
assert.equal(ui.modelLabel({production_version:'85'}),'V85');
assert.equal(ui.audioLabel({production_version:'85',temporary_no_audio:true,original_source_audio_present:true,audio_delivery_status:'pending_source_bound_audio_fill'}),'Video only · audio pending');
assert.equal(ui.modelLabel({production_version:'clock-prestitched'}),'Clock-prestitched');
console.log('CLOCK boundary labels: provenance, ordering, deduplication, and negative evidence PASS');
