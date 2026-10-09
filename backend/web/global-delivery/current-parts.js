/* Current source-once file projection. Original capture arrays remain immutable. */
(function(root){'use strict';
function project(parts,assemblies){
 const originals=new Map(parts.map(p=>[String(p[0]),p])),byInput=new Map();
 if(originals.size!==parts.length)throw Error('Duplicate original timeline identity');
 for(const assembly of assemblies){
  const ids=assembly.input_ids.map(String),members=ids.map(id=>originals.get(id));
  if(members.some(p=>!p))continue; // An assembly in another scheduled hour.
  if(new Set(ids).size!==ids.length||ids.length<2||!Number.isFinite(assembly.seconds)||assembly.seconds<=0)throw Error('Invalid current assembly');
  for(const id of ids){if(byInput.has(id))throw Error('Overlapping current assemblies');byInput.set(id,assembly);}
 }
 const seen=new Set(),result=[];
 for(const original of parts){
  const assembly=byInput.get(String(original[0]));
  if(!assembly){result.push(original.slice());continue;}
  if(seen.has(assembly.sha256))continue;seen.add(assembly.sha256);
  const members=assembly.input_ids.map(id=>originals.get(String(id))),part=original.slice();
  part[2]=assembly.seconds;part[3]=Math.min(...members.map(p=>p[3]));part[4]=Math.max(...members.map(p=>p[4]));
  // Do not infer source union or clip count by summing overlapping evidence.
  part[5]=null;part[6]=null;part[9]=true;part.currentAssembly=assembly;
  part.sourceIntervals=members.map(p=>[p[3],p[4]]);result.push(part);
 }
 result.forEach((part,i)=>part[1]=i+1);return result;
}
if(typeof module==='object'&&module.exports)module.exports=project;else root.projectCurrentDeliveryParts=project;
})(typeof window==='object'?window:this);
