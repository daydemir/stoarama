"""Validate immutable same-source experimental predictions before displaying them."""
import hashlib,json,re
SHA=re.compile(r'[0-9a-f]{64}')
LABELS=('V7','V7.5','V7.9','V5.5')
def validated_model_comparison(case):
 matrix=case.get('model_comparison')
 if not isinstance(matrix,dict) or set(matrix) not in ({'Model79','Model89'},set(LABELS),set(LABELS)|{'V7.99'},set(LABELS)|{'V7.99','V7.999'},set(LABELS)|{'V7.99','V7.999','Model49'},set(LABELS)|{'V7.99','V7.999','Model49','Model59'},set(LABELS)|{'V7.99','V7.999','Model49','Model59','Model55'},set(LABELS)|{'V7.99','V7.999','Model49','Model59','Model55','Model69'},set(LABELS)|{'V7.99','V7.999','Model49','Model59','Model55','Model69','Model79'}):return None
 labels=tuple(label for label in LABELS+('V7.99','V7.999','Model49','Model55','Model59','Model69','Model79','Model89')if label in matrix)
 receipts=case.get('source_receipts');bindings=case.get('comparison_source_bindings')
 if not isinstance(receipts,list) or len(receipts)!=2 or not isinstance(bindings,list) or len(bindings)!=2:return None
 normalized=[]
 for r in receipts:
  if not isinstance(r,dict) or r.get('verified') is not True or type(r.get('clip_id')) is not int or type(r.get('size_bytes')) is not int or r['size_bytes']<=0 or not isinstance(r.get('sha256'),str) or not SHA.fullmatch(r['sha256']):return None
  normalized.append({k:r[k] for k in ('clip_id','sha256','size_bytes')})
 if bindings!=normalized or case.get('clip_ids')!=[r['clip_id'] for r in normalized]:return None
 try:diagnostic=hashlib.sha256(json.dumps(case['current_v3'],sort_keys=True,separators=(',',':'),allow_nan=False).encode()).hexdigest()
 except (KeyError,ValueError,TypeError):return None
 for label in labels:
  row=matrix[label]
  if not isinstance(row,dict) or row.get('decision') not in ('join','split') or row.get('diagnostic_sha256')!=diagnostic or not isinstance(row.get('implementation_sha256'),str) or not SHA.fullmatch(row['implementation_sha256']) or not isinstance(row.get('parameters'),dict):return None
  if label=='V7.99' and row['implementation_sha256']!='b5987e031cc48ebaf522bbfe8f58186a8bfa158433213c682f3a3cf95e5f7133':return None
  if label=='V7.999' and row['implementation_sha256']!='10bf837b8714d4eab6d7e2c7d60ef21e09983a76fe3aecd3af248dfc68f2c166':return None
  if label=='Model49' and row['implementation_sha256']!='8d453c6abe4f7560ecea55b9a2cbf3044340591273e16bf5063d22070b6d050a':return None
  try:
   if label=='Model49' and hashlib.sha256(json.dumps(row['parameters'],sort_keys=True,separators=(',',':'),allow_nan=False).encode()).hexdigest()!='649554ca2ad11b95692b237f33bd9000a8b9a9a335b2faed61761aa6a9455e59':return None
   if label=='Model59' and (row['implementation_sha256']!='0edfb031077a5e4620f0b793e4420fda20ab067a354aae8e864f5af5f20126cf' or hashlib.sha256(json.dumps(row['parameters'],sort_keys=True,separators=(',',':'),allow_nan=False).encode()).hexdigest()!='77f3685f7ef51ed696c684cdbc1e52a8e80d48cab285d032a451efaf90a7a29a'):return None
   if label=='Model55' and (row['implementation_sha256']!='a70fed40fe92508359e911bc4e7fa4965947117e45dd0e5821d0f8635b150cf2' or hashlib.sha256(json.dumps(row['parameters'],sort_keys=True,separators=(',',':'),allow_nan=False).encode()).hexdigest()!='5529c07fe21299daf18d9341eda791579fe37897e847bb5c07a4cd75aa299a8e'):return None
   if label=='Model69' and (row['implementation_sha256']!='7c56d598d5a4595081a56d96669611b3c6de6f364c5426f2640d1b62f261d2e3' or hashlib.sha256(json.dumps(row['parameters'],sort_keys=True,separators=(',',':'),allow_nan=False).encode()).hexdigest()!='bdbbfefb94e327a13f9ea6b8aa87513e38b599546ee129507078dad2bfc04ff1'):return None
   if label=='Model79' and (row['implementation_sha256']!='d79c05e1c13f502ef09feffbc2324cf07596b438f5d4622bd5b4bf262445ced6' or hashlib.sha256(json.dumps(row['parameters'],sort_keys=True,separators=(',',':'),allow_nan=False).encode()).hexdigest()!='87ebb93f37cb850aa2154c4e9733497575e4eaafaf915f93bccdf5639000d2d7'):return None
   if label=='Model89' and (row['implementation_sha256'],hashlib.sha256(json.dumps(row['parameters'],sort_keys=True,separators=(',',':'),allow_nan=False).encode()).hexdigest()) not in {('ad327123777cce4ebc1a622c440cbbd8f4081dcfc49863aa181b9784be8ecf0b','f0924c78b66436ba80cf0021ac656cf14232b5a3b08d0c089d1b4677b5165d30'),('27b03616462353a8010ae62a9119f2f5c4de35036bd824d31077c4709b35f8e9','c9bd8f00b50af0d54a78e728e7e50be428e7862091728793b13e753f097cb6c4')}:return None
   if len(json.dumps(row['parameters'],allow_nan=False))>4096:return None
  except (ValueError,TypeError):return None
 if case.get('version_disagreement') is not (len({r['decision'] for r in matrix.values()})>1):return None
 return matrix
