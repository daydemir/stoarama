"""Human-facing aliases only; immutable algorithm names and receipts stay unchanged."""
from review_comparison import validated_model_comparison
ALIASES={
 'model89-calibrated-activity-bands-revision-i-review-20261010':('89','Model89','27b03616462353a8010ae62a9119f2f5c4de35036bd824d31077c4709b35f8e9'),
 'model89-calibrated-activity-bands-revision-c-review-20261010':('89','Model89','ad327123777cce4ebc1a622c440cbbd8f4081dcfc49863aa181b9784be8ecf0b'),
 'model79-ordinary-keyframe-calibrated-motion-frontier-review-20261009':('79','Model79','d79c05e1c13f502ef09feffbc2324cf07596b438f5d4622bd5b4bf262445ced6'),
 'model55-edge-anchored-overlap-review-with-chronology-guard-20261009':('55','Model55','a70fed40fe92508359e911bc4e7fa4965947117e45dd0e5821d0f8635b150cf2'),
 'model69-unverified-overlap-reconsideration-dual-calibration-review-20261009':('69','Model69','7c56d598d5a4595081a56d96669611b3c6de6f364c5426f2640d1b62f261d2e3'),
 'model59-revision-b-ordinary-motion-context-frontier-review-20261009':('59','Model59','0edfb031077a5e4620f0b793e4420fda20ab067a354aae8e864f5af5f20126cf'),
 'model49-long-context-edge-frontier-review-20261009':('49','Model49','8d453c6abe4f7560ecea55b9a2cbf3044340591273e16bf5063d22070b6d050a'),
 'v7p99-one-sided-tail3-review-20261009':('29','V7.99','b5987e031cc48ebaf522bbfe8f58186a8bfa158433213c682f3a3cf95e5f7133'),
 'v7p999-one-sided-tail-frontier-review-20261009':('39','V7.999','10bf837b8714d4eab6d7e2c7d60ef21e09983a76fe3aecd3af248dfc68f2c166'),
}
def model_label(label,row):
 for alias,stable,sha in ALIASES.values():
  if label==stable and row.get('implementation_sha256')==sha:return alias if stable in ('Model49','Model59','Model55','Model69','Model79','Model89') else alias+' ('+stable+')'
 return label

def algorithm_label(algorithm,case):
 entry=ALIASES.get(algorithm)
 if not entry:return None
 matrix=validated_model_comparison(case)
 alias,stable,sha=entry
 if not matrix or matrix.get(stable,{}).get('implementation_sha256')!=sha:return None
 return alias if stable in ('Model49','Model59','Model55','Model69','Model79','Model89') else alias+' ('+stable+')'
