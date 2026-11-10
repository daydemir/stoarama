"""Review-only V7.999 one-sided tail frontier; no automatic production approval.
Retains V7.99 and adds complete-GOP, zero-metadata-gap nominations with the
right-source match at its first two frames, a boundary no more than twice the
ordinary key-frame step and movement on both sides. A left match within4.8s is
feature evidence, not a proposed temporal cut or proof that motion is continuous.
Overlap/replay reasons remain rejected pending a separately frozen cut proof.
"""
import pathlib,hashlib,importlib.util,math
VERSION='v7p999-one-sided-tail-frontier-review-20261009'
BASE_SHA256='b5987e031cc48ebaf522bbfe8f58186a8bfa158433213c682f3a3cf95e5f7133'
PARAMETERS={'tail':60,'tail_max_seconds':4.8,'head':1,'boundary':2,'tail_sharp':.9,'head_sharp':.825,'motion':.8,'gop':['complete'],'review_only':True}
_path=pathlib.Path(__file__).resolve().parent.parent/'v7p99-rules/v7p99_one_sided_tail3_review.py'
assert hashlib.sha256(_path.read_bytes()).hexdigest()==BASE_SHA256,'Frozen V7.99 differs'
_spec=importlib.util.spec_from_file_location('frozen_visual_base',_path);_base=importlib.util.module_from_spec(_spec);_spec.loader.exec_module(_base)
def nominate(d,p=PARAMETERS):
 if _base.nominate(d,_base.PARAMETERS):return True
 period=d.get('frame_seconds')
 if not isinstance(period,(int,float))or not math.isfinite(period)or not(.01<=period<=.2):return False
 # This uses source-derived cadence; it does not resample or remove source frames.
 expanded=dict(p,tail=min(p['tail'],math.floor(p['tail_max_seconds']/period)))
 return _base._core(d,expanded)
