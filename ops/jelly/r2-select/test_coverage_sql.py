"""Run on Jelly with DATABASE_URL loaded locally; SELECT-only SQL regression checks."""
import importlib.util,json,os,subprocess
from pathlib import Path
spec=importlib.util.spec_from_file_location('cov',Path(__file__).parent/'coverage.py');c=importlib.util.module_from_spec(spec);spec.loader.exec_module(c)
sql=c.query([dict(stream_id=1,tz='America/Los_Angeles')]).replace('WITH ', 'WITH '+(Path(__file__).parent/'coverage_fixture.sql').read_text(),1)
p=subprocess.run(['/opt/homebrew/opt/postgresql@17/bin/psql',os.environ['DATABASE_URL'],'-XqAt','-v','ON_ERROR_STOP=1'],input=sql,text=True,capture_output=True,env=dict(os.environ,PGOPTIONS='-c default_transaction_read_only=on'))
if p.returncode: raise SystemExit('Read-only fixture SQL failed (psql exit %s)' % p.returncode)
r=json.loads(p.stdout); a,b=r['streams'];assert a['stream_id']==1 and b['stream_id']==3
assert a['days']=={'2026-11-01':90000};assert a['scheduled']['2026-11-01']==90000
assert b['days']=={'2026-11-01':1800,'2026-11-02':1800};assert set(b['scheduled'].values())=={7200}
print('SQL: DST 25-hour denominator, overlap union, overnight split, purged/retired gaps and account isolation passed')
