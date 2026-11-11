"""Offline tests. Run with python3 -I test_app.py (legacy select.py shadows stdlib)."""
import copy
import http.client
import http.server
import importlib.util
import json
from pathlib import Path
import tempfile
import threading
import unittest

ROOT = Path(__file__).resolve().parent

def load(name):
    spec = importlib.util.spec_from_file_location('r2_'+name, ROOT/(name+'.py'))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module

coverage = load('coverage')
server = load('server')

class CoverageTests(unittest.TestCase):
    def test_gap_days_union_totals_and_authoritative_policy(self):
        policy = [dict(stream_id=1, tz='Asia/Tokyo', keep_start='2026-10-02',
                       keep_end_excl='2026-10-30', best_start='2026-10-09')]
        catalog = [dict(stream_id=1, tz='UTC', thumbs=['thumbs/1-0.jpg'])]
        data = dict(stream_id=1, tz='Asia/Tokyo', name='Tokyo', city='',country='',
                    days={'2026-10-01':86400,'2026-10-03':43200}, raw_bytes=100,
                    raw_keep=40, stitched_bytes=20,stitched_keep=10)
        snapshot = dict(generated_at='now',streams=[data])
        before = copy.deepcopy((policy,catalog,snapshot))
        row = coverage.build(snapshot,policy,catalog)['streams'][0]
        self.assertEqual(row['seconds'],[86400,0,43200])
        self.assertEqual((row['total_hours'],row['days_recorded'],row['days90']),(36,2,1))
        self.assertEqual(row['avg_coverage'],50)
        self.assertEqual(row['tz'],'Asia/Tokyo')
        self.assertEqual(row['keep_start'],'2026-10-02')
        self.assertEqual(row['thumbs'],['thumbs/1-0.jpg'])
        self.assertEqual((policy,catalog,snapshot),before)

    def test_empty_r2_and_nonpolicy_streams(self):
        base = dict(name='Name',city='',country='',tz='UTC',days={},first_day='2026-11-01',last_day='2026-11-03',raw_bytes=0,
                    raw_keep=0,stitched_bytes=0,stitched_keep=0)
        snapshot = dict(generated_at='now',streams=[dict(base,stream_id=1),dict(base,stream_id=2)])
        rows = coverage.build(snapshot,[dict(stream_id=1,tz='UTC')],[])['streams']
        self.assertEqual([r['stream_id'] for r in rows],[1,2])
        self.assertEqual(rows[0]['first_day'],'2026-11-01')
        self.assertEqual(rows[0]['seconds'],[0,0,0])
        with self.assertRaises(ValueError):
            coverage.build(snapshot,[dict(stream_id=3,tz='UTC')],[])

class SelectionTests(unittest.TestCase):
    def test_owner_round_trip_preserves_all_existing_picks_and_auth(self):
        with tempfile.TemporaryDirectory() as tmp:
            server.ROOT = Path(tmp)
            server.SEL = server.ROOT/'r2-picks.json'
            server.HIST = server.ROOT/'history'
            server.HIST.mkdir()
            original = {'choices':{'3':{'choice':'all','custom':'keep this metadata'},
                                   '999999':{'choice':'none'}},'custom_document':'preserve'}
            server.SEL.write_text(json.dumps(original))
            protected = server.ROOT/'selection.json'
            protected.write_bytes(b'pinned windows must never change')
            before = server.SEL.read_bytes()
            httpd = http.server.ThreadingHTTPServer(('127.0.0.1',0),server.H)
            worker = threading.Thread(target=httpd.serve_forever,daemon=True)
            worker.start()
            def request(method,payload=None,owner=None):
                conn=http.client.HTTPConnection(*httpd.server_address)
                headers={'Content-Type':'application/json'}
                if owner: headers['Tailscale-User-Login']=owner
                conn.request(method,'/api/selection',json.dumps(payload) if payload else None,headers)
                response=conn.getresponse()
                status,body=response.status,response.read()
                conn.close()
                return status,body
            try:
                self.assertEqual(request('POST',original)[0],403)
                self.assertEqual(request('POST',original,'someone@example.com')[0],403)
                self.assertEqual(server.SEL.read_bytes(),before)
                self.assertEqual(request('POST',{'choices':[]},'deniz@aydemir.us')[0],400)
                self.assertEqual(request('POST',original,'deniz@aydemir.us')[0],200)
                status,body=request('GET')
                saved=json.loads(body)
                self.assertEqual(status,200)
                self.assertEqual(saved['choices'],original['choices'])
                self.assertEqual(saved['custom_document'],'preserve')
                self.assertEqual(saved['saved_by'],'deniz@aydemir.us')
                self.assertEqual(protected.read_bytes(),b'pinned windows must never change')
                self.assertEqual(len(list(server.HIST.glob('selection-*.json'))),1)
            finally:
                httpd.shutdown();httpd.server_close();worker.join()

if __name__=='__main__':
    unittest.main()
