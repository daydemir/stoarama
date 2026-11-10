import http.server, json, os, time, pathlib
ROOT=pathlib.Path(__file__).resolve().parent
SEL=ROOT/'r2-picks.json'; HIST=ROOT/'history'; HIST.mkdir(exist_ok=True)
class H(http.server.SimpleHTTPRequestHandler):
    def __init__(s,*a,**k): super().__init__(*a,directory=str(ROOT/'public'),**k)
    def do_GET(s):
        if s.path=='/api/selection':
            b=SEL.read_bytes() if SEL.exists() else b'{}'
            s.send_response(200); s.send_header('Content-Type','application/json'); s.send_header('Cache-Control','no-store'); s.end_headers(); s.wfile.write(b); return
        return super().do_GET()
    def do_POST(s):
        if s.path!='/api/selection': s.send_error(404); return
        if s.headers.get('Tailscale-User-Login','')!='deniz@aydemir.us': s.send_error(403,'Only Deniz can save picks'); return
        n=int(s.headers.get('Content-Length','0'))
        if n>2_000_000: s.send_error(413); return
        try: d=json.loads(s.rfile.read(n)); assert isinstance(d.get('choices'),dict)
        except Exception: s.send_error(400); return
        d['saved_at']=time.strftime('%Y-%m-%dT%H:%M:%SZ',time.gmtime())
        d['saved_by']=s.headers.get('Tailscale-User-Login','')
        b=json.dumps(d,indent=1).encode()
        tmp=SEL.with_suffix('.tmp'); tmp.write_bytes(b); os.replace(tmp,SEL)
        (HIST/f"selection-{int(time.time())}.json").write_bytes(b)
        s.send_response(200); s.send_header('Content-Type','application/json'); s.end_headers(); s.wfile.write(json.dumps({'saved_at':d['saved_at']}).encode())
http.server.ThreadingHTTPServer(('127.0.0.1',8787),H).serve_forever()
