# Resumable batch repair of truncated Stoarama MP4s. Run: python3 -I repair.py [--limit N] [--ids a,b,c] [--init-only]
import sys, os, re, json, time, struct, sqlite3, hashlib, threading, queue, subprocess, signal, base64, collections, argparse, shutil
from fractions import Fraction
BASE = os.environ.get('REPAIR_BASE') or os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB = f'{BASE}/ledger.sqlite'; WORK = f'{BASE}/work'; STOP = f'{BASE}/STOP'; LOG = f'{BASE}/ledger.jsonl'
FFMPEG = os.environ.get('REPAIR_FFMPEG', '/opt/homebrew/bin/ffmpeg'); FFPROBE = os.environ.get('REPAIR_FFPROBE', '/opt/homebrew/bin/ffprobe')
REMOTE = base64.b64encode(open(f'{BASE}/bin/presign_remote.py', 'rb').read()).decode()
CONC, BATCH = int(os.environ.get('REPAIR_CONC', 8)), int(os.environ.get('REPAIR_BATCH', 120))
TM = collections.defaultdict(float)
lock = threading.Lock(); stop = threading.Event()
def log(*a): print(time.strftime('%F %T'), *a, flush=True)

def connect():
    c = sqlite3.connect(DB, timeout=60, check_same_thread=False, isolation_level=None)
    c.execute('pragma journal_mode=wal'); return c
SCHEMA = '''create table if not exists clips(clip_id integer primary key, key text, db_size integer, stream_id integer, clip_start text,
 target_fps text, prio integer, status text default 'pending', attempts integer default 0, error text,
 orig_sha256 text, orig_size integer, repaired_key text, repaired_size integer, repaired_sha256 text,
 duration real, fps real, fps_src text, frames integer, note text, done_at text);
create index if not exists ix_pend on clips(status, prio, clip_id);'''

def init(c):
    c.executescript(SCHEMA)
    if c.execute('select count(*) from clips').fetchone()[0]: return
    win = {w['stream_id']: (w['keep_start_utc'][:19], w['keep_end_utc'][:19]) for w in json.load(open(f'{BASE}/selection.json'))}
    rows = []
    for l in open(f'{BASE}/candidates.tsv'):
        i, k, sz, sid, st, fps, r2 = l.rstrip('\n').split('\t')
        w = win.get(int(sid)); inside = 1 if (w and w[0] <= st < w[1]) else 0
        rows.append((int(i), k, int(sz), int(sid), st, fps, 0 if inside else 1, 'pending' if r2 == '1' else 'nas_only'))
    c.execute('begin'); c.executemany('insert into clips(clip_id,key,db_size,stream_id,clip_start,target_fps,prio,status) values(?,?,?,?,?,?,?,?)', rows); c.execute('commit')

# ---------- SPS / VUI fps ----------
class BR:
    def __init__(s, b): s.b, s.p = b, 0
    def u(s, n):
        v = 0
        for _ in range(n):
            if s.p >= len(s.b) * 8: raise ValueError('eof')
            v = (v << 1) | ((s.b[s.p >> 3] >> (7 - (s.p & 7))) & 1); s.p += 1
        return v
    def ue(s):
        z = 0
        while s.u(1) == 0:
            z += 1
            if z > 32: raise ValueError('ue')
        return (1 << z) - 1 + (s.u(z) if z else 0)
    def se(s):
        k = s.ue(); return (k + 1) // 2 if k & 1 else -(k // 2)
def sps_fps(nal):
    b = bytes(nal[1:]); b = re.sub(rb'\x00\x00\x03', b'\x00\x00', b)
    r = BR(b); prof = r.u(8); r.u(16); r.ue()
    if prof in (100, 110, 122, 244, 44, 83, 86, 118, 128, 138, 139, 134, 135):
        cf = r.ue()
        if cf == 3: r.u(1)
        r.ue(); r.ue(); r.u(1)
        if r.u(1):
            for i in range(8 if cf != 3 else 12):
                if r.u(1):
                    last = nxt = 8
                    for _ in range(16 if i < 6 else 64):
                        if nxt: nxt = (last + r.se()) % 256
                        last = last if nxt == 0 else nxt
    r.ue(); pt = r.ue()
    if pt == 0: r.ue()
    elif pt == 1:
        r.u(1); r.se(); r.se()
        for _ in range(r.ue()): r.se()
    r.ue(); r.u(1); r.ue(); r.ue()
    if not r.u(1): r.u(1)
    r.u(1)
    if r.u(1): [r.ue() for _ in range(4)]
    if not r.u(1): return None
    if r.u(1):
        if r.u(8) == 255: r.u(32)
    if r.u(1): r.u(1)
    if r.u(1):
        r.u(4)
        if r.u(1): r.u(24)
    if r.u(1): r.ue(); r.ue()
    if not r.u(1): return None
    nu, ts = r.u(32), r.u(32)
    if nu == 0 or ts == 0: return None
    return Fraction(ts, 2 * nu)

# ---------- mdat -> annexb ----------
def check_sig(head):
    return len(head) >= 48 and head[44:48] == b'mdat' and struct.unpack('>I', head[40:44])[0] == 0 and head[4:8] == b'ftyp' and b'moov' not in head[:48]
AUD_RE = re.compile(rb'\x00\x00\x00\x02\x09[\x10\x30\x50\x70\x90\xb0\xd0\xf0]')
def parse_mdat(data):
    """Walk length-prefixed NALs; where junk (interleaved audio / zero holes) appears, resync on the next AUD. Returns (nals, stopmsg, gaps, gap_idx)"""
    off = 0
    while off + 8 <= len(data):
        sz, typ = struct.unpack('>I4s', data[off:off+8])
        if typ == b'mdat': off += 16 if sz == 1 else 8; break
        off += sz
    nals = []; stopmsg = 'eof'; gaps = 0; gap_idx = []
    while off + 4 <= len(data):
        ln = struct.unpack('>I', data[off:off+4])[0]
        ok = 0 < ln <= 8_000_000 and off + 4 + ln <= len(data) and not (data[off+4] & 0x80) and (data[off+4] & 0x1f) in (1, 2, 3, 4, 5, 6, 7, 8, 9)
        if not ok:
            m = AUD_RE.search(data, off + 1)
            if off + 4 + ln > len(data) and 0 < ln <= 8_000_000 and (data[off+4] & 0x1f) in (1, 5) and not m:
                stopmsg = 'partial'; break
            if not m: stopmsg = 'junk_tail' if off + 4 + ln <= len(data) or ln > 8_000_000 else 'partial'; break
            off = m.start(); gaps += 1; gap_idx.append(len(nals)); continue
        nals.append(data[off+4:off+4+ln]); off += 4 + ln
    return nals, stopmsg, gaps, gap_idx
def build(nals, drop_vcl=0, upto=None):
    if upto is not None: nals = nals[:upto]
    vcl = [i for i, n in enumerate(nals) if 1 <= (n[0] & 0x1f) <= 5]
    if len(vcl) <= drop_vcl: return None
    end = vcl[-1 - drop_vcl] + 1
    return b''.join(b'\x00\x00\x00\x01' + n for n in nals[:end]), len(vcl) - drop_vcl

def run(cmd, **kw): return subprocess.run(cmd, capture_output=True, text=True, **kw)
def sha_md5(path):
    s, m = hashlib.sha256(), hashlib.md5()
    with open(path, 'rb') as f:
        for ch in iter(lambda: f.read(1 << 20), b''): s.update(ch); m.update(ch)
    return s.hexdigest(), m.hexdigest()

class Transient(Exception): pass
def curl(args, retries=3):
    last = ''
    for a in range(retries):
        p = run(['curl', '-sS', '--max-time', '900', '--connect-timeout', '20', *args])
        if p.returncode == 0: return p.stdout
        last = p.stderr.strip()[:200]; time.sleep(2 * (a + 1))
    raise Transient('curl: ' + last)
def http(method_args, url):
    out = curl([*method_args, '-o', '/dev/null', '-w', '%{http_code}', url]); return int(out.strip() or 0)

def process(row, urls):
    cid, key, dbsize, tfps = row['clip_id'], row['key'], row['db_size'], row['target_fps']
    get, head, put = urls
    d = f'{WORK}/{cid}'; os.makedirs(d, exist_ok=True)
    res = dict(repaired_key='repaired/' + key)
    try:
        src, h264, rep = f'{d}/src.mp4', f'{d}/s.h264', f'{d}/rep.mp4'
        t0 = time.time()
        p = run(['curl', '-sS', '--max-time', '900', '--connect-timeout', '20', '-w', '%{http_code}', '-o', src, get])
        code = p.stdout.strip()
        if p.returncode != 0: raise Transient('download: ' + p.stderr.strip()[:150])
        if code in ('404', '410'): return 'missing_r2', dict(res, error='HTTP ' + code)
        if code != '200': raise Transient('download http ' + code)
        osz = os.path.getsize(src); res['orig_size'] = osz; TM['dl'] += time.time() - t0; t0 = time.time()
        if osz != dbsize: return 'skipped_signature', dict(res, error=f'size {osz} != db {dbsize}')
        data = open(src, 'rb').read()
        if not check_sig(data[:48]) or b'moov' in data[:48]: return 'skipped_signature', dict(res, error='header signature mismatch')
        res['orig_sha256'] = hashlib.sha256(data).hexdigest()
        nals, stopmsg, gaps, gap_idx = parse_mdat(data); del data
        sps = next((n for n in nals if n[0] & 0x1f == 7), None)
        fps, fsrc = None, None
        if sps is not None:
            try:
                f = sps_fps(sps)
                if f and 5 <= f <= 120: fps, fsrc = f.limit_denominator(1001), 'vui'
            except Exception: pass
        if fps is None:
            if tfps.isdigit() and 5 <= int(tfps) <= 120: fps, fsrc = Fraction(int(tfps)), 'db'
            else: fps, fsrc = Fraction(30), 'default'
        res['fps'], res['fps_src'] = float(fps), fsrc
        def attempt(drop, upto):
            b = build(nals, drop, upto)
            if b is None: return 'no video frames'
            open(h264, 'wb').write(b[0])
            p = run([FFMPEG, '-nostdin', '-v', 'error', '-y', '-framerate', f'{fps.numerator}/{fps.denominator}', '-f', 'h264', '-i', h264,
                     '-c', 'copy', '-movflags', '+faststart', rep])
            if p.returncode != 0: return 'remux: ' + p.stderr.strip()[:200]
            p = run([FFMPEG, '-nostdin', '-v', 'error', '-xerror', '-i', rep, '-f', 'null', '-'])
            if p.returncode != 0 or p.stderr.strip(): return 'decode: ' + p.stderr.strip()[:200]
            return ''
        err = ''; attempts = [(d, None) for d in (0, 1)] + ([(d, gap_idx[0]) for d in (0, 1)] if gap_idx else [])
        for drop, upto in attempts:
            err = attempt(drop, upto)
            if not err:
                res['note'] = f'stop={stopmsg} gaps={gaps}' + (f' dropped_extra_vcl={drop}' if drop else '') + (' truncated_at_first_gap' if upto is not None else ''); break
        if err.startswith('decode'):
            # corrupt data inside the stream: keep the longest clean prefix of pictures (binary search on dropped VCL count)
            nv = sum(1 for n in nals if 1 <= (n[0] & 0x1f) <= 5); lo, hi = 0, nv - 1   # drop count: lo=known-bad, hi=assumed-good
            first = err
            if not attempt(hi, None):
                while hi - lo > 1:
                    mid = (lo + hi) // 2
                    if attempt(mid, None): lo = mid   # non-empty = error
                    else: hi = mid
                # smallest clean drop count is hi
                err = attempt(hi, None)
                if not err: res['note'] = f'stop={stopmsg} gaps={gaps} TRUNCATED_AT_CORRUPTION kept={nv - hi}/{nv} pictures'
            else: err = first
        if err: return 'decode_failed', dict(res, error=err)
        p = run([FFPROBE, '-v', 'error', '-count_packets', '-select_streams', 'v:0', '-show_entries', 'stream=nb_read_packets:format=duration', '-of', 'json', rep])
        j = json.loads(p.stdout); res['frames'] = int(j['streams'][0]['nb_read_packets']); res['duration'] = float(j['format']['duration'])
        if not (1 <= res['duration'] <= 60): return 'unusable_duration', dict(res, error=f"duration {res['duration']:.2f}s frames {res['frames']} (not uploaded)")
        TM['fix'] += time.time() - t0; t0 = time.time()
        res['repaired_sha256'], md5 = sha_md5(rep); rsz = res['repaired_size'] = os.path.getsize(rep)
        # existence check before PUT: never overwrite
        r = run(['curl', '-sS', '-I', '--max-time', '60', head])
        if r.returncode != 0: raise Transient('head: ' + r.stderr.strip()[:150])
        hl = r.stdout.splitlines()[0]
        if row.get('overwrite_junk'):
            hd = {k.lower(): v for k, v in (x.split(': ', 1) for x in r.stdout.splitlines()[1:] if ': ' in x)}
            if ' 200' in hl and int(hd.get('content-length', -1)) != json.load(open(f'{BASE}/junk_sizes.json')).get(str(cid)): return 'conflict', dict(res, error=f"junk overwrite: existing object size {hd.get('content-length')} != recorded junk {json.load(open(f'{BASE}/junk_sizes.json')).get(str(cid))}; not overwritten")
            code = http(['-X', 'PUT', '-T', rep, '-H', 'Content-Type: video/mp4', '-H', 'Expect:'], put)
            if code != 200: raise Transient(f'put http {code}')
            return 'done', dict(res, note=(res.get('note') or '') + ' overwrote_junk')
        if ' 200' in hl:
            hd = {k.lower(): v for k, v in (x.split(': ', 1) for x in r.stdout.splitlines()[1:] if ': ' in x)}
            if int(hd.get('content-length', -1)) == rsz and hd.get('etag', '').strip('"').lower() == md5: return 'done', dict(res, note=(res.get('note') or '') + ' already_present')
            return 'conflict', dict(res, error='repaired key exists with different content; not overwritten')
        if ' 404' not in hl: raise Transient('head: ' + hl[:80])
        code = http(['-X', 'PUT', '-T', rep, '-H', 'Content-Type: video/mp4', '-H', 'If-None-Match: *', '-H', 'Expect:'], put)
        TM['up'] += time.time() - t0; TM['n'] += 1
        if code == 412: return 'conflict', dict(res, error='PUT 412 key exists; not overwritten')
        if code != 200: raise Transient(f'put http {code}')
        return 'done', res
    finally:
        shutil.rmtree(d, ignore_errors=True)

def finish(c, cid, status, res, attempts):
    err = res.get('error')
    cols = ['orig_sha256','orig_size','repaired_key','repaired_size','repaired_sha256','duration','fps','fps_src','frames','note']
    with lock:
        c.execute(f"update clips set status=?, error=?, attempts=?, done_at=datetime('now'), {', '.join(x+'=?' for x in cols)} where clip_id=?",
                  (status, err, attempts, *[res.get(x) for x in cols], cid))
        with open(LOG, 'a') as f: f.write(json.dumps(dict(clip_id=cid, status=status, **res)) + '\n')

def presign(rows):
    inp = ''.join(f"{r['clip_id']}\t{r['key']}\n" for r in rows)
    if os.environ.get('REPAIR_PRESIGN', 'local') == 'local':  # running on Jelly, where the scoped R2 credentials live
        p = subprocess.run(['/usr/bin/python3', '-I', f'{BASE}/bin/presign_remote.py'], input=inp, capture_output=True, text=True, timeout=300)
    else:
        p = subprocess.run(['ssh', '-o', 'BatchMode=yes', '-o', 'ConnectTimeout=20', 'jelly@jellys-mac-mini',
                            f"python3 -c \"import base64;exec(base64.b64decode('{REMOTE}'))\""], input=inp, capture_output=True, text=True, timeout=300)
    if p.returncode != 0: raise RuntimeError('presign ssh failed: ' + p.stderr.strip()[:200])
    out = {}
    for l in p.stdout.splitlines():
        i, g, h, u = l.split('\t'); out[int(i)] = (g, h, u)
    return out

def main():
    ap = argparse.ArgumentParser(); ap.add_argument('--limit', type=int, default=0); ap.add_argument('--ids', default=''); ap.add_argument('--init-only', action='store_true'); ap.add_argument('--junk', action='store_true')
    ap.add_argument('--pilot', action='store_true'); a = ap.parse_args()
    os.makedirs(WORK, exist_ok=True); c = connect(); init(c)
    if a.init_only: return
    for sig in (signal.SIGTERM, signal.SIGINT): signal.signal(sig, lambda *x: stop.set())
    c.execute("update clips set status='pending' where status='inprogress'")
    # leftover work dirs from a crash
    for dd in os.listdir(WORK): shutil.rmtree(f'{WORK}/{dd}', ignore_errors=True)
    q = queue.Queue(maxsize=CONC * 2); dispatched = 0; done_flag = object()
    def producer():
        nonlocal dispatched
        try:
            while not stop.is_set() and not os.path.exists(STOP):
                if a.junk: rows = c.execute("select * from clips where status='junk_uploaded' order by clip_id").fetchall()
                elif a.ids: rows = c.execute(f"select * from clips where status='pending' and clip_id in ({a.ids}) order by clip_id limit {BATCH}").fetchall()
                else:
                    lim = BATCH if not a.limit else min(BATCH, a.limit - dispatched)
                    if lim <= 0: break
                    rows = c.execute("select * from clips where status='pending' order by prio, clip_id limit ?", (lim,)).fetchall() if not a.pilot else \
                           c.execute("select * from clips where status='pending' and prio=0 order by random() limit ?", (lim,)).fetchall()
                if not rows: break
                cols = [x[0] for x in c.execute('select * from clips limit 0').description]; rows = [dict(zip(cols, r), overwrite_junk=a.junk) for r in rows]
                with lock: c.execute('begin'); c.executemany("update clips set status='inprogress' where clip_id=?", [(r['clip_id'],) for r in rows]); c.execute('commit')
                try: urls = presign(rows)
                except Exception as e:
                    log('presign error', e)
                    with lock: c.execute('begin'); c.executemany("update clips set status='pending' where clip_id=?", [(r['clip_id'],) for r in rows]); c.execute('commit')
                    time.sleep(30); continue
                for r in rows:
                    if r['clip_id'] in urls: q.put((r, urls[r['clip_id']], time.time())); dispatched += 1
                    else:
                        with lock: c.execute("update clips set status='pending' where clip_id=?", (r['clip_id'],))
                if a.junk or a.ids or (a.limit and dispatched >= a.limit): break
        finally:
            for _ in range(CONC): q.put(done_flag)
    def worker():
        while True:
            it = q.get()
            if it is done_flag: return
            r, urls, t0 = it
            if stop.is_set() or os.path.exists(STOP) or time.time() - t0 > 6000:
                with lock: c.execute("update clips set status='pending' where clip_id=?", (r['clip_id'],)); continue
            att = r['attempts'] + 1
            try: st, res = process(r, urls)
            except Transient as e: st, res = ('failed' if att >= 3 else 'pending'), dict(error=str(e)[:300])
            except Exception as e: st, res = ('failed' if att >= 3 else 'pending'), dict(error=('exc ' + repr(e))[:300])
            finish(c, r['clip_id'], st, res, att)
            if TM['n'] and int(TM['n']) % 40 == 0: log('TIMING avg s/clip dl=%.1f fix=%.1f up=%.1f n=%d' % (TM['dl']/TM['n'], TM['fix']/TM['n'], TM['up']/TM['n'], TM['n']))
            log(r['clip_id'], st, res.get('error') or f"{res.get('duration')}s {res.get('fps')}fps")
    ts = [threading.Thread(target=producer)] + [threading.Thread(target=worker) for _ in range(CONC)]
    [t.start() for t in ts]; [t.join() for t in ts]
    log('exit', dict(c.execute('select status, count(*) from clips group by 1').fetchall()))
main()
