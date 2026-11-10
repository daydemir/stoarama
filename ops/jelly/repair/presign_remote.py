# Runs on Jelly via `ssh jelly@... python3 -c "$(cat presign_remote.py)"`; stdin: "id\tkey" lines; stdout: "id\tget\thead\tput"
# Never prints credentials. Presigned URLs for source key (GET) and repaired/<key> (HEAD, PUT).
import sys, hmac, hashlib, datetime, urllib.parse, re
env = {}
for l in open('/Users/jelly/Build/stoarama/local/render.env'):
    m = re.match(r'\s*(?:export\s+)?([A-Z0-9_]+)=(.*)$', l.rstrip('\n'))
    if m: env[m[1]] = m[2].strip().strip('"').strip("'")
AK, SK, ACC, BKT = env['R2_ACCESS_KEY_ID'], env['R2_SECRET_ACCESS_KEY'], env['R2_ACCOUNT_ID'], env['R2_BUCKET']
HOST = f'{ACC}.r2.cloudflarestorage.com'; EXP = 7200
def sign(k, m): return hmac.new(k, m.encode(), hashlib.sha256).digest()
def presign(method, key, now):
    amz = now.strftime('%Y%m%dT%H%M%SZ'); ds = amz[:8]; scope = f'{ds}/auto/s3/aws4_request'
    path = '/' + BKT + '/' + urllib.parse.quote(key, safe='/~')
    q = {'X-Amz-Algorithm': 'AWS4-HMAC-SHA256', 'X-Amz-Credential': f'{AK}/{scope}', 'X-Amz-Date': amz,
         'X-Amz-Expires': str(EXP), 'X-Amz-SignedHeaders': 'host'}
    cq = '&'.join(f'{urllib.parse.quote(k, safe="~")}={urllib.parse.quote(v, safe="~")}' for k, v in sorted(q.items()))
    creq = f'{method}\n{path}\n{cq}\nhost:{HOST}\n\nhost\nUNSIGNED-PAYLOAD'
    sts = f'AWS4-HMAC-SHA256\n{amz}\n{scope}\n{hashlib.sha256(creq.encode()).hexdigest()}'
    k = sign(sign(sign(sign(('AWS4' + SK).encode(), ds), 'auto'), 's3'), 'aws4_request')
    sig = hmac.new(k, sts.encode(), hashlib.sha256).hexdigest()
    return f'https://{HOST}{path}?{cq}&X-Amz-Signature={sig}'
now = datetime.datetime.now(datetime.timezone.utc)
for line in sys.stdin:
    line = line.rstrip('\n')
    if not line: continue
    i, key = line.split('\t', 1)
    rk = 'repaired/' + key
    print(i, presign('GET', key, now), presign('HEAD', rk, now), presign('PUT', rk, now), sep='\t')
