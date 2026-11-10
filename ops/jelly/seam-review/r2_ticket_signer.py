"""Server-only, one-object GET signing with the existing R2 environment."""
import os,json,sys,re,hmac,hashlib,datetime
from urllib.parse import quote

def sign(o,conditional=True,now=None):
 account=os.environ['R2_ACCOUNT_ID'];bucket=os.environ['R2_BUCKET'];region=os.environ.get('R2_REGION','auto');access=os.environ['R2_ACCESS_KEY_ID'];secret=os.environ['R2_SECRET_ACCESS_KEY']
 if not re.fullmatch('[0-9a-f]{32}',account) or not re.fullmatch('[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]',bucket):raise ValueError('Invalid configured storage')
 host=account+'.r2.cloudflarestorage.com';path='/'+quote(bucket,safe='')+'/'+quote(o['object_key'],safe='/~')
 now=now or datetime.datetime.now(datetime.timezone.utc);stamp=now.strftime('%Y%m%dT%H%M%SZ');day=stamp[:8];scope=day+'/'+region+'/s3/aws4_request'
 name=o['path'].split('/')[-1];fallback=re.sub('[^A-Za-z0-9._-]','_',name);disp='attachment; filename="'+fallback+'"; filename*=UTF-8\'\''+quote(name,safe='')
 headers={'host':host}
 if conditional:headers['if-match']=o['etag']
 signed=';'.join(sorted(headers));q={'X-Amz-Algorithm':'AWS4-HMAC-SHA256','X-Amz-Credential':access+'/'+scope,'X-Amz-Date':stamp,'X-Amz-Expires':'600','X-Amz-SignedHeaders':signed,'response-content-disposition':disp}
 query='&'.join(quote(k,safe='~')+'='+quote(v,safe='~') for k,v in sorted(q.items()))
 canonical='GET\n'+path+'\n'+query+'\n'+''.join(k+':'+headers[k]+'\n' for k in sorted(headers))+'\n'+signed+'\nUNSIGNED-PAYLOAD'
 msg='AWS4-HMAC-SHA256\n'+stamp+'\n'+scope+'\n'+hashlib.sha256(canonical.encode()).hexdigest()
 key=('AWS4'+secret).encode()
 for step in (day,region,'s3','aws4_request'):key=hmac.new(key,step.encode(),hashlib.sha256).digest()
 signature=hmac.new(key,msg.encode(),hashlib.sha256).hexdigest()
 return {'url':'https://'+host+path+'?'+query+'&X-Amz-Signature='+signature,'storage_host':host,'object_path_sha256':hashlib.sha256(path.encode()).hexdigest(),'expires_at':now.timestamp()+600,'required_headers':{'If-Match':o['etag']} if conditional else {},'etag':o['etag'],'sha256':o['sha256'],'size_bytes':o['size_bytes'],'filename':name,'transport':'r2-direct','ttl_seconds':600}
if __name__=='__main__':
 try:
  d=json.load(sys.stdin);print(json.dumps(sign(d['output'],d['conditional'])))
 except Exception:sys.exit('Storage signing unavailable')
