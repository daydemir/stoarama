#!/usr/bin/env python3
"""Operator-host finite worker bootstrap. Default dry-run; no builds or media.

SSH always uses the operator's existing CLI identity and strict pinned host keys.
Only a single expiring binary GET capability crosses to a worker via stdin.
"""
import argparse
import base64
import datetime
import hashlib
import hmac
import ipaddress
import json
import os
import pathlib
import shlex
import subprocess
import time
import urllib.parse

TAG='best14-sprint-20261026'
BINARY_SHA='aff7398ccfd41f98255e1361f9d7c5aa1d8a52d54e43f2f9393a59ffd03a5479'
BINARY_BYTES=10347727
BINARY_KEY='staging/best14-20261021/worker-runtime/'+BINARY_SHA+'/collation-job'
SOURCE_READ="""import pathlib,json,base64,hashlib,subprocess
r=pathlib.Path('/root/Build/stoarama');p=r/'infra/collation-v2'
print(json.dumps({'commit':subprocess.check_output(['git','rev-parse','HEAD'],cwd=r,text=True).strip(),'files':{n:base64.b64encode((p/n).read_bytes()).decode() for n in ['capability-worker.py','capability_export.py']}}))"""
REMOTE_BOOTSTRAP=r'''
import json,sys,pathlib,os,subprocess,hashlib,base64,pwd,grp,time,urllib.request,ipaddress
p=json.load(sys.stdin)
SERVICE_USER='stoarama'  # Public Unix service identity; no password is supplied.
def run(argv):return subprocess.check_output(argv,stderr=subprocess.DEVNULL,text=True).strip()
def fail():raise RuntimeError('bootstrap identity or storage check refused')
h=p['host'];name=h['name'];volume=h['volume_name'];private=h['private_ip']
if run(['hostname'])!=name:fail()
if ipaddress.ip_address(private) not in ipaddress.ip_network('10.0.0.0/8'):fail()
device=pathlib.Path('/dev/disk/by-id/scsi-0DO_Volume_'+volume)
if not device.exists() or not device.is_symlink():fail()
resolved=device.resolve()
if run(['blkid','-s','TYPE','-o','value',str(resolved)])!='ext4':fail()
uuid=run(['blkid','-s','UUID','-o','value',str(resolved)])
if not uuid or pathlib.Path('/dev/disk/by-uuid',uuid).resolve()!=resolved:fail()
# lsblk proves this is a volume block device, never the boot root device.
rows=json.loads(run(['lsblk','-J','-o','PATH,TYPE,FSTYPE,UUID,MOUNTPOINTS',str(resolved)]))['blockdevices']
if len(rows)!=1 or rows[0]['type']!='disk' or rows[0].get('uuid')!=uuid:fail()
mount=pathlib.Path('/mnt')/volume
current=run(['findmnt','-rn','-S',str(resolved),'-o','TARGET']) if any(rows[0].get('mountpoints') or []) else ''
if current and current!=str(mount):fail()
if not mount.exists():mount.mkdir(mode=0o555)
if mount.is_symlink():fail()
if not os.path.ismount(mount):
 if any(mount.iterdir()):fail()
 os.chown(mount,0,0);mount.chmod(0o555)
 subprocess.run(['mount','-t','ext4','UUID='+uuid,str(mount)],check=True,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
if not os.path.ismount(mount) or mount.stat().st_dev!=resolved.stat().st_rdev:fail()
os.chown(mount,0,0);mount.chmod(0o755)
def require_mount():
 if not os.path.ismount(mount) or mount.stat().st_dev!=resolved.stat().st_rdev:fail()
fstab=pathlib.Path('/etc/fstab');entry='UUID='+uuid+' '+str(mount)+' ext4 defaults,nofail 0 2'
lines=fstab.read_text().splitlines()
conflicting=[l for l in lines if l and not l.startswith('#') and (str(mount) in l.split() or 'UUID='+uuid in l.split())]
if conflicting and conflicting!=[entry]:fail()
if not conflicting:
 with fstab.open('a') as out:out.write('\n'+entry+'\n');out.flush();os.fsync(out.fileno())
try:account=pwd.getpwnam(SERVICE_USER)
except KeyError:
 subprocess.run(['useradd','--system','--user-group','--no-create-home','--shell','/usr/sbin/nologin',SERVICE_USER],check=True,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
 account=pwd.getpwnam(SERVICE_USER)
if account.pw_uid==0 or grp.getgrgid(account.pw_gid).gr_name!='stoarama' or set(os.getgrouplist(SERVICE_USER,account.pw_gid))!={account.pw_gid}:fail()
# Packages and immutable runtime live on the boot OS; all job/tmp/log data is
# exclusively below the retained mount, under the unprivileged worker account.
env=dict(os.environ,DEBIAN_FRONTEND='noninteractive')
for argv in [['apt-get','update','-qq'],['apt-get','install','-y','-qq','python3','ffmpeg','ca-certificates']]:subprocess.run(argv,check=True,env=env,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
app=pathlib.Path('/opt/stoarama-best14')
if app.is_symlink():fail()
app.mkdir(mode=0o755,exist_ok=True)
for name,target in [('capability-worker.py','worker.py'),('capability_export.py','capability_export.py')]:
 body=base64.b64decode(p['files'][name]);
 if hashlib.sha256(body).hexdigest()!=p['source_sha256'][name]:fail()
 path=app/target
 if path.is_symlink():fail()
 if path.exists() and hashlib.sha256(path.read_bytes()).hexdigest()!=p['source_sha256'][name]:fail()
 path.write_bytes(body);os.chown(path,0,0);path.chmod(0o555)
binary=app/'collation-job'
if binary.is_symlink():fail()
if not binary.exists():
 try:
  with urllib.request.urlopen(p['binary_get'],timeout=120) as response:body=response.read(p['binary_bytes']+1)
 except Exception:raise RuntimeError('pinned binary download unavailable') from None
 if len(body)!=p['binary_bytes'] or hashlib.sha256(body).hexdigest()!=p['binary_sha256']:fail()
 fd=os.open(binary,os.O_WRONLY|os.O_CREAT|os.O_EXCL,0o555)
 with os.fdopen(fd,'wb') as out:out.write(body);out.flush();os.fsync(out.fileno())
if binary.stat().st_size!=p['binary_bytes'] or hashlib.sha256(binary.read_bytes()).hexdigest()!=p['binary_sha256']:fail()
os.chown(binary,0,0);binary.chmod(0o555)
app.chmod(0o555)
workers=[]
for index in range(22):
 require_mount()
 root=mount/'workers'/str(index).zfill(2);root.mkdir(mode=0o700,parents=True,exist_ok=True)
 os.chown(root,account.pw_uid,account.pw_gid);root.chmod(0o700)
 for child in ['tmp','logs']:
  path=root/child;path.mkdir(mode=0o700,exist_ok=True);os.chown(path,account.pw_uid,account.pw_gid)
 log=root/'logs'/'worker.log'
 fd=os.open(log,os.O_CREAT|os.O_APPEND|os.O_WRONLY,0o600);os.fchown(fd,account.pw_uid,account.pw_gid);os.close(fd)
 marker=root/'build-complete'
 if not marker.exists():marker.write_text(p['binary_sha256']+'\n');os.chown(marker,account.pw_uid,account.pw_gid);marker.chmod(0o600)
 port=8080+index;unit='stoarama-best14-'+str(index).zfill(2)+'.service'
 config='[Unit]\nDescription=Finite retained-volume stitch worker\nAfter=network-online.target\nRequiresMountsFor='+str(mount)+'\n[Service]\nType=simple\nUser=stoarama\nGroup=stoarama\nWorkingDirectory='+str(root)+'\nEnvironment=PYTHONDONTWRITEBYTECODE=1\nEnvironment=STOARAMA_WORKER_ROOT='+str(root)+'\nEnvironment=STOARAMA_WORKER_BINARY='+str(binary)+'\nEnvironment=STOARAMA_WORKER_BIND='+private+'\nEnvironment=STOARAMA_WORKER_PORT='+str(port)+'\nEnvironment=STOARAMA_VOLUME_MOUNT='+str(mount)+'\nEnvironment=STOARAMA_VOLUME_UUID='+uuid+'\nEnvironment=TMPDIR='+str(root/'tmp')+'\nExecStart=/usr/bin/python3 '+str(app/'worker.py')+'\nStandardOutput=append:'+str(log)+'\nStandardError=append:'+str(log)+'\nNoNewPrivileges=true\nProtectSystem=strict\nProtectHome=true\nReadWritePaths='+str(mount)+'\nRestart=on-failure\nRestartSec=5\nTimeoutStopSec=1870\n[Install]\nWantedBy=multi-user.target\n'
 (pathlib.Path('/etc/systemd/system')/unit).write_text(config)
 workers.append({'id':'sprint-'+h['suffix']+'-'+str(index).zfill(2),'host_id':h['id'],'host':private,'port':port,'unit':unit})
subprocess.run(['systemctl','daemon-reload'],check=True,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
require_mount()
for w in workers:subprocess.run(['systemctl','enable','--now',w['unit']],check=True,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
# Probe only readiness, never dispatch media or create job requests.
for w in workers:
 ready=None
 for attempt in range(20):
  try:
   with urllib.request.urlopen('http://'+private+':'+str(w['port'])+'/ready',timeout=2) as response:ready=json.load(response)
   if ready.get('build_complete'):break
  except Exception:time.sleep(.25)
 if not ready or not ready.get('build_complete') or ready.get('active_jobs')!=0 or ready.get('root_device')!=mount.stat().st_dev or not ready.get('volume_identity'):fail()
 w.update(root_device=ready['root_device'],volume_identity=ready['volume_identity']);w.pop('unit')
os.sync()
print(json.dumps({'host':h,'volume_uuid':uuid,'mount':str(mount),'workers':workers,'binary_sha256':p['binary_sha256'],'source_sha256':p['source_sha256'],'ready_verified':True,'workload_dispatched':False}))
'''

class BootstrapError(RuntimeError):pass

def ssh_command(host,knownhosts,code):
    if not host or any(c.isspace() for c in host):raise BootstrapError('invalid SSH host')
    return ['ssh','-o','BatchMode=yes','-o','StrictHostKeyChecking=yes','-o','UserKnownHostsFile='+str(knownhosts),'root@'+host,'python3 -c '+shlex.quote(code)]

def ssh(host,knownhosts,code,payload=None):
    result=subprocess.run(ssh_command(host,knownhosts,code),input=json.dumps(payload).encode() if payload is not None else b'',stdout=subprocess.PIPE,stderr=subprocess.PIPE,timeout=900)
    if result.returncode:raise BootstrapError('remote bootstrap unavailable; inspect host and retained-volume state')
    try:return json.loads(result.stdout)
    except Exception:raise BootstrapError('remote bootstrap receipt invalid') from None

def signed_binary_get(envpath):
    env={}
    for line in pathlib.Path(envpath).read_text().splitlines():
        if not line.strip() or line.lstrip().startswith('#'):continue
        key,value=line.removeprefix('export ').split('=',1);parts=shlex.split(value)
        if len(parts)==1:env[key]=parts[0]
    host=env['R2_ACCOUNT_ID']+'.r2.cloudflarestorage.com';stamp=datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%dT%H%M%SZ');day=stamp[:8];scope=day+'/auto/s3/aws4_request';uri='/stoarama/'+BINARY_KEY
    values={'X-Amz-Algorithm':'AWS4-HMAC-SHA256','X-Amz-Credential':env['R2_ACCESS_KEY_ID']+'/'+scope,'X-Amz-Date':stamp,'X-Amz-Expires':'3600','X-Amz-SignedHeaders':'host'}
    query='&'.join(urllib.parse.quote(k,safe='')+'='+urllib.parse.quote(v,safe='~') for k,v in sorted(values.items()))
    canonical='GET\n'+uri+'\n'+query+'\nhost:'+host+'\n\nhost\nUNSIGNED-PAYLOAD'
    key=('AWS4'+env['R2_SECRET_ACCESS_KEY']).encode()
    for part in (day,'auto','s3','aws4_request'):key=hmac.new(key,part.encode(),hashlib.sha256).digest()
    signature=hmac.new(key,('AWS4-HMAC-SHA256\n'+stamp+'\n'+scope+'\n'+hashlib.sha256(canonical.encode()).hexdigest()).encode(),hashlib.sha256).hexdigest()
    return 'https://'+host+uri+'?'+query+'&X-Amz-Signature='+signature


def validate(policy,review,sources,tracker):
    if policy.get('allow_start') is not True or policy.get('additional_budget_usd')!=150:raise BootstrapError('approved 150 USD policy required')
    commit=sources.get('commit')
    if not commit or review.get('commit')!=commit or review.get('review_passed') is not True or review.get('merged_default') is not True or tracker.get('reviewed_commit')!=commit:raise BootstrapError('reviewed merged source differs')
    manifest=policy.get('worker_source_sha256',{})
    for name in ('capability-worker.py','capability_export.py'):
        if hashlib.sha256(base64.b64decode(sources['files'][name])).hexdigest()!=manifest.get(name):raise BootstrapError('worker source manifest differs')
    firewall=tracker.get('firewall',{}).get('receipt',{})
    if not tracker.get('prepared') or firewall.get('status')!='succeeded':raise BootstrapError('private firewall not ready')
    ids=[tracker['resources']['cpu-'+s]['receipt']['id'] for s in 'abc']
    if sorted(firewall.get('droplet_ids',[]))!=sorted(ids) or firewall.get('tags',[]):raise BootstrapError('private firewall host scope differs')
    if len(firewall.get('inbound_rules',[]))!=2:raise BootstrapError('unexpected inbound firewall rules')
    for rule in firewall.get('inbound_rules',[]):
        if any(v for k,v in rule.get('sources',{}).items() if k!='addresses'):raise BootstrapError('unexpected firewall source peers')
    app=[r for r in firewall.get('inbound_rules',[]) if r.get('protocol')=='tcp' and r.get('ports')=='8080-8101']
    if len(app)!=1:raise BootstrapError('worker firewall rule missing')
    addresses=app[0].get('sources',{}).get('addresses',[])
    if len(addresses)!=1 or not ipaddress.ip_network(addresses[0]).subnet_of(ipaddress.ip_network('10.0.0.0/8')) or not addresses[0].endswith('/32'):raise BootstrapError('worker firewall private source differs')
    if any(r.get('ports') not in ('22','8080-8101') for r in firewall.get('inbound_rules',[])):raise BootstrapError('unexpected inbound firewall rule')
    return manifest

RECORD="""import pathlib,json,sys,os
p=json.load(sys.stdin);root=pathlib.Path('/var/lib/stoarama-best14')
for name,value in p.items():
 if not (name=='sprint-roster.json' or name.startswith('sprint-bootstrap-') and name.endswith('.json')) or '/' in name:raise ValueError('receipt name refused')
 path=root/name;temp=path.with_suffix('.pending');fd=os.open(temp,os.O_WRONLY|os.O_CREAT|os.O_TRUNC,0o600)
 with os.fdopen(fd,'w') as out:json.dump(value,out);out.flush();os.fsync(out.fileno())
 os.replace(temp,path)
fd=os.open(root,os.O_RDONLY|os.O_DIRECTORY);os.fsync(fd);os.close(fd)
print(json.dumps({'durable':True}))"""


def main():
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--apply',action='store_true');parser.add_argument('--controller',default='159.203.177.94');parser.add_argument('--known-hosts',default='/Users/jelly/.stoarama/best14-sprint-20261026/known-hosts');parser.add_argument('--policy');parser.add_argument('--review-receipt');parser.add_argument('--r2-env',default='/Users/jelly/Build/stoarama/local/render.env');args=parser.parse_args()
    if not args.apply:
        print(json.dumps({'mode':'dry-run','workers':66,'hosts':3,'no_local_build':True,'binary_sha256':BINARY_SHA,'requires':'approved policy, reviewed source hashes, strict pinned SSH hosts, applied private firewall'}));return
    if not args.policy or not args.review_receipt:raise BootstrapError('policy and review receipt required')
    policy=json.loads(pathlib.Path(args.policy).read_text());review=json.loads(pathlib.Path(args.review_receipt).read_text())
    if policy.get('allow_start') is not True or policy.get('additional_budget_usd')!=150:raise BootstrapError('approved policy required before SSH')
    tracker=ssh(args.controller,args.known_hosts,"import json,pathlib;print((pathlib.Path('/var/lib/stoarama-best14')/'sprint-physical-resources.json').read_text())")
    sources=ssh(args.controller,args.known_hosts,SOURCE_READ);manifest=validate(policy,review,sources,tracker)
    hosts=[];workers=[]
    for suffix in 'abc':
        cpu=tracker['resources']['cpu-'+suffix]['receipt'];volume=tracker['resources']['volume-'+suffix]['receipt']
        fresh=cpu.get('networks',{}).get('v4',[]);public=[n['ip_address'] for n in fresh if n.get('type')=='public'];private=[n['ip_address'] for n in fresh if n.get('type')=='private']
        if len(public)!=1 or len(private)!=1 or cpu.get('name')!='stoarama-'+TAG+'-'+suffix or volume.get('name')!=TAG+'-'+suffix or cpu.get('volume_ids')!=[volume['id']] or cpu.get('size_slug')!='c-32' or volume.get('size_gigabytes')!=80 or volume.get('filesystem_type')!='ext4' or volume.get('region',{}).get('slug')!='nyc1' or not set((TAG,'projectstoarama'))<=set(volume.get('tags',[])) or not set((TAG,'projectstoarama'))<=set(cpu.get('tags',[])):raise BootstrapError('physical host identity differs')
        host={'id':cpu['id'],'name':cpu['name'],'suffix':suffix,'volume_id':volume['id'],'volume_name':volume['name'],'volume_gib':80,'size':'c-32','hourly_usd':1,'created_at_epoch':datetime.datetime.fromisoformat(cpu['created_at'].replace('Z','+00:00')).timestamp(),'private_ip':private[0]}
        payload={'host':host,'files':sources['files'],'source_sha256':manifest,'binary_sha256':BINARY_SHA,'binary_bytes':BINARY_BYTES,'binary_get':signed_binary_get(args.r2_env)}
        receipt=ssh(public[0],args.known_hosts,REMOTE_BOOTSTRAP,payload)
        if receipt.get('ready_verified') is not True or len(receipt.get('workers',[]))!=22:raise BootstrapError('worker readiness incomplete')
        ssh(args.controller,args.known_hosts,RECORD,{'sprint-bootstrap-'+suffix+'.json':receipt})
        hosts.append(host);workers.extend(receipt['workers'])
    ssh(args.controller,args.known_hosts,RECORD,{'sprint-roster.json':{'hosts':hosts,'workers':workers}})
    print(json.dumps({'bootstrapped_hosts':3,'ready_workers':66,'workload_dispatched':False}))

if __name__=='__main__':
    try:main()
    except Exception:raise SystemExit('bootstrap blocked; inspect pinned host, policy, firewall, and retained-volume receipt') from None
