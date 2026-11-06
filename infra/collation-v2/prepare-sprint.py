#!/usr/bin/env python3
"""Plan, or explicitly prepare only three new sprint CPUs and retained volumes.

No worker starts, SSH operations, deletion, or credential propagation. DO create
request attaches volumes using `volumes`; `volume_ids` is response-only:
https://docs.digitalocean.com/reference/pydo/reference/droplets/create/
"""
import argparse
import fcntl
import hashlib
import ipaddress
import json
import math
import os
import pathlib
import shlex
import subprocess
import time
import urllib.request

TAG='best14-sprint-20261026';REGION='nyc1';CONTROLLER=612617424
TAGS=[TAG,'projectstoarama']
USER_DATA='#cloud-config\nssh_pwauth: false\ndisable_root: false\n'
RESOURCE_FILE='sprint-physical-resources.json'

class PreparationError(RuntimeError):pass


def atomic(path,state):
    temp=path.with_suffix('.pending');fd=os.open(temp,os.O_CREAT|os.O_TRUNC|os.O_WRONLY,0o600)
    with os.fdopen(fd,'w') as out:json.dump(state,out,separators=(',',':'));out.flush();os.fsync(out.fileno())
    os.replace(temp,path);fd=os.open(path.parent,os.O_RDONLY|os.O_DIRECTORY)
    try:os.fsync(fd)
    finally:os.close(fd)


def digest(value):return hashlib.sha256(json.dumps(value,sort_keys=True,separators=(',',':')).encode()).hexdigest()


def validate_policy(policy,review,commit):
    if policy.get('schema')!='best14-retained-sprint-v1' or policy.get('allow_start') is not True or policy.get('additional_budget_usd')!=150:raise PreparationError('150 USD sprint authority required')
    minima={'baseline_upper_usd':75,'compute_reserve_usd':15,'retained_volume_reserve_usd':24,'controller_reserve_usd':3,'r2_reserve_usd':5}
    values=[]
    for key,minimum in minima.items():
        value=policy.get(key)
        if type(value) not in (int,float) or not math.isfinite(value) or value<minimum:raise PreparationError('budget reserves incomplete')
        values.append(value)
    if sum(values)+5>150:raise PreparationError('projected budget exceeds authority')
    if review.get('review_passed') is not True or review.get('merged_default') is not True or review.get('commit')!=commit or len(commit)!=40:raise PreparationError('reviewed merged commit receipt required')
    return sum(values)+5


def plan(vpc_uuid):
    return [{'suffix':s,'volume':{'size_gigabytes':80,'name':TAG+'-'+s,'region':REGION,'filesystem_type':'ext4','tags':TAGS},'droplet':{'name':'stoarama-'+TAG+'-'+s,'region':REGION,'size':'c-32','image':'ubuntu-24-04-x64','ssh_keys':[58824129],'vpc_uuid':vpc_uuid,'backups':False,'ipv6':False,'monitoring':False,'tags':TAGS,'user_data':USER_DATA}} for s in 'abc']


def call(api,method,path,body=None):
    try:return api(method,path,body)
    except Exception:raise PreparationError('provider request unavailable; intent retained') from None


def list_resources(api,kind):
    values=[]
    for page in range(1,101):
        query='?per_page=200&page='+str(page)
        if kind=='volumes':query+='&region='+REGION
        response=call(api,'GET','/v2/'+kind+query)
        items=response.get(kind)
        if not isinstance(items,list):raise PreparationError('provider resource list invalid')
        values.extend(items)
        if not response.get('links',{}).get('pages',{}).get('next'):return values
    raise PreparationError('provider inventory bound exceeded')


def verify(kind,value,spec):
    if value.get('name')!=spec['name']:raise PreparationError('resource name differs')
    if not value.get('id') or value.get('region',{}).get('slug')!=REGION or not set(TAGS)<=set(value.get('tags',[])):raise PreparationError('resource ownership differs')
    if kind=='volumes':
        if value.get('size_gigabytes')!=80 or value.get('filesystem_type')!='ext4' or value.get('droplet_ids') not in ([],):raise PreparationError('new retained volume differs or already attached')
    else:
        if value.get('size_slug')!='c-32' or value.get('vpc_uuid')!=spec['vpc_uuid'] or value.get('volume_ids')!=spec['volumes'] or value.get('image',{}).get('slug')!='ubuntu-24-04-x64':raise PreparationError('new CPU specification differs')
    return value


def ensure_resource(api,state,key,kind,spec,save):
    entry=state['resources'].get(key);spec_hash=digest(spec)
    if entry is None:
        if any(v.get('name')==spec['name'] for v in list_resources(api,kind)):raise PreparationError('existing name without creation intent retained')
        entry={'name':spec['name'],'spec':spec,'spec_sha256':spec_hash,'intent_at':time.time(),'submission_uncertain':True}
        state['resources'][key]=entry;save()  # Before the sole creation attempt.
        try:response=call(api,'POST','/v2/'+kind,spec)
        except PreparationError:return None
        value=response.get('volume' if kind=='volumes' else 'droplet',{})
        # Persist the returned ID before any further read or interpretation.
        entry['receipt']=value;save()
    elif entry.get('spec_sha256')!=spec_hash or entry.get('spec')!=spec:raise PreparationError('durable creation intent differs')
    if not entry.get('verified'):
        matches=[v for v in list_resources(api,kind) if v.get('name')==spec['name']]
        if len(matches)>1:raise PreparationError('ambiguous creation reconciliation')
        if not matches:return None  # Lost POST outcome never causes duplicate creation.
        value=verify(kind,matches[0],spec)
        if entry.get('receipt',{}).get('id') not in (None,value['id']):raise PreparationError('creation receipt identity differs')
        entry.update(receipt=value,verified=True,submission_uncertain=False);save()
    return entry['receipt']


def network_receipt(value,now):
    try:ip=ipaddress.ip_address(value['ip'])
    except (KeyError,ValueError):raise PreparationError('verified Jelly IPv4 receipt required') from None
    if ip.version!=4 or not ip.is_global or value.get('source')!='jelly-public-ip' or not isinstance(value.get('verified_at'),(int,float)) or not 0<=now-value['verified_at']<=300:raise PreparationError('fresh verified Jelly public IPv4 required')
    return str(ip)+'/32'


def firewall_spec(ids,jelly,controller):
    return {'name':TAG+'-workers','droplet_ids':sorted(ids),'inbound_rules':[{'protocol':'tcp','ports':'22','sources':{'addresses':[jelly]}},{'protocol':'tcp','ports':'8080-8101','sources':{'addresses':[controller+'/32']}}], 'outbound_rules':[{'protocol':'tcp','ports':'80','destinations':{'addresses':['0.0.0.0/0']}},{'protocol':'tcp','ports':'443','destinations':{'addresses':['0.0.0.0/0']}},{'protocol':'udp','ports':'53','destinations':{'addresses':['0.0.0.0/0']}},{'protocol':'tcp','ports':'53','destinations':{'addresses':['0.0.0.0/0']}}]}


def normalize_rules(rules):
    result=[]
    for rule in rules:
        side='sources' if 'sources' in rule else 'destinations'
        peers=rule.get(side,{})
        if any(value for key,value in peers.items() if key!='addresses'):raise PreparationError('unexpected firewall peers')
        result.append((rule.get('protocol'),rule.get('ports'),tuple(sorted(peers.get('addresses',[])))))
    return sorted(result)


def prepare(root,policy,review,commit,jelly_receipt,api,now=None):
    now=time.time() if now is None else now;projected=validate_policy(policy,review,commit)
    jelly=network_receipt(jelly_receipt,now)
    coordinator=call(api,'GET','/v2/droplets/'+str(CONTROLLER))['droplet']
    if coordinator.get('id')!=CONTROLLER or coordinator.get('name')!='stoarama-'+TAG or coordinator.get('region',{}).get('slug')!=REGION or TAG not in coordinator.get('tags',[]):raise PreparationError('coordinator scope differs')
    private=[n['ip_address'] for n in coordinator.get('networks',{}).get('v4',[]) if n.get('type')=='private']
    if len(private)!=1 or ipaddress.ip_address(private[0]) not in ipaddress.ip_network('10.0.0.0/8'):raise PreparationError('coordinator private IPv4 unavailable')
    vpc=coordinator.get('vpc_uuid')
    if not vpc:raise PreparationError('coordinator VPC unavailable')
    root=pathlib.Path(root);root.mkdir(mode=0o700,parents=True,exist_ok=True);path=root/RESOURCE_FILE
    fd=os.open(root/'sprint-physical-resources.lock',os.O_RDWR|os.O_CREAT,0o600)
    with os.fdopen(fd,'w') as lock:
        fcntl.flock(lock,fcntl.LOCK_EX)
        state=json.loads(path.read_text()) if path.exists() else {'schema':'best14-physical-resources-v1','reviewed_commit':commit,'projected_upper_usd':projected,'resources':{}}
        if state.get('reviewed_commit')!=commit:raise PreparationError('physical resource reviewed commit differs')
        def save():atomic(path,state)
        save();hosts=[]
        for item in plan(vpc):
            suffix=item['suffix'];volume=ensure_resource(api,state,'volume-'+suffix,'volumes',item['volume'],save)
            if volume is None:return {'status':'pending','reason':'volume_creation_unknown','suffix':suffix}
            actual=call(api,'GET','/v2/volumes/'+str(volume['id']))['volume']
            verify('volumes',dict(actual,droplet_ids=[]),item['volume'])
            prior_cpu=state['resources'].get('cpu-'+suffix,{}).get('receipt',{}).get('id')
            if prior_cpu is None and 'cpu-'+suffix in state['resources']:
                cpu_matches=[v for v in list_resources(api,'droplets') if v.get('name')==item['droplet']['name']]
                if len(cpu_matches)>1:raise PreparationError('ambiguous CPU reconciliation')
                if cpu_matches:
                    verify('droplets',cpu_matches[0],dict(item['droplet'],volumes=[volume['id']]))
                    prior_cpu=cpu_matches[0]['id']
            if actual.get('droplet_ids') not in ([],[prior_cpu] if prior_cpu else []):raise PreparationError('retained volume unexpectedly attached')
            # On retries the volume may now be attached to its intended CPU.
            # ensure_resource verifies only before its first verified receipt.
            spec=dict(item['droplet'],volumes=[volume['id']])
            cpu=ensure_resource(api,state,'cpu-'+suffix,'droplets',spec,save)
            if cpu is None:return {'status':'pending','reason':'cpu_creation_unknown','suffix':suffix}
            hosts.append(cpu)
        # No daemon is installed by userdata; worker ports remain closed until
        # parent bootstrap verifies this firewall's active application.
        fresh=[call(api,'GET','/v2/droplets/'+str(h['id']))['droplet'] for h in hosts]
        for item,h in zip(plan(vpc),fresh):
            spec=dict(item['droplet'],volumes=[state['resources']['volume-'+item['suffix']]['receipt']['id']]);verify('droplets',h,spec)
            attached=call(api,'GET','/v2/volumes/'+spec['volumes'][0])['volume']
            verify('volumes',dict(attached,droplet_ids=[]),item['volume'])
            if attached.get('droplet_ids')!=[h['id']]:raise PreparationError('retained volume attachment differs')
            if h.get('status')!='active' or not any(n.get('type')=='public' for n in h.get('networks',{}).get('v4',[])):return {'status':'pending','reason':'cpu_networks'}
        for item,h in zip(plan(vpc),fresh):state['resources']['cpu-'+item['suffix']]['receipt']=h
        save()
        spec=firewall_spec([h['id'] for h in fresh],jelly,private[0]);entry=state.get('firewall')
        if entry is None:
            existing=call(api,'GET','/v2/firewalls?per_page=200').get('firewalls',[])
            if any(f.get('name')==spec['name'] for f in existing):raise PreparationError('firewall name exists without intent')
            state['firewall']={'spec':spec,'spec_sha256':digest(spec),'intent_at':now};save()
            try:response=call(api,'POST','/v2/firewalls',spec)
            except PreparationError:return {'status':'pending','reason':'firewall_creation_unknown'}
            state['firewall']['receipt']=response.get('firewall',{});save()
        elif entry.get('spec_sha256')!=digest(spec):raise PreparationError('firewall intent differs')
        matching=[f for f in call(api,'GET','/v2/firewalls?per_page=200').get('firewalls',[]) if f.get('name')==spec['name']]
        if len(matching)>1:raise PreparationError('ambiguous firewall reconciliation')
        if not matching:return {'status':'pending','reason':'firewall_creation_unknown'}
        firewall=matching[0]
        if state['firewall'].get('receipt',{}).get('id') not in (None,firewall.get('id')):raise PreparationError('firewall receipt identity differs')
        if firewall.get('name')!=spec['name']:raise PreparationError('firewall name differs')
        for key in ('inbound_rules','outbound_rules'):
            if normalize_rules(firewall.get(key,[]))!=normalize_rules(spec[key]):raise PreparationError('firewall rules differ')
        if sorted(firewall.get('droplet_ids',[]))!=spec['droplet_ids'] or firewall.get('tags',[]) or firewall.get('status')!='succeeded':return {'status':'pending','reason':'firewall_application'}
        state['firewall']['receipt']=firewall;state['prepared']=True;save()
        return {'status':'prepared','hosts':[h['id'] for h in fresh],'retained_volumes':[state['resources']['volume-'+s]['receipt']['id'] for s in 'abc'],'firewall_id':firewall['id'],'worker_started':False}


def provider(method,path,body=None):
    if method not in ('GET','POST') or not path.startswith('/v2/'):raise PreparationError('provider operation outside preparation scope')
    request=urllib.request.Request('https://api.digitalocean.com'+path,data=json.dumps(body).encode() if body is not None else None,method=method,headers={'Authorization':'Bearer '+os.environ['DIGITALOCEAN_TOKEN'],'Content-Type':'application/json'})
    with urllib.request.urlopen(request,timeout=45) as response:return json.load(response)


def main():
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--apply',action='store_true');parser.add_argument('--root',default='/var/lib/stoarama-best14');parser.add_argument('--policy');parser.add_argument('--review-receipt');parser.add_argument('--jelly-ip-receipt');args=parser.parse_args()
    if not args.apply:
        print(json.dumps({'mode':'dry-run','resources':plan('COORDINATOR_VPC_UUID'),'projected_upper_usd':127,'requires':'allow_start +150 USD policy, reviewed merged commit, fresh Jelly public IP receipt','worker_started':False},indent=2));return
    if not all((args.policy,args.review_receipt,args.jelly_ip_receipt)):raise PreparationError('explicit policy and review/network receipts required')
    policy=json.loads(pathlib.Path(args.policy).read_text());review=json.loads(pathlib.Path(args.review_receipt).read_text());jelly=json.loads(pathlib.Path(args.jelly_ip_receipt).read_text())
    commit=subprocess.check_output(['git','rev-parse','HEAD'],cwd=pathlib.Path(__file__).resolve().parents[2],text=True).strip()
    validate_policy(policy,review,commit);network_receipt(jelly,time.time())
    # Existing controller-only scoped credential consumer. Never serialize env.
    for line in pathlib.Path('/etc/stoarama-best14/controller.env').read_text().splitlines():
        if not line.strip() or line.startswith('#'):continue
        key,value=line.removeprefix('export ').split('=',1);parts=shlex.split(value)
        if len(parts)!=1:raise PreparationError('controller env syntax invalid')
        os.environ[key]=parts[0]
    print(json.dumps(prepare(args.root,policy,review,commit,jelly,provider)))

if __name__=='__main__':
    try:main()
    except Exception:raise SystemExit('sprint preparation blocked; private intent retained; inspect policy, receipts, and provider status') from None
