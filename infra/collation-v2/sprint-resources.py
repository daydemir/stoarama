#!/usr/bin/env python3
"""Finite sprint CPU retirement; retained volumes are never deleted.

Caller supplies api(method, /v2/path, payload=None), raising an exception with
code/status_code on HTTP errors. No credentials, creation, or networking here.
Proofs must come from all worker drains after logs, jobs, TMPDIR and metadata
were placed on the retained volume; root-disk fallback is forbidden.

Official DO action routes:
https://docs.digitalocean.com/products/volumes/reference/api/block-storage-actions/
https://docs.digitalocean.com/reference/api/reference/droplet-actions/
"""
import copy
import datetime
import fcntl
import json
import os
import pathlib
import time
import uuid

TAG='best14-sprint-20261026'
REGION='nyc1'
PROOF_MAX_AGE=300

class RetirementError(RuntimeError):
    """Sanitized ownership or durability gate failure."""

class ProviderUnavailable(RetirementError):pass


def atomic_record(path,state):
    """Return only after file and parent directory are flushed."""
    path=pathlib.Path(path)
    temporary=path.with_suffix('.pending')
    fd=os.open(temporary,os.O_WRONLY|os.O_CREAT|os.O_TRUNC,0o600)
    with os.fdopen(fd,'w') as out:
        json.dump(state,out,separators=(',',':'));out.flush();os.fsync(out.fileno())
    os.replace(temporary,path)
    fd=os.open(path.parent,os.O_RDONLY|os.O_DIRECTORY)
    try:os.fsync(fd)
    finally:os.close(fd)


def _call(api,method,path,payload=None,missing=False):
    try:return api(method,path,payload)
    except Exception as error:
        if missing and getattr(error,'code',getattr(error,'status_code',None))==404:return None
        raise ProviderUnavailable('provider request unavailable') from None


def _intent(host):
    identifier=host.get('id')
    if type(identifier) is not int or identifier<=0:raise RetirementError('droplet identity required')
    name=host.get('name','');volume_name=host.get('volume_name','')
    if not name.startswith('stoarama-'+TAG+'-') or not volume_name.startswith(TAG+'-'):raise RetirementError('sprint resource name required')
    try:volume_id=str(uuid.UUID(host['volume_id']))
    except (KeyError,ValueError,TypeError,AttributeError):raise RetirementError('volume identity required') from None
    return {'id':identifier,'name':name,'volume_id':volume_id,'volume_name':volume_name,'tag':TAG,'region':REGION,'size_gigabytes':80}


def _proof(host,now):
    if host.get('drain_confirmed') is not True or host.get('checkpoint_confirmed') is not True:raise RetirementError('drain and checkpoint confirmation required')
    at=host.get('proof_at')
    if not isinstance(at,(int,float)) or not 0<=now-at<=PROOF_MAX_AGE:raise RetirementError('fresh retirement proof required')
    drain=host.get('drain_receipt',{});checkpoint=host.get('checkpoint_receipt',{})
    if not (drain.get('active_jobs')==0 and drain.get('pending_exports')==0 and drain.get('synced') is True and drain.get('all_mutable_on_volume') is True):raise RetirementError('flushed retained-volume drain required')
    if checkpoint.get('full_readback_verified') is not True or checkpoint.get('durable') is not True:raise RetirementError('durable readback checkpoint required')
    return {'at':at,'drain':{k:drain[k] for k in ('active_jobs','pending_exports','synced','all_mutable_on_volume')},'checkpoint':{k:checkpoint[k] for k in ('full_readback_verified','durable')}}


def _volume(value,intent):
    if not value:raise RetirementError('retained volume missing')
    v=value.get('volume',{})
    if (v.get('id')!=intent['volume_id'] or v.get('name')!=intent['volume_name'] or v.get('size_gigabytes')!=80 or v.get('region',{}).get('slug')!=REGION or TAG not in v.get('tags',[])):raise RetirementError('retained volume ownership differs')
    attached=v.get('droplet_ids')
    if not isinstance(attached,list) or attached not in ([],[intent['id']]):raise RetirementError('unexpected volume attachment')
    return attached


def _droplet(value,intent):
    d=value.get('droplet',{})
    if d.get('id')!=intent['id'] or d.get('name')!=intent['name'] or TAG not in d.get('tags',[]) or d.get('region',{}).get('slug')!=REGION:raise RetirementError('droplet ownership differs')
    volumes=d.get('volume_ids')
    if not isinstance(volumes,list) or any(v!=intent['volume_id'] for v in volumes):raise RetirementError('unexpected droplet volume')
    return d


def _action(value,kind):
    action=value.get('action',{})
    if type(action.get('id')) is not int or action['id']<=0 or action.get('type') not in ({'shutdown'} if kind=='shutdown' else {'detach','detach_volume'}):raise RetirementError('provider action identity differs')
    if action.get('status') not in ('in-progress','completed','errored'):raise RetirementError('provider action status unknown')
    if action['status']=='errored':raise RetirementError('provider action failed; retained for review')
    return action


def _started(action):
    try:return datetime.datetime.fromisoformat(action['started_at'].replace('Z','+00:00')).timestamp()
    except (KeyError,ValueError,TypeError):return -1


def _action_scope(action,kind,intent):
    # DO volume actions may use resource_type=backend and a null resource_id.
    # The scoped host/volume action endpoint remains the primary binding.
    rid=action.get('resource_id');rtype=action.get('resource_type')
    expected=intent['volume_id'] if rtype=='volume' else intent['id']
    if rid is not None and str(rid)!=str(expected):raise RetirementError('action resource identity differs')
    region=action.get('region_slug') or action.get('region',{}).get('slug')
    if region is not None and region!=REGION:raise RetirementError('action region differs')


def _step_action(state,kind,base,payload,save,api,now,retry_allowed=False):
    """Reconcile first; retry an unaccepted action only in a fresh safe state."""
    key=kind+'_action';entry=state.get(key)
    def submit():
        entry['attempts']=entry.get('attempts',0)+1
        entry['last_attempt_at']=now;save()
        try:response=_call(api,'POST',base,payload)
        except ProviderUnavailable:return False
        action=_action(response,kind);_action_scope(action,kind,state['intent'])
        entry['id']=action['id'];save();return False
    if entry is None:
        entry={'intent_at':int(now),'attempts':0};state[key]=entry
        return submit()
    if 'id' not in entry:
        result=_call(api,'GET',base+'?per_page=200')
        expected={'shutdown'} if kind=='shutdown' else {'detach','detach_volume'}
        candidates=[a for a in result.get('actions',[]) if a.get('type') in expected and _started(a)>=entry['intent_at']]
        for action in candidates:_action_scope(action,kind,state['intent'])
        if not candidates:
            if not retry_allowed or now-entry.get('last_attempt_at',entry['intent_at'])<30:return False
            return submit()
        # A late accepted request and its safe resend can produce two records.
        # Both are scoped to the same exact current resource: prefer completion,
        # otherwise the latest in-progress action; never force power-off.
        candidates.sort(key=lambda a:(a.get('status')=='completed',a.get('status')=='in-progress',_started(a),a.get('id',0)),reverse=True)
        action=_action({'action':candidates[0]},kind);entry['id']=action['id'];save()
    action=_action(_call(api,'GET',base+'/'+str(entry['id'])),kind)
    _action_scope(action,kind,state['intent'])
    if action['id']!=entry['id']:raise RetirementError('provider action receipt differs')
    if action['status']!='completed':return False
    entry['completed']=True;save();return True


def retire_host(host,root,api,record=None,now=None):
    """Advance once; returns pending/retired without sleeping or forced poweroff.

    root is a private durable controller receipt directory, never a worker root.
    Optional record(path,state) replaces the fsyncing atomic recorder and MUST
    return only after durable persistence. Concurrent calls serialize by host.
    Provider failures reveal no response bodies, URLs, or credentials.
    """
    now=time.time() if now is None else now
    intent=_intent(host);root=pathlib.Path(root)
    root.mkdir(mode=0o700,parents=True,exist_ok=True)
    path=root/('retire-'+str(intent['id'])+'.json')
    fd=os.open(path.with_suffix('.lock'),os.O_RDWR|os.O_CREAT,0o600)
    with os.fdopen(fd,'w') as lock:
        fcntl.flock(lock,fcntl.LOCK_EX)
        state=json.loads(path.read_text()) if path.exists() else None
        if state is None:
            proof=_proof(host,now)
            state={'version':1,'intent':intent,'proof':proof,'phase':'validated'}
        elif state.get('intent')!=intent:raise RetirementError('durable host intent differs')
        def save():
            (record or atomic_record)(path,copy.deepcopy(state))
        save()
        volume_path='/v2/volumes/'+intent['volume_id']
        droplet_path='/v2/droplets/'+str(intent['id'])
        attached=_volume(_call(api,'GET',volume_path),intent)
        response=_call(api,'GET',droplet_path,missing=True)
        if response is None:
            if attached or not state.get('delete_intent') or not state.get('detach_confirmed'):raise RetirementError('missing droplet without retirement evidence')
            state['phase']='retired';state['retired_at']=now;save()
            return {'status':'retired','droplet_id':intent['id'],'retained_volume_id':intent['volume_id']}
        droplet=_droplet(response,intent)
        if state.get('phase')=='retired':raise RetirementError('retired droplet unexpectedly exists')
        def pending(reason):return {'status':'pending','reason':reason,'droplet_id':intent['id'],'retained_volume_id':intent['volume_id']}
        if not state.get('shutdown_action'):
            # Proofs cannot authorize a shutdown beginning long after the drain.
            state['proof']=_proof(host,now);save()
            if attached!=[intent['id']] or droplet['volume_ids']!=[intent['volume_id']]:raise RetirementError('initial attachment evidence missing')
        if not _step_action(state,'shutdown',droplet_path+'/actions',{'type':'shutdown'},save,api,now,retry_allowed=droplet.get('status')=='active' and attached==[intent['id']] and droplet['volume_ids']==[intent['volume_id']]):return pending('shutdown_action')
        if droplet.get('status')!='off':return pending('graceful_shutdown')
        state['off_confirmed']=True;save()
        if not state.get('detach_action') and attached!=[intent['id']]:raise RetirementError('volume detached without intent')
        if not _step_action(state,'detach',volume_path+'/actions',{'type':'detach','droplet_id':intent['id'],'region':REGION},save,api,now,retry_allowed=droplet.get('status')=='off' and attached==[intent['id']] and droplet['volume_ids']==[intent['volume_id']]):return pending('detach_action')
        # Confirm both the action and current provider attachment state. Fresh
        # droplet ownership/off checks immediately precede ordinary CPU deletion.
        attached=_volume(_call(api,'GET',volume_path),intent)
        if attached:return pending('volume_still_attached')
        state['detach_confirmed']=True;save()
        droplet=_droplet(_call(api,'GET',droplet_path),intent)
        if droplet.get('status')!='off' or droplet['volume_ids']:return pending('droplet_not_detached_and_off')
        deletion=state.get('delete_intent')
        if deletion is None:
            deletion={'at':now,'attempts':0};state['delete_intent']=deletion
        elif now-deletion.get('last_attempt_at',deletion['at'])<30:
            return pending('deletion_confirmation')
        # Exact-ID ordinary DELETE is idempotent. A lost response may leave an
        # owned, powered-off CPU still billing: retry only after fresh ownership,
        # off-state and both empty-attachment checks above, with bounded cadence.
        deletion['attempts']=deletion.get('attempts',1)+1
        deletion['last_attempt_at']=now;save()
        try:_call(api,'DELETE',droplet_path)
        except ProviderUnavailable:return pending('delete_response_unknown')
        state['delete_requested']=True;save()
        return pending('deletion_confirmation')
