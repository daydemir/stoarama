#!/usr/bin/env python3
"""Build the pinned metadata-only NAS audit index in memory; no media is opened.

Uses existing scoped R2 environment credentials; never transfers auth to CI.
Every derived object is conditional and receives exact full readback verification.
The root is published only after hashing the complete immutable source audit.

Schema v2 (keep28 policy) indexes every reviewed folder node owned by exactly
one stream, not only the old RETENTION_DELETE_CLEARED folders: the per-file
keep28 window check, not the folder report action, now decides KEEP. Output is
deterministic (sorted JSON, gzip mtime=0), so the default compute-only run prints
the root SHA without any R2 write; pass --publish to upload the immutable
shards/root after that SHA is pinned in server_nas_cleanup_index.go.
"""

def main():
    import os,json,subprocess,base64,hashlib,sys
    publish='--publish' in sys.argv[1:]
    assert set(sys.argv[1:])<={'--publish'},'usage: build_frozen_index.py [--publish]'
    env=os.environ.copy()
    env['AWS_ACCESS_KEY_ID']=env['R2_ACCESS_KEY_ID']
    env['AWS_SECRET_ACCESS_KEY']=env['R2_SECRET_ACCESS_KEY']
    env['AWS_DEFAULT_REGION']='auto'
    env['AWS_MAX_ATTEMPTS']='2'
    endpoint='https://'+env['R2_ACCOUNT_ID']+'.r2.cloudflarestorage.com'
    bucket=env['R2_BUCKET']
    def aws(args,payload=None,allow_absent=False):
     command=['aws']+args+['--endpoint-url',endpoint,'--cli-connect-timeout','10','--cli-read-timeout','45']
     if payload is not None:
      at=command.index('--cli-input-json')+1
      assert command[at]=='file:///dev/stdin'
      command[at]=payload.decode('utf-8');payload=None
     footprint=sum(len(x.encode())+1 for x in command)+sum(len(k.encode())+len(v.encode())+2 for k,v in env.items())
     assert footprint < os.sysconf('SC_ARG_MAX')*.9,'Release exceeds safe argument budget'
     p=subprocess.run(command,env=env,input=payload,stdout=subprocess.PIPE,stderr=subprocess.PIPE,timeout=100)
     if p.returncode:
      if allow_absent and any(x in p.stderr for x in (b'(404)',b'(NoSuchKey)',b'(NotFound)')):return None
      raise RuntimeError('R2 release operation failed; inspect exact release key before retry')
     return p.stdout
    def small(key,maximum):
     h=aws(['s3api','head-object','--bucket',bucket,'--key',key],allow_absent=True)
     if h is None:return None,None
     h=json.loads(h);assert 0<int(h['ContentLength'])<=maximum
     b=aws(['s3','cp','s3://'+bucket+'/'+key,'-','--no-progress','--only-show-errors'])
     assert len(b)==int(h['ContentLength'])
     return b,h
    # The existing R2 consumer uses curl's native SigV4 and environment expansion:
    # credential contents never appear in argv, stdout, files, or CI.
    def metadata_request(method,key,body=None):
     url=endpoint+'/'+bucket+'/'+key
     args=['curl','--silent','--show-error','--connect-timeout','10','--max-time','45','--aws-sigv4','aws:amz:auto:s3','--variable','%R2_ACCESS_KEY_ID','--variable','%R2_SECRET_ACCESS_KEY','--expand-user','{{R2_ACCESS_KEY_ID}}:{{R2_SECRET_ACCESS_KEY}}','--write-out',chr(10)+'%{http_code}','--request',method]
     if body is not None:args+=['--header','If-None-Match: *','--header','Content-Type: application/gzip','--header','Content-MD5: '+base64.b64encode(hashlib.md5(body).digest()).decode(),'--data-binary','@-']
     args.append(url)
     p=subprocess.run(args,input=body,capture_output=True,env=env,timeout=50)
     if p.returncode:raise RuntimeError('R2 metadata request failed without changing deletion state')
     data,code=p.stdout.rsplit(bytes([10]),1)
     return data,int(code)
    def immutable(key,body,content_type):
     if not publish:return False
     _,code=metadata_request('PUT',key,body)
     assert code in (200,204,412),'R2 metadata conditional write failed: '+str(code)
     got,status=metadata_request('GET',key)
     assert status==200 and got==body,'R2 metadata full readback differs'
     return code!=412
    
    import gzip,io,posixpath,concurrent.futures,collections,time
    REPORT='64eee0bc589c9be4dcab5b5ea7668270291a07ce4fd0db98dcd0a484e19d84c1'
    AUDIT='480d85a49288d79e201b14405dc72d0f6fc979a0fd06f1b7a95663053583865b'
    reportkey='staging/best14-20261021/nas-retention-20261025/current-folder-preparation-20261031/'+REPORT+'/retention-only-folder-report-109.json.gz'
    rb,_=small(reportkey,500000);assert hashlib.sha256(rb).hexdigest()==REPORT
    report=json.loads(gzip.decompress(rb))
    nodes={n['path']:n for n in report['complete_node_actions']}
    parents={n['path']:{'owner_stream_id':n['owner_stream_ids'][0],'shards':[],'entries':0} for n in report['complete_node_actions'] if len(n['owner_stream_ids'])==1}
    assert len(nodes)==3304 and len(parents)==3288
    prefix='staging/best14-20261021/nas-retention-20261025/frozen-index-20261102/'
    auditkey='nas-space-audits/13/6cd48108-ca05-4df7-988b-0bd7e4d320fe/'+AUDIT+'.jsonl.gz'
    head=json.loads(aws(['s3api','head-object','--bucket',bucket,'--key',auditkey]));assert head['ContentLength']==88667812
    command=['aws','s3','cp','s3://'+bucket+'/'+auditkey,'-','--no-progress','--only-show-errors','--endpoint-url',endpoint,'--cli-connect-timeout','10','--cli-read-timeout','90']
    cp=subprocess.Popen(command,env=env,stdout=subprocess.PIPE,stderr=subprocess.PIPE)
    digest=hashlib.sha256();compressed_count=[0]
    class Hashed:
     def read(self,n=-1):
      b=cp.stdout.read(n);digest.update(b);compressed_count[0]+=len(b);return b
    reader=Hashed();futures=[];pool=concurrent.futures.ThreadPoolExecutor(max_workers=4)
    active_parent=None;active_entries=[];active_bytes=0
    totals=collections.Counter();folder_totals={};summary_seen=False
    def put(shardkey,blob):
     immutable(shardkey,blob,'application/gzip')
     return True
    def flush():
     nonlocal active_entries,active_bytes
     if not active_entries:return
     blob=gzip.compress(json.dumps({'parent':active_parent,'entries':active_entries},sort_keys=True,separators=(',',':')).encode(),mtime=0)
     assert len(blob)<100000
     sha=hashlib.sha256(blob).hexdigest();key=prefix+sha+'/files.json.gz'
     ref={'key':key,'sha256':sha,'bytes':len(blob),'entries':len(active_entries)}
     parents[active_parent]['shards'].append(ref);parents[active_parent]['entries']+=len(active_entries)
     futures.append(pool.submit(put,key,blob));active_entries=[];active_bytes=0
     # Bound in-flight compressed metadata, AWS processes and retained errors.
     if len(futures)>=16:
      futures.pop(0).result()
    try:
     with gzip.GzipFile(fileobj=reader,mode='rb') as archive:
      for line in archive:
       assert len(line)<=2*1024*1024,'oversized audit entry'
       e=json.loads(line)
       if e.get('type')=='summary':
        assert not summary_seen and e['audit_id']=='6cd48108-ca05-4df7-988b-0bd7e4d320fe'
        summary_seen=True;continue
       totals['entries']+=1;kind=e.get('kind','');totals[kind+'_entries']+=1
       path=e.get('path');size=e.get('size') or 0
       assert isinstance(size,int) and size>=0
       if isinstance(path,str) and kind=='file':
        parent=posixpath.dirname(path)
        n=nodes.get(parent,{})
        f=folder_totals.setdefault(parent,{'path':parent,'owner_stream_ids':n.get('owner_stream_ids',[]),'reviewed_action':n.get('direct_files_action','KEEP_UNASSIGNED'),'file_count':0,'logical_bytes':0,'raw_bytes':0,'stitched_bytes':0,'metadata_other_bytes':0})
        f['file_count']+=1;f['logical_bytes']+=size;totals['regular_file_bytes']+=size
        label='stitched_bytes' if path.startswith('joined/') and path.lower().endswith('.mp4') else 'raw_bytes' if path.lower().endswith('.mp4') else 'metadata_other_bytes'
        f[label]+=size
       parent=posixpath.dirname(path) if isinstance(path,str) else None
       if parent in parents:
        if active_parent!=parent or active_bytes>=60000:
         flush();active_parent=parent
        fingerprint=[e.get(k) or 0 for k in ('size','mtime_ns','ctime_ns','inode','device')]
        assert all(isinstance(v,int) for v in fingerprint)
        entry={'path':path,'path_bytes_b64':e.get('path_bytes_b64') or '', 'kind':kind,'error':e.get('error') or '', 'links':e.get('links') or 0,'frozen':fingerprint}
        active_entries.append(entry);active_bytes+=len(json.dumps(entry,separators=(',',':')).encode())
       if totals['entries']%250000==0:print(json.dumps({'phase':'indexing_frozen_metadata','entries':totals['entries'],'shard_uploads_pending':len(futures),'no_media_opened':True}),flush=True)
     flush()
     while reader.read(1024*1024):pass
     assert cp.wait(timeout=120)==0,'source audit stream failed'
     assert digest.hexdigest()==AUDIT and compressed_count[0]==88667812 and summary_seen
     assert totals['entries']==2943864,'full inventory entry accounting differs'
     for f in futures:f.result()
     pool.shutdown(wait=True)
     index={'schema':'nas-cleanup-frozen-index-v2','parent_scope':'all_single_owner_report_nodes','audit_id':'6cd48108-ca05-4df7-988b-0bd7e4d320fe','source_audit_sha256':AUDIT,'source_audit_key':auditkey,'source_audit_bytes':compressed_count[0],'report_sha256':REPORT,'entry_totals':dict(totals),'parents':parents,'folder_totals':sorted(folder_totals.values(),key=lambda f:f['path']),'execution_enabled':False}
     encoded=json.dumps(index,sort_keys=True,separators=(',',':')).encode()
     blob=gzip.compress(encoded,mtime=0);sha=hashlib.sha256(blob).hexdigest();key=prefix+sha+'/index.json.gz'
     immutable(key,blob,'application/gzip')
     if not publish:
      # Local, metadata-only copy for review and plan preparation.
      with open('frozen-index-'+sha+'.json.gz','wb') as local:local.write(blob)
     print(json.dumps({'phase':'INDEX_COMPLETE','index_key':key,'index_sha256':sha,'index_bytes':len(blob),'index_uncompressed_bytes':len(encoded),'parents':len(parents),'shards':sum(len(x['shards']) for x in parents.values()),'totals':dict(totals),'all_source_bytes_sha_verified':True,'published':publish,'all_shards_exact_readback':publish,'deletion_enabled':False}),flush=True)
    finally:
     if cp.poll() is None:cp.terminate()
     pool.shutdown(wait=True)
    

if __name__ == "__main__":
    main()
