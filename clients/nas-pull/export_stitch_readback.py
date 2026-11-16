#!/usr/bin/env python3
"""Assemble preserved metadata and replay immutable byte snapshots offline.

No credentials, network, NAS/media access or activation. The output is evidence
components plus a server-validation REQUEST, never an accepted attestation.
Registry acceptance, actual server/provider reads and independent review remain
separate. An explicitly synthetic input remains synthetic after successful replay.
"""
import argparse
import ast
import base64
import hashlib
import json
import os
from pathlib import Path
import re
import stat
import sys
import uuid
import zlib


NATIVE_VERIFIER = ("81ff91e5accd93d32ff475024c10281177684b583e121a6c5d0c1f1938d49532",18817)
READBACK_VERIFIER = ('f3a22aebaf50eb3e2faa5c669a56a059fea3d973bf6cd13ad3edaf890be94c74', 9051)
LIMITS = {"proof-v1":1048576,"task-v2":1048576,"producer-v2":65536,
          "reader-v2":65536,"pair-v2":65536,"readback-v2":65536}


class ExportError(ValueError):
    pass


def strict_json(raw):
    def pairs(items):
        value={}
        for key,item in items:
            if key in value:raise ExportError("duplicate metadata field")
            value[key]=item
        return value
    def invalid(value):raise ExportError("nonfinite metadata")
    return json.loads(raw.decode("utf-8"),object_pairs_hook=pairs,parse_constant=invalid)


def canonical(value):return json.dumps(value,sort_keys=True,separators=(",",":"),allow_nan=False).encode()
def sha(raw):return hashlib.sha256(raw).hexdigest()


def digest(value):
    if type(value) is not str or re.fullmatch(r"[a-f0-9]{64}",value) is None:raise ExportError("exact digest required")
    return value


def identity(value):
    if type(value) is not str or str(uuid.UUID(value))!=value or uuid.UUID(value).int==0:raise ExportError("canonical nonzero identity required")
    return value


def relative(value):
    if (type(value) is not str or not value or len(value)>2048 or "\\" in value or "\x00" in value
            or any(part in ("",".","..") for part in value.split("/")) or Path(value).is_absolute()
            or ":" in value):raise ExportError("canonical relative component path required")
    return value.split("/")


def directories(path):
    """Pin every absolute ancestor; never follow a symlink or a URL."""
    path=Path(path).absolute()
    parts=relative(str(path).removeprefix("/"))
    descriptors=[os.open("/",os.O_RDONLY|os.O_DIRECTORY)]
    try:
        for part in parts:
            descriptors.append(os.open(part,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW,dir_fd=descriptors[-1]))
        yield_value=(path,descriptors[-1])
        return yield_value,descriptors
    except BaseException:
        for fd in reversed(descriptors):os.close(fd)
        raise


def verify_directories(path,descriptors):
    parts=relative(str(path).removeprefix("/"))
    for index,part in enumerate(parts):
        named=os.stat(part,dir_fd=descriptors[index],follow_symlinks=False)
        held=os.fstat(descriptors[index+1])
        if (named.st_dev,named.st_ino,named.st_mode)!=(held.st_dev,held.st_ino,held.st_mode):raise ExportError("absolute metadata ancestor changed")


def read_component(root,name,limit,expected=None):
    parts=relative(name)
    (root_path,fd),descriptors=directories(root)
    before_links=[];root_descriptors=list(descriptors)
    try:
        for part in parts[:-1]:
            parent=fd;fd=os.open(part,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW,dir_fd=parent)
            descriptors.append(fd);before_links.append((parent,part,os.fstat(fd)))
        opened=os.open(parts[-1],os.O_RDONLY|os.O_NOFOLLOW|os.O_NONBLOCK,dir_fd=fd);descriptors.append(opened)
        before=os.fstat(opened)
        if not stat.S_ISREG(before.st_mode) or before.st_nlink!=1 or not 0<before.st_size<=limit:raise ExportError("bounded regular single-link metadata required")
        chunks=[];size=0
        while size<=limit:
            block=os.read(opened,min(1048576,limit+1-size))
            if not block:break
            size+=len(block);chunks.append(block)
        raw=b"".join(chunks)
        def key(info):return (info.st_dev,info.st_ino,info.st_mode,info.st_nlink,info.st_size,info.st_mtime_ns,info.st_ctime_ns)
        if size>limit or key(before)!=key(os.fstat(opened)) or key(before)!=key(os.stat(parts[-1],dir_fd=fd,follow_symlinks=False)):raise ExportError("metadata changed while captured")
        for parent,part,prior in before_links:
            named=os.stat(part,dir_fd=parent,follow_symlinks=False)
            if (prior.st_dev,prior.st_ino,prior.st_mode)!=(named.st_dev,named.st_ino,named.st_mode):raise ExportError("metadata ancestor changed")
        verify_directories(root_path,root_descriptors)
        if expected is not None and (sha(raw),len(raw))!=expected:raise ExportError("component bytes differ from exact reference")
        return raw
    finally:
        for opened in reversed(descriptors):os.close(opened)


def reassemble(pages,kind,expected,key,reader_boot):
    digest(expected);plan,run,pair,readback=key
    digest(plan);identity(run);digest(pair);identity(readback);identity(reader_boot)
    chunks={};count=None;size=None
    prefix="V85r "+kind+" "+plan+" "+expected+" "
    for page in pages:
        if type(page) is not dict or page.get("connection_id")!=13 or type(page.get("connection_id")) is not int or type(page.get("lines")) is not list or len(page["lines"])>10000:raise ExportError("scoped bounded log response required")
        for row in page["lines"]:
            if type(row) is not dict or type(row.get("message")) is not str:raise ExportError("typed log row required")
            message=row["message"]
            if not message.startswith(prefix):continue
            fields=message.split(" ")
            if kind=="proof-v1":
                if len(fields)!=6:raise ExportError("native transport changed")
                ordinal,part=fields[4:];claimed=None
            else:
                if len(fields)!=10:raise ExportError("selected metadata transport header differs")
                identity(fields[4]);digest(fields[5]);identity(fields[6])
                # Identical task/profile bytes are legitimately resent by later
                # readbacks. Select the complete identity before census/conflict
                # checks; another valid context is not conflicting evidence.
                if fields[4:7]!=[run,pair,readback]:continue
                if re.fullmatch(r"[1-9][0-9]{0,6}",fields[7]) is None:raise ExportError("metadata size encoding differs")
                claimed=int(fields[7]);ordinal,part=fields[8:]
            if row.get("client_version")!="r2-copy-v3" or row.get("boot_id")!=reader_boot or row.get("level")!="INFO":continue
            match=re.fullmatch(r"(0|[1-9][0-9]{0,3})/([1-9][0-9]{0,3})",ordinal)
            if match is None or re.fullmatch(r"[a-f0-9]{2,1024}",part) is None or len(part)%2:raise ExportError("metadata ordinal/hex differs")
            index,total=int(match[1]),int(match[2])
            if not 0<=index<total<=((LIMITS[kind]+511)//512) or (count is not None and count!=total):raise ExportError("transport census differs")
            if claimed is not None and (not 0<claimed<=LIMITS[kind] or total!=(claimed+511)//512 or (size is not None and size!=claimed)):raise ExportError("transport size census differs")
            block=bytes.fromhex(part)
            if index<total-1 and len(block)!=512:raise ExportError("short nonterminal chunk")
            if index in chunks and chunks[index]!=block:raise ExportError("conflicting repeated chunk")
            count,size=total,claimed;chunks[index]=block
    if count is None or set(chunks)!=set(range(count)):raise ExportError("incomplete selected evidence")
    body=b"".join(chunks[n] for n in range(count))
    if not 0<len(body)<=LIMITS[kind] or sha(body)!=expected or (size is not None and size!=len(body)):raise ExportError("reassembled evidence bytes differ")
    return body


def load_verifier(raw,pin):
    if (sha(raw),len(raw))!=pin:raise ExportError("reviewed offline verifier pin differs")
    namespace={"__name__":"checksum_pinned_offline_export"}
    exec(compile(raw,"<immutable reviewed verifier>","exec"),namespace)
    return namespace


def artifact_version(raw,expected):
    values=[ast.literal_eval(n.value) for n in ast.parse(raw).body if isinstance(n,ast.Assign)
        for target in n.targets if isinstance(target,ast.Name) and target.id=="CLIENT_VERSION"]
    if values!=[expected] or expected not in ("v85r-nas-proof-v2","r2-copy-v3"):raise ExportError("artifact literal differs from profile; unstamped source is not a release")


def assemble(pages,key,readback_sha,task_sha,reader_boot,producer_client_sha,reader_client_sha,component):
    """component is a bounded file-reference reader, not an acceptance table."""
    parts={"readback.json":reassemble(pages,"readback-v2",readback_sha,key,reader_boot)}
    readback=strict_json(parts["readback.json"])
    expected={"native_plan_sha256":key[0],"native_run_id":key[1],"profile_pair_sha256":key[2],"id":key[3]}
    if any(readback.get(name)!=value for name,value in expected.items()):raise ExportError("selected readback identity differs")
    if readback.get("reader_execution",{}).get("client_boot_id")!=reader_boot:raise ExportError("selected readback boot differs")
    for name,kind,expected_sha in (("native-bundle.json.gz","proof-v1",readback["native_bundle_sha256"]),
            ("native-task.json","task-v2",task_sha),("producer-profile.json","producer-v2",readback["producer_profile_sha256"]),
            ("reader-profile.json","reader-v2",readback["reader_profile_sha256"]),("profile-pair.json","pair-v2",key[2])):
        parts[name]=reassemble(pages,kind,expected_sha,key,reader_boot)
    producer,reader,pair=(strict_json(parts[name]) for name in ("producer-profile.json","reader-profile.json","profile-pair.json"))
    references=[producer["client_artifact"],reader["client_artifact"],producer["package_manifest"],reader["package_manifest"]]
    references.extend(producer["contracts"].values());references.extend(reader["contracts"].values())
    references.extend(pair[name] for name in ("producer_profile","reader_profile","native_schema","native_verifier","readback_schema","readback_validator","package_manifest"))
    for ref in references:
        relative(ref["file"]);digest(ref["sha256"])
        if type(ref["size_bytes"]) is not int or not 0<ref["size_bytes"]<=8*1024*1024:raise ExportError("bounded exact component reference required")
    def capture(ref):
        name=ref["file"]
        candidate=next((parts[label] for label in ("producer-profile.json","reader-profile.json") if sha(parts[label])==ref["sha256"]),None)
        raw=candidate if candidate is not None else component(ref)
        if type(raw) is not bytes or (sha(raw),len(raw))!=(ref["sha256"],ref["size_bytes"]):raise ExportError("profile component snapshot differs")
        if name in parts and parts[name]!=raw:raise ExportError("conflicting component path")
        parts[name]=raw
        return raw
    for ref in references:capture(ref)
    producer_source=capture(producer["client_artifact"]);reader_source=capture(reader["client_artifact"])
    # Executable source pins come from the reviewed operation argv, never from
    # the uploaded narrative/profile alone. Verify before extracting any code.
    if sha(producer_source)!=digest(producer_client_sha) or sha(reader_source)!=digest(reader_client_sha):raise ExportError("reviewed executable source pin differs")
    artifact_version(producer_source,producer["client_version"]);artifact_version(reader_source,reader["client_version"])
    native=load_verifier(capture(pair["native_verifier"]),NATIVE_VERIFIER)
    replay=load_verifier(capture(pair["readback_validator"]),READBACK_VERIFIER)
    producer_sha,reader_sha=sha(producer_source),sha(reader_source)
    core=native["load_core_bytes"](producer_source,producer_sha)
    task=strict_json(parts["native-task.json"])
    receipt=native["verify"](core,task,parts["native-bundle.json.gz"],producer_sha)
    if receipt["run"]["id"]!=key[1]:raise ExportError("original native run differs")
    native_replay=native["replay_result"](receipt,task,producer_sha,sha(parts["native-bundle.json.gz"]))
    names=("reader_source_b64","readback_receipt_b64","native_bundle_b64","native_task_b64","producer_profile_b64","reader_profile_b64","profile_pair_b64")
    bodies=(reader_source,parts["readback.json"],parts["native-bundle.json.gz"],parts["native-task.json"],parts["producer-profile.json"],parts["reader-profile.json"],parts["profile-pair.json"])
    envelope=dict(schema=replay["INPUT_SCHEMA"],**{name:base64.b64encode(body).decode("ascii") for name,body in zip(names,bodies)})
    readback_replay=replay["replay_stream"](canonical(envelope),tuple(sha(body) for body in bodies))
    decoder=zlib.decompressobj(31);registration=decoder.decompress(parts["native-bundle.json.gz"],16*1024*1024+1)
    if len(registration)>16*1024*1024 or not decoder.eof or decoder.unused_data or decoder.unconsumed_tail:raise ExportError("full decoded registration bound differs")
    if any(name in parts for name in ("local-registration.json","server-validation-request.json","native-replay.json","readback-replay.json","components.json")):raise ExportError("profile reference collides with generated metadata")
    parts["local-registration.json"]=registration
    result=canonical(receipt["result"])
    if len(result)>65536:raise ExportError("native result exceeds server request bound")
    request=dict(schema="stoarama.nas-stitch-validation-request.v2",readback_b64=base64.b64encode(parts["readback.json"]).decode("ascii"),readback_receipt_sha256=readback_sha,
        native_result_b64=base64.b64encode(result).decode("ascii"),native_result_sha256=sha(result),
        native_task_b64=base64.b64encode(parts["native-task.json"]).decode("ascii"),native_task_sha256=sha(parts["native-task.json"]))
    parts["server-validation-request.json"]=canonical(request)
    parts["native-replay.json"]=canonical(native_replay);parts["readback-replay.json"]=canonical(readback_replay)
    return parts


def write_new(root,parts):
    root=Path(root).absolute();relative(root.name)
    (_,parent),descriptors=directories(root.parent)
    try:
        parent_descriptors=list(descriptors)
        os.mkdir(root.name,0o700,dir_fd=parent)
        fd=os.open(root.name,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW,dir_fd=parent);descriptors.append(fd)
        owned=os.fstat(fd)
        for name,body in sorted(parts.items()):
            verify_directories(root.parent,parent_descriptors)
            components=relative(name);target=fd;opened=[]
            try:
                for part in components[:-1]:
                    try:os.mkdir(part,0o700,dir_fd=target)
                    except FileExistsError:pass
                    target=os.open(part,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW,dir_fd=target);opened.append(target)
                output=os.open(components[-1],os.O_WRONLY|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW,0o600,dir_fd=target)
                with os.fdopen(output,"wb") as stream:stream.write(body);stream.flush();os.fsync(stream.fileno())
                os.fsync(target)
            finally:
                for child in reversed(opened):os.close(child)
            named=os.stat(root.name,dir_fd=parent,follow_symlinks=False)
            if (named.st_dev,named.st_ino,named.st_mode)!=(owned.st_dev,owned.st_ino,owned.st_mode):raise ExportError("owned export directory changed")
        os.fsync(fd);os.fsync(parent)
    finally:
        for opened in reversed(descriptors):os.close(opened)


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--logs",type=Path,action="append",required=True)
    parser.add_argument("--components-root",type=Path,required=True)
    for name in ("plan-sha","native-run","pair-sha","readback-id","readback-sha","native-task-sha","reader-boot","producer-client-sha","reader-client-sha"):parser.add_argument("--"+name,required=True)
    parser.add_argument("--out",type=Path,required=True)
    args=parser.parse_args()
    if not 1<=len(args.logs)<=64:raise ExportError("bounded original log-page set required")
    total=0;pages=[]
    for path in args.logs:
        raw=read_component(path.absolute().parent,path.name,1048576);total+=len(raw)
        if total>16*1024*1024:raise ExportError("aggregate metadata bound exceeded")
        pages.append(strict_json(raw))
    def component(ref):return read_component(args.components_root,ref["file"],ref["size_bytes"],(ref["sha256"],ref["size_bytes"]))
    parts=assemble(pages,(digest(args.plan_sha),identity(args.native_run),digest(args.pair_sha),identity(args.readback_id)),digest(args.readback_sha),digest(args.native_task_sha),identity(args.reader_boot),digest(args.producer_client_sha),digest(args.reader_client_sha),component)
    manifest=dict(schema="stoarama.nas-stitch-evidence-components.v1",authority="none",files=[dict(file=name,sha256=sha(body),size_bytes=len(body)) for name,body in sorted(parts.items())])
    parts["components.json"]=canonical(manifest)
    write_new(args.out,parts)
    print(json.dumps(dict(file="components.json",sha256=sha(parts["components.json"]),size_bytes=len(parts["components.json"])),sort_keys=True))


if __name__=="__main__":
    try:main()
    except Exception:
        print("offline evidence assembly refused",file=sys.stderr);raise SystemExit(1)
