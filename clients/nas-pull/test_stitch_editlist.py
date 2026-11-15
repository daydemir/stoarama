"""Content conservation and direct edit evidence; no fixture is NAS proof."""
import copy
import gzip
import hashlib
import io
import json
from pathlib import Path
import struct
import tempfile
import threading
import unittest

from test_stitch_proof import pull, verify, synthetic_receipt


def evidence(scale=1, duration=3, offset=2, version=0):
    def clock():
        return bytes([version, 0, 0, 0]) + bytes(16 if version else 8) + struct.pack('>IQ' if version else '>II', scale, duration)
    return dict(movie_header=clock().hex(), media_header=clock().hex(),
        track_header=(bytes([version, 0, 0, 0]) + bytes(16 if version else 8) + struct.pack('>I', 1)).hex(),
        handler=(bytes(8) + b'vide').hex(),
        edit_list=(bytes([version, 0, 0, 0]) + struct.pack('>I', 1) + struct.pack('>Qqhh' if version else '>Iihh', duration, offset, 1, 0)).hex())


def document(**kw):
    return dict(streams=[dict(id='0x1', codec_type='video', codec_name='h264', codec_tag_string='avc1',
        width=16, height=16, pix_fmt='yuv420p', time_base='1/1', extradata_hash='SHA256:'+'f'*64)], mp4_edit=evidence(**kw))


class EditListTests(unittest.TestCase):
    def setUp(self):
        # B-frame packet order: PTS is reordered, DTS is increasing.
        self.rows=[(0,-1,1,5,'K__','SHA256:'+'a'*64),(2,0,1,6,'___','SHA256:'+'b'*64),(1,1,1,7,'___','SHA256:'+'c'*64)]
        self.hidden=[(p+2,d+2,*rest) for p,d,*rest in self.rows]

    def check(self, doc=None, actual=None, hidden=None):
        return pull.stitch_editlist_records(doc if doc is not None else document(),
            iter(self.rows if actual is None else actual), iter(self.hidden if hidden is None else hidden))

    def test_only_one_translation_with_direct_v0_or_v1_evidence(self):
        for version in (0,1):
            self.assertEqual(self.check(document(version=version)),2)
        self.assertEqual(self.check(document(offset=0),hidden=self.rows),0)

    def test_every_non_timestamp_field_and_packet_count_order_is_conserved(self):
        for field,value in [(2,2),(3,9),(4,'KD_'),(5,'SHA256:'+'d'*64)]:
            altered=list(self.hidden);row=list(altered[1]);row[field]=value;altered[1]=tuple(row)
            with self.subTest(field=field),self.assertRaises(pull.StitchHold):self.check(hidden=altered)
        for altered in (self.hidden[:-1],self.hidden+self.hidden[-1:],list(reversed(self.hidden))):
            with self.assertRaises(pull.StitchHold):self.check(hidden=altered)
        with self.assertRaises(pull.StitchHold):self.check(actual=[],hidden=[])

    def test_nonconstant_or_unequal_pts_dts_shift_fails(self):
        for field in (0,1):
            changed=list(self.hidden);row=list(changed[-1]);row[field]+=1;changed[-1]=tuple(row)
            with self.assertRaises(pull.StitchHold):self.check(hidden=changed)
        with self.assertRaises(pull.StitchHold):self.check(document(offset=1))

    def test_media_duration_is_checked_at_the_edit_media_origin(self):
        # FFmpeg8 B-frames can declare mdhd longer than the edited segment.
        # The segment still covers every packet after translating its origin.
        doc=document()
        doc['mp4_edit']['media_header']=(bytes(12)+struct.pack('>II',1,4)).hex()
        self.assertEqual(self.check(doc),2)  # edit media interval[2,5]
        doc['mp4_edit']['media_header']=(bytes(12)+struct.pack('>II',1,6)).hex()
        with self.assertRaises(pull.StitchHold):self.check(doc)
        # A cut through actual packet content remains forbidden even when
        # the declared bound itself fits the shifted edit interval.
        with self.assertRaises(pull.StitchHold):self.check(document(duration=2))

    def test_edits_cannot_hide_cut_or_synthesize_timeline(self):
        for offset,count,duration,rate in [(-1,1,3,1),(2,0,3,1),(2,2,3,1),(2,1,0,1),(2,1,2,1),(2,1,3,0),(2,1,3,2)]:
            doc=document();doc['mp4_edit']['edit_list']=(bytes(4)+struct.pack('>IIihh',count,duration,offset,rate,0)).hex()
            with self.subTest(offset=offset,count=count,duration=duration,rate=rate),self.assertRaises(pull.StitchHold):self.check(doc)
        # Even if mdhd lies about the duration, all packet intervals must fit.
        with self.assertRaises(pull.StitchHold):self.check(document(duration=2))
        for key,value in [('track_header',(bytes(12)+struct.pack('>I',2)).hex()),('handler',(bytes(8)+b'soun').hex()),('media_header',(bytes(12)+struct.pack('>II',2,3)).hex())]:
            doc=document();doc['mp4_edit'][key]=value
            with self.assertRaises(pull.StitchHold):self.check(doc)

    def test_missing_malformed_oversized_and_unknown_evidence_fail_closed(self):
        for value in [None,{},dict(evidence(),extra='00'),dict(evidence(),edit_list='00'),dict(evidence(),edit_list='zz'*20),dict(evidence(),edit_list='00'*4097),dict(evidence(),edit_list='02'+evidence()['edit_list'][2:])]:
            doc=document();doc['mp4_edit']=value
            with self.assertRaises(pull.StitchHold):self.check(doc)

    def test_packet_parser_rejects_side_data_duplicate_fields_and_invalid_hash(self):
        good='pts=0|dts=0|duration=1|size=1|flags=K__|data_hash=SHA256:'+'a'*64
        for bad in [good+'|pts=0',good+'|side_data_type=x',good+'|junk',good.replace('size=1','pts=1'),good[:-1]+'z']:
            with self.assertRaises(pull.StitchHold):list(pull.stitch_packet_records([bad]))

    def test_direct_box_reader_versions_lengths_and_ambiguity(self):
        def box(kind,payload,wide=False):
            return (struct.pack('>I4sQ',1,kind,len(payload)+16) if wide else struct.pack('>I4s',len(payload)+8,kind))+payload
        for version in (0,1):
            e={k:bytes.fromhex(v) for k,v in evidence(version=version).items()}
            mdia=box(b'mdia',box(b'mdhd',e['media_header'])+box(b'hdlr',e['handler']))
            trak=box(b'trak',box(b'tkhd',e['track_header'])+mdia+box(b'edts',box(b'elst',e['edit_list'])))
            good=box(b'moov',box(b'mvhd',e['movie_header'])+trak,wide=bool(version))
            with tempfile.TemporaryDirectory() as temp:
                path=Path(temp)/'synthetic-boxes.mp4';path.write_bytes(good)
                self.assertEqual(pull.stitch_editlist_metadata(path,threading.Event()),evidence(version=version))
                for malformed in [good[:-1],b'\0\0\0\0mdat',good+box(b'moof',b''),box(b'moov',box(b'mvhd',e['movie_header'])+trak+trak),good+good]:
                    path.write_bytes(malformed)
                    with self.assertRaises(pull.StitchHold):pull.stitch_editlist_metadata(path,threading.Event())

    def test_real_census_packet_lists_replay_without_raw_byte_equality(self):
        fixtures=json.loads(gzip.decompress(Path(__file__).with_name('testdata').joinpath('editlist-census-packets.json.gz').read_bytes()))
        for fixture in fixtures['clips']:
            with self.subTest(clip_id=fixture['clip_id']):
                self.assertEqual(pull.stitch_editlist_records(fixture['document'],
                    pull.stitch_packet_records(io.StringIO(fixture['default'])),
                    pull.stitch_packet_records(io.StringIO(fixture['hidden']))),fixture['shift'])
                self.assertEqual(fixture['default']==fixture['hidden'],fixture['shift']==0)

    def test_offline_receipt_checks_both_sources_and_output(self):
        source=Path(pull.__file__).read_bytes();sha=hashlib.sha256(source).hexdigest()
        core=verify.load_core_bytes(source,sha)
        task,receipt=synthetic_receipt();receipt['client_artifact_sha256']=sha
        for prefix,duration in [('source-000',60),('source-001',60),('output',120)]:
            doc=json.loads(receipt['audits'][prefix+'.json']);doc['mp4_edit']=evidence(duration=duration)
            receipt['audits'][prefix+'.json']=json.dumps(doc)
            text=receipt['audits'][prefix+'.hidden'];rows=list(pull.stitch_packet_records(io.StringIO(text)))
            receipt['audits'][prefix+'.hidden']=''.join('pts=%d|dts=%d|duration=%d|size=%d|flags=%s|data_hash=%s\n'%(p+2,d+2,*rest) for p,d,*rest in rows)
        def replay(r):return verify.verify(core,task,gzip.compress(pull.cleanup_json_bytes(r)),sha)
        self.assertEqual(replay(receipt)['result'],receipt['result'])
        for prefix in ('source-000','source-001','output'):
            altered=copy.deepcopy(receipt);altered['audits'][prefix+'.hidden']=altered['audits'][prefix+'.hidden'].replace('pts=2','pts=3',1)
            with self.assertRaises(verify.ProofError):replay(altered)
            altered=copy.deepcopy(receipt);doc=json.loads(altered['audits'][prefix+'.json']);del doc['mp4_edit'];altered['audits'][prefix+'.json']=json.dumps(doc)
            with self.assertRaises(verify.ProofError):replay(altered)


if __name__=='__main__':unittest.main()
