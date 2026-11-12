import json
import sqlite3
import tempfile
import threading
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest import mock
import stoarama_pull as pull

class ListingProofTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.cfg = SimpleNamespace(output_dir=self.root / 'clips', state_dir=self.root / 'state')
        self.cfg.output_dir.mkdir()
        self.cfg.state_dir.mkdir()
        for name in ('raw/a.mp4', 'joined/b.mp4', '_V85r_keep28/c.mp4'):
            p = self.cfg.output_dir / name
            p.parent.mkdir(exist_ok=True)
            p.write_bytes(b'abc')
        self.calls = []
    def request(self, cfg, method, path, body):
        self.calls.append(body)
        return {'ok': True}
    def test_complete_covers_all_trees_without_hashing(self):
        with mock.patch.object(pull, 'request_json', side_effect=self.request), mock.patch.object(pull, 'sha256_file_throttled_stable', side_effect=AssertionError('hashed media')):
            self.assertTrue(pull.listing_proof_once(self.cfg, threading.Event()))
        self.assertEqual(self.calls[0]['action'], 'start')
        self.assertEqual(self.calls[0]['folders'], ['.', '_V85r_keep28', 'joined', 'raw'])
        files = [f for c in self.calls for f in c.get('files', [])]
        self.assertEqual({f['path'] for f in files}, {'raw/a.mp4', 'joined/b.mp4', '_V85r_keep28/c.mp4'})
        self.assertTrue(all(f['size'] == 3 and f['inode'] > 0 for f in files))
        self.assertEqual(self.calls[-1]['action'], 'complete')
    def test_lost_response_replays_same_page_and_generation(self):
        failed = False
        def request(*args, **kwargs):
            nonlocal failed
            body = kwargs['body']
            self.calls.append(body)
            if body['action'] == 'page' and not failed:
                failed = True
                raise OSError('lost ack')
            return {'ok': True}
        with mock.patch.object(pull, 'request_json', side_effect=request):
            with self.assertRaises(OSError):
                pull.listing_proof_once(self.cfg, threading.Event())
            pull.listing_proof_once(self.cfg, threading.Event())
        starts = [c for c in self.calls if c['action'] == 'start']
        self.assertEqual(starts[0], starts[1])
        pages = [c for c in self.calls if c['action'] == 'page']
        self.assertEqual(pages[0], pages[1])
    def test_directory_change_never_completes(self):
        def request(*args, **kwargs):
            body = kwargs['body']
            self.calls.append(body)
            if body['action'] == 'page':
                p = self.cfg.output_dir / 'raw/a.mp4'
                if p.exists():
                    p.rename(p.with_name('renamed.mp4'))
            return {'ok': True}
        with mock.patch.object(pull, 'request_json', side_effect=request):
            with self.assertRaises(pull.ListingChanged):
                pull.listing_proof_once(self.cfg, threading.Event())
        self.assertFalse(any(c['action'] == 'complete' for c in self.calls))
    def test_symlink_fails_closed(self):
        (self.cfg.output_dir / 'raw/link.mp4').symlink_to('a.mp4')
        with mock.patch.object(pull, 'request_json', side_effect=self.request):
            with self.assertRaises(pull.ListingChanged):
                pull.listing_proof_once(self.cfg, threading.Event())
        self.assertFalse(any(c['action'] == 'complete' for c in self.calls))
