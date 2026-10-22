import hashlib
import io
import pathlib
import tempfile
import unittest
import urllib.error
from unittest import mock

import capability_export as export


class Response(io.BytesIO):
    status = 200

    def __init__(self, body=b''):
        super().__init__(body)
        self.headers = {'Content-Length': str(len(body))}


class ExportTest(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = pathlib.Path(self.directory.name)
        self.body = b'verified-media'
        self.sha = hashlib.sha256(self.body).hexdigest()
        self.key = 'joined/best14-20261021/objects/' + self.sha + '.mp4'
        self.path = self.root / 'outputs' / self.key
        self.path.parent.mkdir(parents=True)
        self.path.write_bytes(self.body)
        self.result = {'ready_to_publish': False, 'manifest': {'outputs': [
            {'object_key': self.key, 'size_bytes': len(self.body), 'sha256': self.sha}]},
            'full_decode_proofs': {self.key: {'strict_full_decode': True, 'bytes': len(self.body), 'sha256': self.sha}}}
        url = 'https://account.r2.cloudflarestorage.com/bucket/' + self.key
        self.caps = {self.key: {'bucket': 'bucket', 'put': {'url': url + '?put',
            'headers': {'If-None-Match': '*', 'Content-Length': str(len(self.body))}},
            'get': {'url': url + '?get', 'headers': {}}}}

    def test_upload_and_readback(self):
        calls = []

        def opened(request, timeout):
            calls.append(request.get_method())
            if request.get_method() == 'PUT':
                self.assertEqual(request.data.read(), self.body)
                return Response()
            return Response(self.body)

        with mock.patch.object(export.OPENER, 'open', side_effect=opened):
            receipts = export.export(self.root, self.result, self.caps)
        self.assertEqual(calls, ['PUT', 'GET'])
        self.assertTrue(receipts[0]['full_readback_verified'])
        self.assertTrue(self.path.exists())

    def test_uncertain_put_requires_matching_complete_readback(self):
        for existing, succeeds in [(self.body, True), (b'wrong-content!', False)]:
            with self.subTest(succeeds=succeeds):
                conflict = urllib.error.HTTPError('redacted', 412, 'exists', {}, None)
                with mock.patch.object(export.OPENER, 'open', side_effect=[conflict, Response(existing)]):
                    if succeeds:
                        self.assertTrue(export.export(self.root, self.result, self.caps)[0]['full_readback_verified'])
                    else:
                        with self.assertRaises(ValueError):
                            export.export(self.root, self.result, self.caps)
                self.assertTrue(self.path.exists())

    def test_mismatched_destination_never_uploads_or_certifies(self):
        self.caps[self.key]['get']['url'] = self.caps[self.key]['get']['url'].replace('account.', 'another.')
        with mock.patch.object(export.OPENER, 'open') as opened:
            with self.assertRaisesRegex(ValueError, 'destinations differ'):
                export.export(self.root, self.result, self.caps)
            opened.assert_not_called()

    def test_changed_local_artifact_never_uploads(self):
        self.path.write_bytes(b'corrupt')
        with mock.patch.object(export.OPENER, 'open') as opened:
            with self.assertRaisesRegex(ValueError, 'identity differs'):
                export.export(self.root, self.result, self.caps)
            opened.assert_not_called()


if __name__ == '__main__':
    unittest.main()
