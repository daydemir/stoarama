#!/usr/bin/env python3
"""Copy manifest-listed staging artifacts with expiring, create-only capabilities.

The caller owns admission and immutable receipts. No bucket credentials, source
deletion, coverage manifests or public generation changes are supported here.
"""
import hashlib
import pathlib
import urllib.error
import urllib.parse
import urllib.request


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *args, **kwargs):
        return None


OPENER = urllib.request.build_opener(NoRedirect)


def endpoint(cap, bucket, key):
    parsed = urllib.parse.urlsplit(cap['url'])
    if (parsed.scheme != 'https' or parsed.username or parsed.password or
            not (parsed.hostname or '').endswith('.r2.cloudflarestorage.com') or
            parsed.port not in (None, 443) or not bucket or '/' in bucket or
            urllib.parse.unquote(parsed.path) != '/' + bucket + '/' + key):
        raise ValueError('artifact capability endpoint refused')
    return cap['url']


def identity(stream, limit):
    digest = hashlib.sha256()
    size = 0
    while True:
        block = stream.read(min(1024 * 1024, limit + 1 - size))
        if not block:
            return size, digest.hexdigest()
        size += len(block)
        digest.update(block)
        if size > limit:
            raise ValueError('artifact exceeds expected size')


def export(root, result, capabilities):
    outputs = result['manifest']['outputs']
    if result.get('ready_to_publish') is not False or not outputs:
        raise ValueError('export requires nonempty staging result')
    keys = [o['object_key'] for o in outputs]
    if len(keys) != len(set(keys)) or set(keys) != set(capabilities):
        raise ValueError('export artifact accounting differs')
    receipts = []
    for output in outputs:
        key = output['object_key']
        relative = pathlib.PurePosixPath(key)
        if (not key.startswith('joined/best14-20261021/') or
                str(relative) != key or '..' in relative.parts or '\\' in key):
            raise ValueError('artifact namespace refused')
        path = root / 'outputs' / key
        if path.is_symlink() or not path.resolve().is_relative_to((root / 'outputs').resolve()):
            raise ValueError('artifact path refused')
        expected = (output['size_bytes'], output['sha256'])
        proof = result['full_decode_proofs'][key]
        if (proof.get('strict_full_decode') is not True or
                (proof['bytes'], proof['sha256']) != expected):
            raise ValueError('artifact full decode proof differs')
        with path.open('rb') as source:
            if identity(source, expected[0]) != expected:
                raise ValueError('local artifact identity differs')
        caps = capabilities[key]
        put_url = endpoint(caps['put'], caps['bucket'], key)
        get_url = endpoint(caps['get'], caps['bucket'], key)
        put, get = urllib.parse.urlsplit(put_url), urllib.parse.urlsplit(get_url)
        if (put.hostname, put.port or 443, urllib.parse.unquote(put.path)) != (get.hostname, get.port or 443, urllib.parse.unquote(get.path)):
            raise ValueError('export PUT and GET destinations differ')
        headers = caps['put']['headers']
        if headers.get('If-None-Match') != '*' or int(headers.get('Content-Length', '-1')) != expected[0]:
            raise ValueError('artifact PUT must be bounded and create only')
        try:
            with path.open('rb') as source:
                request = urllib.request.Request(put_url, data=source, headers=headers, method='PUT')
                with OPENER.open(request, timeout=120) as response:
                    if response.status not in (200, 201):
                        raise ValueError('artifact PUT status differs')
        except urllib.error.HTTPError as error:
            # A prior uncertain submission may already have created exactly this
            # artifact. Only a complete readback can establish that identity.
            if error.code != 412:
                raise ValueError('artifact PUT rejected') from None
        except (OSError, TimeoutError):
            # Reconcile an uncertain PUT through the same full readback.
            pass
        request = urllib.request.Request(get_url, headers=caps['get'].get('headers', {}), method='GET')
        try:
            with OPENER.open(request, timeout=120) as response:
                if response.status != 200 or int(response.headers.get('Content-Length', '-1')) != expected[0]:
                    raise ValueError('artifact readback size differs')
                if identity(response, expected[0]) != expected:
                    raise ValueError('artifact readback identity differs')
        except urllib.error.URLError:
            raise ValueError('artifact readback failed') from None
        receipts.append({'object_key': key, 'bytes': expected[0], 'sha256': expected[1],
                         'full_readback_verified': True})
    return receipts
