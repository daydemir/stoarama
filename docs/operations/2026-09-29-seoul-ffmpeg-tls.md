# Seoul/Kamery24 FFmpeg TLS diagnosis — 2026-09-29

## Finding

The observed origins omit intermediates. This is a provider chain-configuration
error, with a practical client workaround: supplement the existing roots with
verified public intermediates for these exact input hosts. Repeated retries
cannot repair a consistently incomplete chain.

All three authoritative Seoul nameservers (`ns`, `ns2`, `ns3.metro.seoul.kr`),
the local resolver, Cloudflare, and Google returned one A record:
`topiscctv1.eseoul.go.kr -> 175.193.202.230`. No AAAA record was returned.
The master, variant, and segment URLs for both 402 and 404 stay on that host;
there were no observed redirects or additional edge hostnames. `sd1` and
`edge13` are URL paths, not separate DNS hosts. DNS is a point-in-time observation;
a single advertised IP does not rule out hidden provider load balancing.

| Origin / advertised IP | Wire chain | Default trust result | Supplemented result |
| --- | --- | --- | --- |
| `topiscctv1.eseoul.go.kr` / `175.193.202.230` (402 and 404) | One leaf, `CN=*.eseoul.go.kr`; issuer GlobalSign GCC R6 AlphaSSL CA 2025 | OpenSSL verification errors 20/21; FFmpeg exit 251, `certificate verify failed` | OpenSSL verifies; each HLS source produces 20 seconds of video, exit 0 |
| `online2.kamery24.org` / `178.19.110.122` | One leaf, `CN=kamery24.org`; issuer Let's Encrypt YR2 | OpenSSL verification errors 20/21; FFmpeg exit 251 | Full YR2 + cross-signed Root YR chain verifies; FFmpeg reaches HTTP 404 for `/cam/debica.m3u8` |

Seoul leaf SHA-256:
`61c337753a5d2469c6ba79513988144100305327c0b249aa5b6ce33071d59dce`.
Validity: 2025-11-17 05:37:41 UTC to 2026-12-19 05:37:40 UTC.
The repeated handshakes observed the same leaf, not a different/expired certificate.

Kamery24 leaf SHA-256:
`bc8c105d21c6e75050ae0bb13ef629e630a4a5b3e59ba83aef74f0020a261808`.
Validity: 2026-07-16 09:45:04 UTC to 2026-10-14 09:45:03 UTC.
It omits **both** YR2 and the cross-signed Root YR needed to reach ISRG Root X1.

## Reproduction and TLS backend

`openssl s_client -connect IP:443 -servername HOST -showcerts -verify_hostname HOST`
returned only the leaves above. Re-running with `-verify_return_error` made the
failure exit nonzero; supplying roots plus the verified intermediates succeeded.
Plain `s_client` may exit zero despite a verification error, so its exit code
alone is not evidence of a valid chain.

Node 93's `~/.stoarama/bin/ffmpeg` is Homebrew **9.0.1**, linked to OpenSSL 3
(`--enable-openssl`). Its TLS help reports verification **true** by default.
OpenSSL's installation default is `/opt/homebrew/etc/openssl@3/cert.pem`;
the relay's existing Darwin startup selects `/etc/ssl/cert.pem` and sets
`SSL_CERT_FILE` (clearing `SSL_CERT_DIR`). Both root bundles contain the needed
public roots, but neither contains the omitted intermediates. FFmpeg's OpenSSL
backend uses `SSL_CTX_set_default_verify_paths`; there is no AIA-fetching path
in its TLS implementation. Its `-ca_file` path calls `SSL_CTX_load_verify_locations`.

The installed Apple curl 8.7.1 uses **SecureTransport**, rather than FFmpeg's
OpenSSL path. Curl succeeds against Seoul. This is consistent with native
chain building / intermediate retrieval or caching; this probe does not
separate native AIA retrieval from an already-cached intermediate.

Light FFmpeg probes used an ephemeral loopback CONNECT forwarder mapping the
original hostname to the observed IP, preserving Host and SNI without editing
DNS or the running relay. Baseline failed immediately for both Seoul paths.
`-ca_file` repaired the master request but **failed on the variant playlist**:
FFmpeg HLS's `ffio_copy_url_options` does not copy `ca_file` or `tls_verify`.
Putting the augmented bundle in the child's `SSL_CERT_FILE` repaired master,
variant, segment, and playlist-reload connections. Each `-t 20 -c copy -f null`
probe produced 600 video frames with exit 0. Live input arrived faster than real
time, so these 20-media-second probes lasted approximately 12–13 wall seconds.
A final probe used the changed `CaptureContinuousWithHeaders` code with a
20-second context and five-second target segments. Recording 402 returned four
finalized segments and no error (25.7 wall seconds including clean shutdown).
Recording 404 returned three finalized segments, then `missing moov box` for
an unfinished final segment at shutdown (40.0 wall seconds, consistent with the
existing 20-second shutdown grace expiring). Neither produced a TLS error.
The standalone 20-media-second FFmpeg probe for 404 exited successfully; the
bounded capture probe additionally exposes a shutdown/finalization limitation.
Source timestamp warnings were observed and are independent of TLS; these short
probes do not certify 404's long-window quality grade.

The production heartbeat reports nodes 72/73 on Debian/Raspberry Pi FFmpeg
`7.1.4-0+deb13u1+rpt1`, node 93 on `9.0.1`, all relay version `a038ca72`.
No Pi binary or TLS backend was independently probed. Stock FFmpeg 7.1.1
has verification disabled by default, and a GnuTLS build ignores OpenSSL's
`SSL_CERT_FILE`. The workaround is proven for node 93 / modern OpenSSL builds;
it does not claim a fleet-wide TLS policy change or prove TLS failures on Pis.

## Read-only production evidence

Queries used `PGOPTIONS='-c default_transaction_read_only=on -c statement_timeout=15000'`.
Recording 402 jobs `8556876` and `8729916` each have attempt_count 4 and a saved
`continuous capture made no progress for 5m0s: ... certificate verify failed`
error. Their final lease owner is node 72, but the job error is retained across
handoffs, so that owner is not attribution of the TLS error to that Pi.

Four long within-window gaps in recording 402:

| Date (UTC) | Previous clip end | Next clip start | Gap |
| --- | --- | --- | --- |
| 09-28 | 00:09:56.457 | 00:20:33 | 637 s |
| 09-28 | 02:14:25.758 | 02:24:11 | 585 s |
| 09-29 | 00:12:08.318 | 00:22:55 | 647 s |
| 09-29 | 10:42:13.393 | 10:53:25 | 672 s |

These are consistent with repeated five-minute no-progress handoffs, though the
DB snapshot does not reconstruct the complete per-node attempt timeline.
Recording 404's corresponding jobs completed with empty error_text. Its chain
is equally broken when reached by the verified OpenSSL client; its observed
shorter gaps and C grades cannot all be attributed to TLS from this evidence.
The roughly 12-hour overnight gap is the scheduled closed window, not a failure.

## Change and expected impact

For only `https://topiscctv1.eseoul.go.kr` and `https://online2.kamery24.org`,
all network FFmpeg capture/probe paths create a private per-child CA file from
the inherited `SSL_CERT_FILE` (or supported system bundle) plus embedded public
intermediates. Supplemental CAs must first verify to the selected existing roots;
invalid explicit root bundles fail closed. `SSL_CERT_FILE` changes only on the
child command, never in the relay process environment or system CA store.
Capture paths use their existing owned temp directory and remove the file on
return. No trust file, running service, or binary is modified by this PR.

TLS verification settings remain unchanged, including the existing defaults
of older builds. No `tls_verify=0`, leaf certificate pin, live AIA fetch, global
trust-store mutation, supervisor timing change, API/schema change, or deployment
is included. Existing reconnects already start at 1–2 seconds and cap at 30
seconds; making them faster would not fix this deterministic verification error.

Once separately reviewed/released, modern OpenSSL capture should open these
incomplete-chain origins directly, removing the TLS-driven five-minute surrender
cycles demonstrated on node 93. The expected benefit is preventing future
10-minute losses caused by those cycles; historical coverage is not recoverable.
GnuTLS builds keep their existing system trust behavior, so any independently
confirmed verified-GnuTLS failure needs its own tested solution/provider repair.
Kamery24 still needs a working stream URL: fixing TLS exposes HTTP 404.

The provider should serve a full chain. Remove or refresh the Seoul supplement
before its intermediate expires **2027-05-21**; verify the actual provider chain
before removing it. Kamery24's YR2 expires **2028-09-02**. A provider issuer
rotation may require refreshing these scoped supplements. Root test fixtures
are verification fixtures only and are not embedded in the product.

Validation: full `go test ./...`; the real localhost OpenSSL HLS regression runs
with `FFMPEG_BIN="$HOME/.stoarama/bin/ffmpeg" go test ./internal/capture -run TestFFmpegTLSIncompleteHLSChain`.
The regression reproduces the option-only child-playlist failure, succeeds with
per-child trust, and rejects expired leaves and wrong hostnames. Unit tests cover
exact host matching, original-root preservation, untrusted CA rejection,
malformed explicit roots, immutable parent environment, fingerprints, and cleanup.

## Primary references / certificate provenance

- [GlobalSign AlphaSSL intermediates](https://support.globalsign.com/ca-certificates/intermediate-certificates/alphassl-intermediate-certificates)
- [GlobalSign GCC R6 AlphaSSL CA 2025 DER](https://secure.globalsign.com/cacert/gsgccr6alphasslca2025.crt)
  SHA-256 `a883559231f8388daf35ce41c8101040ae8fd9b656434247b9475af592cc08ca`
- [GlobalSign Root R6 DER](https://secure.globalsign.com/cacert/root-r6.crt) (test fixture)
- [Let's Encrypt chain hierarchy](https://letsencrypt.org/certificates/)
- [YR2 PEM](https://letsencrypt.org/certs/gen-y/int-yr2.pem)
  SHA-256 `238b85a0099c65b970477d5724f1a1d475ce5058cffe4efa8733899bdb863c47`
- [Root YR cross-signed by ISRG Root X1](https://letsencrypt.org/certs/gen-y/root-yr-by-x1.pem)
  SHA-256 `072639d0b140d5bffae16ad9c3f6cc6086040621f51ee61a6d46a8915c07cf76`
- [ISRG Root X1](https://letsencrypt.org/certs/isrgrootx1.pem) (test fixture)
- [FFmpeg OpenSSL backend](https://github.com/FFmpeg/FFmpeg/blob/master/libavformat/tls_openssl.c)
- [FFmpeg HLS URL option propagation](https://github.com/FFmpeg/FFmpeg/blob/master/libavformat/aviobuf.c)
- [FFmpeg 7.1.1 TLS defaults](https://github.com/FFmpeg/FFmpeg/blob/n7.1.1/libavformat/tls.h)
