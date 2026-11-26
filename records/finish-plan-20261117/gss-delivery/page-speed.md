# Global Street Scores delivery page speed

## Outside measurements on November 26, 2026

Mooney used curl with compression and a new Chromium incognito tab. Both HTML pages returned 200. The delivery data API stalled during sampling, so the browser reached the loading screens and usable live timings could not be collected. Live probes stopped during the incident. All subsequent timing, manifest and ticket requests used loopback and a local generation.

| Live request | TTFB | Total | Encoded body |
| --- | ---: | ---: | ---: |
| Delivery HTML, fresh curl connection | 155 ms | 155 ms | 2,302 B |
| Files HTML, reused curl connection | 109 ms | 109 ms | 1,407 B |
| Plain metadata | 125.13 s | 125.13 s | 7,735 B, Cloudflare 524 |
| Metadata `.gz` probe | 7.71 s | 7.71 s | 39 B, 404 |
| Catalog | >35 s | curl stopped at 35 s | 0 B |
| Growth | >35 s | curl stopped at 35 s | 0 B |

The main page painted at 512 ms and fired the load event at 703 ms. Styles started around 176 ms and finished by 465 ms. Blocking scripts finished by 502 ms. The four fonts finished at 678–702 ms and accounted for 368,504 encoded bytes. The Files page painted at 164 ms, and fired load at 297 ms, after the main page warmed its shared styles, fonts, and download script. Both pages still showed “Loading streams…”.

| Main live startup asset | Requests | Encoded bytes | Last byte after navigation |
| --- | ---: | ---: | ---: |
| style.css | 1 | 3,587 | 305 ms |
| unified.css | 1 | 2,393 | 311 ms |
| fonts.css | 1 | 165 | 312 ms |
| loading.css | 1 | 633 | 313 ms |
| blue-evidence.css | 1 | 530 | 442 ms |
| compact.css | 1 | 2,339 | 465 ms |
| production-bridge.js | 1 | 1,174 | 304 ms |
| current-parts.js | 1 | 867 | 305 ms |
| availability.js | 1 | 1,471 | 305 ms |
| blue-evidence.js | 1 | 2,477 | 318 ms |
| one-line.js | 1 | 990 | 323 ms |
| loading.js | 1 | 1,489 | 455 ms |
| weekly-downloads.js | 1 | 3,334 | 502 ms |
| compact.js | 1 | 2,384 | 293 ms |
| coverage-growth.js | 1 | 4,195 | 315 ms |
| unified.js | 1 | 3,662 | 327 ms |
| app.js | 1 | 12,135 | 424 ms |
| plex-1.ttf | 1 | 53,476 | 693 ms |
| plex-2.ttf | 1 | 97,711 | 678 ms |
| plex-3.ttf | 1 | 108,592 | 702 ms |
| plex-4.ttf | 1 | 108,725 | 693 ms |

Files used the three shared CSS assets, the shared download script, the four cached fonts, and one new request for files.js (1,631 encoded bytes; finished at 293 ms). The waterfall above counts completed resources. Hung API fetches were still in flight. Live cold Files, warm main, manifest, and ticket timing were left unmeasured when live sampling stopped.

## What delayed interaction

- Every request acquired a database connection to read the cleanup fence before taking the registry mutex. On a cold public registry load, the mutex holder then asked the same pool for another connection to check durable keeps. If the pool was full of requests waiting for the mutex, neither side could proceed. A local Postgres test with a one-connection pool reproduces the prior failure at the three-second context deadline and passes with the patch. This proves the starvation path. Live timings alone cannot distinguish it from contention on the cleanup advisory lock.
- Unpinned traffic reread and decoded the entire registry after the 15-second freshness window, even when the pointer was unchanged. A 55,000-file test registry allocated about 85 MB per load and retained about 22 MB. The server now rechecks the pointer HEAD and the durable keeps at the same cadence and reuses the parsed registry on identical HEAD identity. The two-entry cache and four concurrent asset-body slots remain.
- The main page serialized metadata, readiness and two missing `.gz` probes. A second, independent growth fetch pulled the same 8.29 MB and delayed the unified stream controls. The page now requests metadata and readiness together, keeps the provisioned stream `.gz` twins, and shares one growth fetch across startup, refresh and an early hour click.
- The files page needs the catalog before loading the first stream's manifest. Origin gzip shrinks both. Public asset validators are bound to the verified asset SHA-256 and content type. Stamped manifests also bind to the serving generation and downloader SHA-256. Responses are private and must revalidate, including pinned URLs. A conditional request resolves the same registry and fence before it can return 304. Tickets and video keep their existing cache policy.
- Deferred scripts retain their dependency order. WOFF2 keeps the same fonts. The existing TTF URLs remain available to cached stylesheets.

## Local before and after

Baseline was `a8cea303`. Both binaries served the same staged generation `fbc10e1a2b099e3dddf315d07ac14f0a` through the real router, using local object bodies and a fixed 60 ms delay per object-store HEAD or GET. The browser comparison uses Chromium, a 1280×800 preview, a first visit to a fresh origin, and an immediate second navigation. Bytes are browser **transferSize**, including response headers and counting requests completed in the first 1.5 s. Tables are single controlled samples. They measure direct Go responses, so production bandwidth and latency depend on Cloudflare compression, R2, and the database. The database fence regression was tested separately with local Postgres.

| Browser checkpoint | Main cold, before → after | Main warm, before → after | Files cold, before → after | Files warm, before → after |
| --- | ---: | ---: | ---: | ---: |
| First contentful paint | 40 → 32 ms | 44 → 60 ms | 28 → 36 ms | 36 → 36 ms |
| Stream list or selector available | 464 → 374 ms | 184 → 52 ms | 350 → 348 ms | 82 → 28 ms |
| First clickable coverage day or folder | 740 → 630 ms | 441 → 135 ms | 421 → 416 ms | 146 → 28 ms |
| Requests including document | 59 → 56 | 59 → 56 | 11 → 11 | 11 → 11 |
| Transfer, decimal MB | 19.63 → 3.32 | 18.75 → 0.017 | 1.124 → 0.299 | 0.554 → 0.004 |

| Cold browser assets | Main, requests before → after | Main, encoded bytes before → after | Files, requests before → after | Files, encoded bytes before → after |
| --- | ---: | ---: | ---: | ---: |
| HTML, CSS and JS | 18 → 18 | 130,706 → 131,265 | 6 → 6 | 30,566 → 30,551 |
| Font faces requested | 4 → 4 | 745,744 → 261,120 | 3 → 3 | 540,452 → 184,716 |
| Metadata including missing .gz probes | 2 → 1 | 107,456 → 22,457 | — | — |
| Readiness including missing .gz probes | 2 → 1 | 110,535 → 21,975 | — | — |
| Catalog | 1 → 1 | 72,344 → 13,522 | 1 → 1 | 72,344 → 13,522 |
| Growth | 2 → 1 | 16,570,116 → 1,315,213 | — | — |
| Image index | 1 → 1 | 12,105 → 2,901 | — | — |
| Blue evidence | 2 → 2 | 5,154 → 1,978 | — | — |
| Thumbnails | 21 → 21 | 1,192,210 → 1,192,210 | — | — |
| Visible stream timelines | 3 → 3 | 127,437 → 127,437 | — | — |
| Visible exact coverage ranges | 3 → 3 | 533,701 → 216,787 | — | — |
| Default Files manifest | — | — | 1 → 1 | 476,998 → 66,516 |

In the first local main navigation, metadata and readiness finished at 342 and 408 ms before the patch. Both finished at 340 ms after it. The old startup issued growth at 30 and 529 ms. The patched startup issued it once, at 358 ms, finishing at 446 ms. On the Files page, catalog then manifest finished at 346 and 415 ms before the patch and 343 and 410 ms after on cold navigation. The warm Files requests finished at 15 and 20 ms with revalidation.

| Loopback curl with Accept-Encoding | TTFB before → after | Total before → after | Wire bytes before → after |
| --- | ---: | ---: | ---: |
| Metadata after pointer TTL, same publication | 313 → 145 ms | 314 → 145 ms | 107,437 → 22,457 |
| Warm registry, metadata | 62.6 → 63.3 ms | 62.7 → 63.8 ms | 107,437 → 22,457 |
| Warm registry, catalog | 62.7 → 63.4 ms | 62.8 → 63.9 ms | 72,344 → 13,522 |
| Warm registry, growth | 71.7 → 73.7 ms | 74.4 → 96.6 ms | 8,285,058 → 1,315,213 |
| Warm registry, largest manifest in fixture (stream 410) | 72.4 → 68.9 ms | 86.4 → 80.4 ms | 2,498,323 → 301,344 |
| Warm registry, ticket with local presigner | 0.52 → 0.27 ms | 0.58 → 0.31 ms | 479 → 479 |

A matching validator with a warm registry returned 304 for metadata, catalog, growth and the largest manifest in 0.25–0.54 ms with zero body bytes. Ticket timing used a deterministic fixture presigner. The compressed growth response took 22 ms longer over loopback, which has no meaningful transfer delay.

## Verification

- Ran `viewer_smoke.py` and `download_smoke.py` offline on the staged 60-stream generation and again with `--current --origin` directed at the patched local API. Both passed 60/60, including naming.
- Opened a pinned hour for stream 335 in Chromium. It showed two verified parts with the expected July/Thursday names. Opening Download folder produced its pinned Python command.
- Go tests exercise the production-scale compressed manifest memory budget (15.1 MB allocated outside the captured response body for a 4.4 MB manifest), unchanged-pointer reuse and keep recheck, gzip and identity equivalence, generation-specific validators, and the existing NAS fence and pin behavior.
