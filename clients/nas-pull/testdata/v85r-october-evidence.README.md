The compressed fixture contains 14 complete recorded cases from October's
qualified15 corpus (SHA-256
`b4020c3c03c001e3d123d349cc1c2f31a6946aceaa7676696870668cb7e0217b`).
Expected decisions come from the recovered 73,843-edge JSONL (SHA-256
`5b2765ecb4f6483c8e48b5a6c958c7c0d8082764d4da57901626a22569c6346b`).

The Mooney recovery index `handoffs/v85-missing-pieces-evidence/r2-proof-scan.jsonl`
provides independently hash-checked archive members. Each fixture records its
archive, native observation and full-source qualification SHA. Source contexts
are reconstructed from the qualification's recorded full decoded PTS vector,
last presentation endpoint, expected capture span and original decoded-proof
member SHA; source SHA/size and strict-decode flags are checked against the
qualified15 bindings. Spatial diagnostics and packet/audio observations come
from those native members; pair metadata and the frozen 20-second diagnostic
come from qualified15. Spatial diagnostic hashes use the pinned classifier's
canonical JSON format. Historical capture lease tokens are replaced with test
aliases preserving equality and absence. No capabilities or credentials are
included. The missing fifteenth case has no complete recovered spatial row and
is deliberately excluded.

These are recorded-feature adapter replays, not fresh media recomputations or
claims that the entire October corpus has been reproduced. The separate Linux
test executes the packaged producer on actual source bytes with the pinned
October FFmpeg/ffprobe. Synthetic mutations test the stationary negative veto,
missing authentication and the disabled spatial-positive route.
