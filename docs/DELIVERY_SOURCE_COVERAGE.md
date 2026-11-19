# Delivery source coverage

Collation v2 records which raw clips went into a delivery video. To measure
exact per-source coverage, use the frozen manifest and the bytes of one
delivery part plus all its raw members.

```sh
python3 clients/nas-pull/delivery_coverage.py \
  --manifest hour.json --manifest-sha256 "$catalog_manifest_sha256" --part 1 \
  output.mp4 first-raw.mp4 second-raw.mp4 > coverage.json
```

Supply the raw files in the output part's `source_clip_ids` order. Use the
manifest SHA-256 pinned in the catalog. Run on a host with FFmpeg and FFprobe
that support the integer AAC decoder `aac_fixed` and the `ashowinfo` filter.
CI exercises the exact media-tool pair pinned for joined recording tests.
The verifier uses scratch
space for the decoded audio of one output and one raw clip at a time.

The command checks stable local file identities, SHA-256 packet chains,
exact presentation mapping and every visible H.264 frame. For AAC-LC it
compares every visible sample using the integer decoder. The verifier reads
the MP4 track flags and edit bounds directly and rejects dwell, non-unit
rates, and unsupported edit forms. It compares the
first frame at each in-part seam with a reference decode primed by the
predecessor's exact AAC packet. Changed output identity or ambiguous packet
mapping returns a nonzero exit code without evidence. Once the complete packet
mapping is verified, a source that fails its independent decode checks receives
a `not_proven` verdict and reason. Only `proven` rows carry media evidence.

The JSON is local media evidence. A delivery handoff also needs to reconcile
the manifest with the current catalog, recheck the exact NAS copy and verify
recipient access before any delivery hold can be changed. This verifier
does not change the catalog, R2, the delivery page or NAS holds.
