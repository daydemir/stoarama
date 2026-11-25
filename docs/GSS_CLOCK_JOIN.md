# GSS clock join: second stitching pass, and the 435 pilot runbook

Deniz, Nov 25 2026, verbatim:
- "also have a subagent look up the clock based stitching we were doing too we can also do that in parallel after the first stitching pass for the stream is done"
- "keep the old file names agnostic to the stitching tool ... the files should just be stitched and delivered"
- He approved the 2-hour clock readability check ("yes"). His October goals are quoted in
  `records/finish-plan-20261117/gss-delivery/clock-stitching.md`.

## What the pass does

`stoaramactl collation-v2 clock-join` runs after collation-v2 has produced and registered an
hour's parts. For each seam where collation-v2 split two adjacent parts, it reads the
burned-in seconds clock and joins the parts only when all of these hold
(`backend/internal/clockjoin`):

| Check | Rule |
|---|---|
| Parts | Both parts are collated files in the parent delivery generation, and no live assembly already replaces either. |
| Seam | The manifest has the direct seam (last clip of part N → first clip of part N+1). Its split reason is visual or low-confidence: `frame_low_motion`, `frame_jump`, `frame_insufficient_frames`, `frame_match_missing`, or `db_gap` of at most 2 s. Every other split stays, e.g. a job or capture change, `packet_replay`, `frame_overlap`, a decode failure. |
| Known bad | No source clip of either part is on `--veto-clips` (required: an absent file fails, an empty one states "none known"), the recording is not on `--veto-recordings`, and no quarantined clip lies between the parts. |
| Media | Both parts have the same codec parameters, so stream copy is valid. |
| Clock gap | The clock at N's last frame and N+1's first frame differ by 0–1 s. |
| Frame count | For each part, N (collation-v2's verified video packets) is within ±20% of fps × clock span (fps from packet cadence, never from N). |

Each clock read is cross-checked: the edge frame and the frame 1 s inward must differ by 0–2 s in the right
direction, so a misread digit fails as `clock_inconsistent`. OCR is tesseract on a cropped,
3× upscaled single plane, on CPU (`clock-profiles.json` holds each recording's crop and plane).

A run of joined seams becomes one assembly:
- It is built by `collation.BuildPart`: ffmpeg concat stream copy, `-map 0:v:0 -map 0:a? -c copy`, so **audio is kept**.
- It is verified as a collation-v2 part is: payload chain, packet timing, and a strict full decode. Its audio packet count must equal the parts' total.
- **Delivery name**: the parts' own format, with no tool in it. A run that covers the whole hour takes the hour's single-part name (`…_hour_03.mp4`). Any other run takes its first part's delivered name (`…_hour_03_part_02.mp4`), since that part is no longer delivered.
- **Tool and version** appear only in metadata: the proof JSON (`pass_version`, ffmpeg and tesseract version lines, policy, every read) and the growth repair (`production_version: clock-prestitched`, CLOCK_20 boundary annotations).

**Output.** Everything goes under `--out` and is laid out by R2 key. Each file is created only if absent (an existing key must already hold the same sha256):
- `joined/clock-prestitch-YYYYMMDD/objects/<sha256>.mp4`: the assembly. This is a new prefix, never `experimental/stitching-confident-repairs/`.
- `joined/clock-prestitch-YYYYMMDD/proofs/<sha256>.json`: its proof.
- `results/<hour_id>.json`: every seam verdict and assembly for the hour. A rerun skips any hour that already has a result.

**What the pass never does.** It writes nothing to R2 and nothing to the database.
- `--source-r2` reads through an `r2.Client` that has no writer database, so the client itself refuses every write.
- No hour or output rows are created. The assemblies' inputs stay registered collation-v2 outputs.

## Registration: the existing generation and keep-hold model

Run `backend/scripts/gss/build_generation.py --clock-assemblies <clock-join out>...`. The builder adds each assembly to the new generation:
- in `files`, as `sha256:<sha>`, which the server treats as `video_assembly`;
- in the stream manifest, with `replacement_inputs`; its inputs leave the file list but stay on the timeline;
- in growth `repairs`, where the page shows "N original parts → one verified Clock-prestitched assembly · CLOCK_20 · k boundaries verified".

It also writes three files for the controller:
- `uploads.tsv`: the video and the proof, hard-linked into `objects/`;
- `holds.tsv`: `video_assembly` and `clock_proof` rows;
- `dependencies.tsv`: `delivery_keep_output_dependencies` rows for every input.

The builder refuses any of these:
- an input that another live assembly already replaces;
- an input that is not a selected collated file of the same recording;
- an input claimed by two clock assemblies;
- a delivery path that is already taken.

**Writer fence: publishing needs a decision from Deniz.** This PR changes no fence, and no existing fence exception admits these uploads.
- The #620 exception (migration 0262) admits only collation-v2 recovery hours and their parts, through `r2.Client`. Clock assemblies are neither.
- The generation builder's asset uploads (#618) are controller uploads outside `r2.Client`. No fence row admits them either.
- So the pilot can run, verify and build its preview generation without any R2 write (steps 0–3). Step 4's uploads need Deniz to choose one of two routes:
  - approve them as controller create-only uploads, as he did for the generation assets;
  - ask for a reviewed fence design that admits `joined/clock-prestitch-*` keys.

**Protection.** `delivery_keep_holds` gates publication (`RequireDeliveryKeep`). Since migration 0244 it no longer holds R2 objects from retirement.
- Today the new prefix is outside every retirement lane's selection. `inverseCollatedPrefixAllowed` names only `joined/goodplus-`, `joined/nightly-collation-v2/` and `joined/best14-20261021/`, and the delivery exemptions cover only `experimental/stitching-confident-repairs/`.
- That is incidental, so step 4 also adds the keys to the Jelly lane protect list, as best-14 plan step 0 does for the other delivery keys.

## Pilot: stream 435 (Kroměříž – Hanácké náměstí)

**Facts as of Nov 25** (live generation `ae14e746`, read-only):
- The best-14 window is 09-19 to 10-02.
- 09-19 to 09-21 are KE days that have not been stitched yet; they are deferred on the droplets.
- The other 11 days have 82 collated hours and 306 delivered parts.
- **439 live V79/V85 assemblies already replace 3,084 of 435's parts.** The pass leaves those alone (`already_assembled`). It therefore reaches only the roughly 224 seams between parts that are still delivered. If the V79/V85 assemblies are retired (Deniz's Nov 19 delete order covers `experimental/stitching-confident-repairs/`), rerun against a parent generation without them.

**Pre-pilot read check.** This was read-only, on five random adjacent part pairs fetched from the public file URLs, using the committed 435 profile (top right, blue plane):
- 20 of 20 edge reads were OK;
- all 5 seams read a gap of 0 s;
- spans were 60 s against 1,800 frames at 30 fps, an error of 0%.

### 0. Tools (Mooney, then the droplet)

```sh
cd backend && GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -o /tmp/stoaramactl-clock ./cmd/stoaramactl
scp /tmp/stoaramactl-clock backend/scripts/gss/clock-profiles.json backend/scripts/gss/build_generation.py root@167.71.91.3:/opt/collate/bin/
ssh root@167.71.91.3 'apt-get install -y tesseract-ocr && tesseract --version | head -1'   # expect: tesseract 5.x
```

### 1. Hour list (Jelly, read-only; psql uses PG* env only)

```sh
PGOPTIONS='-c default_transaction_read_only=on' psql -At -v ON_ERROR_STOP=1 -c "
SELECT manifest_object_key FROM recording_collation_hours
WHERE recording_id=435 AND generation=2 AND status='collated'
  AND local_date BETWEEN '2026-09-19' AND '2026-10-02'
ORDER BY local_date, delivery_hour" > hours-435.txt
wc -l hours-435.txt          # expect 82
scp hours-435.txt root@167.71.91.3:/mnt/gss_best14_scratch/
```

### 2. Clock join (droplet; R2 reads only)

The known-bad list is required.
- October's known-bad flags are in `v85-local-plan.md:59-67`. For the named streams they excluded 433 (`CLOCK_present_knownbad_excluded`) and flagged no 435 clip.
- The clips collation-v2 itself found broken are already quarantined in each manifest, and the pass vetoes any quarantined clip between two parts.
- So for 435 the list is explicitly empty. For another stream, put the clip ids of any known-bad source in it.

```sh
: > veto-clips-435.txt     # explicit: no known-bad 435 clips
cd /mnt/gss_best14_scratch
set -a; . /opt/collate/r2.env; set +a
export FFMPEG_BIN=/opt/collate/bin/ffmpeg FFPROBE_BIN=/opt/collate/bin/ffprobe
/opt/collate/bin/stoaramactl-clock collation-v2 clock-join \
  --hours hours-435.txt --source-r2 --veto-clips veto-clips-435.txt \
  --registry datasets/global-street-scores-delivery/47/public/generations/ae14e746468c5c6402cd6f8ca55aca6e.json \
  --profiles /opt/collate/bin/clock-profiles.json --veto-recordings 433 \
  --batch clock-prestitch-20261126 --out clock-435 2>&1 | tee clock-435.log
```

**Expected result:**
- One log line per hour (`status=joined|no_join|single_part`).
- A final JSON summary with `hours: 82` and `failed: 0`. Single-part hours count under `statuses.single_part`.
- `seams` counts every adjacent pair of manifest parts, including the pairs vetoed before OCR. `reasons` breaks them down:
  - `part_not_delivered` and `already_assembled`: parts that V79/V85 assemblies replace in `ae14e746`. This is the bulk, since 3,084 of 435's parts are replaced.
  - `seam_not_eligible`: metadata or replay splits.
  - The rest went to the clock, about 224 seams: `clock_continuous` (joined), or `clock_gap`, `clock_overlap`, `clock_unreadable`, `clock_inconsistent`, `frame_count_mismatch`.
- The pilot's yield is `clock_continuous` divided by the seams that reached the clock.
- A failed hour (a network error, for example) is retried by rerunning the same command. Hours that are done are skipped.

**Verify on the droplet before registering:**

```sh
ls clock-435/results | wc -l                                    # 82
python3 -I -c 'import json,glob; a=[x for f in glob.glob("clock-435/results/*.json") for x in json.load(open(f))["assemblies"]]; print(len(a), sum(x["input_parts"] for x in a), sorted({x["audio_delivery_status"] for x in a}))'
for f in $(ls clock-435/joined/clock-prestitch-20261126/objects/*.mp4 | head -3); do
  sha256sum "$f"; ffprobe -v error -show_entries stream=codec_type,codec_name -of csv=p=0 "$f"   # name = sha; h264 + aac
done
```

To review a seam by eye:
1. Take a result's `seams[].prev.tail` / `next.head` reads and the assembly's `input_ids`.
2. Cut 6 s around the join: `ffmpeg -ss <part1 seconds - 3> -i <assembly> -t 6 -c copy seam.mp4`.
3. Watch the clock tick over without a jump.

### 3. Build the preview generation (droplet; local files only)

The builder needs the parent mirror (registry plus assets by key), the base asset holds and `best14-days.tsv`. These are the same inputs that built `ae14e746`.

```sh
P=ae14e746468c5c6402cd6f8ca55aca6e; D=datasets/global-street-scores-delivery/47
export AWS_ACCESS_KEY_ID="$R2_ACCESS_KEY_ID" AWS_SECRET_ACCESS_KEY="$R2_SECRET_ACCESS_KEY"
aws s3 cp "s3://$R2_BUCKET/$D/public/generations/$P.json" parent.json --endpoint-url "$R2_ENDPOINT"
python3 -I -c 'import json; [print(o["key"]) for o in json.load(open("parent.json"))["assets"].values()]' |
  while read k; do aws s3 cp "s3://$R2_BUCKET/$k" "assets/$k" --endpoint-url "$R2_ENDPOINT" --only-show-errors; done
python3 -I -c 'import json; [print(k) for k in json.load(open("parent.json"))["files"]]' > parent-files.txt
# base-holds.json: the read-only psql export in build_generation.py's docstring (Jelly), copied here.
python3 -I /opt/collate/bin/build_generation.py --parent parent.json --base parent.json --assets assets \
  --base-holds base-holds.json --best14 best14-days.tsv --files parent-files.txt \
  --clock-assemblies clock-435 --note "ae14e746 + clock-prestitched pilot 435" --out gen-clock-435
```

The output directory must be on the same filesystem as `clock-435`, because the videos are hard-linked.

**Expected summary:**
- `clock_assemblies` = the assemblies counted in step 2;
- `changed_recordings: [435]`;
- `files` = parent files − replaced inputs + assemblies.

`uploads.tsv` lists the changed assets plus 2 rows per assembly (video and proof). `holds.tsv` has the same keys. `dependencies.tsv` has one row per input.

### 4. Preview (controller, with go-ahead: R2 create-only uploads and DB inserts)

1. **Only after Deniz approves the upload route** (see the writer-fence note), upload every `uploads.tsv` row create-only and check its ETag:
   ```sh
   tail -n +2 gen-clock-435/uploads.tsv | while IFS=$'\t' read key local sha size ctype etag; do
     got=$(aws s3api put-object --bucket "$R2_BUCKET" --key "$key" --body "gen-clock-435/$local" --content-type "$ctype" \
       --if-none-match '*' --endpoint-url "$R2_ENDPOINT" --query ETag --output text) || { echo "REFUSED $key"; break; }
     [ "$got" = "\"$etag\"" ] || { echo "ETAG MISMATCH $key"; break; }
   done
   ```
   A refused PUT on a key that already exists is fine only if HEAD shows the same ETag. Then add every new `joined/clock-prestitch-*` key to the Jelly lane protect list.
2. Insert the holds and dependencies (psql via PG* env):
   ```sql
   \copy delivery_keep_holds(bucket,object_key,etag,size_bytes,recording_id,kind,sha256,relative_path) FROM 'holds.tsv' WITH (FORMAT text, HEADER)
   CREATE TEMP TABLE d(output_id bigint, recording_id bigint);
   \copy d FROM 'dependencies.tsv' WITH (FORMAT text, HEADER)
   INSERT INTO delivery_keep_output_dependencies SELECT * FROM d ON CONFLICT (output_id) DO NOTHING;
   SELECT count(*) FROM d JOIN delivery_keep_output_dependencies k USING (output_id) WHERE k.recording_id<>d.recording_id;  -- expect 0
   ```
3. Upload `registry.json.gz` as `datasets/global-street-scores-delivery/47/public/generations/<new id>.json` **only**. Do not touch `public/current.json`.
4. Open `https://stoarama.com/global-street-scores-delivery?generation=<new id>`, choose Kroměříž, and expand **Current assemblies**. Check that:
   - the "Clock-prestitched" assemblies appear with "CLOCK_20 · k boundaries verified";
   - an hour shows the assembly once, under its tool-agnostic name, in place of its parts;
   - Play shows the clock running through each seam, with sound;
   - a downloaded file's sha256 equals its registry `sha256` (the R2 key is `<sha256>.mp4`; the download name is the tool-agnostic part name).
   While a NAS cleanup permit is live, the public page serves only the pinned generation and answers "public publication deferred during NAS cleanup". If so, preview once the cleanup window closes.

### 5. Swap (only on Deniz's go)

Upload the same registry as `public/current.json` and move the pinned `reviewed` generation in `backend/internal/api/server_global_delivery.go`, in a PR, as #618 did.

**Rollback** is the previous `current.json`. Every new key is create-only, and the old generation is untouched.

**Scale-out after the pilot.** Add the profile of each viable stream from `clock-readability.tsv` (tune it with `clock-read` on 3 parts; expect `status: ok` at both edges), then run on the hours that stream has finished.
