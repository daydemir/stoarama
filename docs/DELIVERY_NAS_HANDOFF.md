# In-window delivery video copies

The controller has a reviewed roster of 10,893 public collated videos from
31 streams, totaling 2,346,687,248,245 bytes. It is the set of generation-2
video holds that intersect the owner's saved windows, with keep-everything
streams excluded. Assembly and experimental repair holds are outside this
roster.

- Approved roster SHA-256 `da4a4a7f4c98409ad0927878ad9995327f2a749d717e1e47b276e19cec06e8ca`
- Owner picks SHA-256 `648629a094016f205bf949d967ec7366acdf0c980f91694cf66c6610b55814db`

Run the read-only preview against the same owner picks used for the roster.

```sh
stoaramactl delivery-nas-handoff \
  --roster inwindow-roster.jsonl \
  --expected-roster-sha256 "$roster_sha256" \
  --picks docs/records/r2-picks-20261118.json \
  --expected-picks-sha256 "$picks_sha256"
```

The command reproduces the approved selection from the live holds and
catalog. It compares each canonical line to the pinned file and rejects
additions, omissions, changed object identities, and ambiguous rows. It
also verifies the canonical NAS paths used for copying.

Merge with `[skip render]`. The controller applies migration 0224,
deploys the matching API, and verifies the matching NAS client release in
that order. A predecessor client continues using its ordinary collated
feed after the migration and API deploy.

Check the active mount's free space and reserve before queuing. Run the
same command with `--apply` to queue and enable only the approved roster
on connection 13. The import is atomic and repeatable. The ordinary
collated-delivery switch keeps its existing setting. Pause the handoff
independently with `stoaramactl delivery-nas-handoff pause-copy`.

A handoff transfer pins the mounted NAS root for storage admission,
directory traversal and readback. Writes use the shared reservations and
recheck the R2-copy free-space floor. The handoff stops writing if the
mount or free-space check fails.

The NAS hashes a final file after copy or an existing file at the canonical
path. It checks the file is unchanged during readback, syncs the file and
directory, and submits the device, inode and timestamps together with the
exact SHA-256 and byte count. A file already acknowledged by ordinary
delivery is included in this readback. A conflicting final file is
preserved and reported. The server records the readback against the exact
approval and live catalog identity.

Once all 10,893 approved outputs have exact readbacks, rerun the preview
with `--start-final-recheck`. The NAS checks each canonical path again. A
missing file can be copied and a conflicting final file is preserved for
reconciliation. Receipts from preceding rounds remain as history.

When the final recheck completes, rerun the preview with
`--export-file-list delivery-nas-file-list.json`. The export requires the
complete approved roster and one mounted-root identity in the current
round. It matches each output to its former public file, playback,
download and ticket links, including aliases that share the same object.
URL variants with extra query parameters can be looked up by their
canonical public file pathname. It writes paths relative to the NAS share and
includes size and SHA-256. It refuses an incomplete readback set or a
changed public registry.

The controller publishes the file list on stoarama.com and retires the old
page. The manifest gives recipients file locations for their existing NAS
handoff. It contains no new hosting endpoint, signed R2 token, or absolute
server path. Until the controller publishes it, keep the old page and its
holds in place. Assembly retirement is handled separately by the
controller. Raw-source coverage and the existing retirement gates are
independent of these NAS delivery-copy receipts.
