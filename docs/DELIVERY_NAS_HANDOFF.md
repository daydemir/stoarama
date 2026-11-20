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
same command with `--apply --approved-root-device DEVICE --approved-root-inode INODE`
to queue and enable only the approved roster on connection 13. The controller
takes that identity from a verified observation of the active `/clips` mount,
such as the latest confirmed connection-13 NAS cleanup proof. The roster pin
survives client restart and every verification round. The import is atomic and repeatable. The ordinary
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
`--request-final-check`. The verified NAS client drains
its writers and checks every approved path while holding the NAS client
process lock. Its existing service restart starts the normal writers again
after the check resolves. The finalizer
can also be run with `delivery-handoff-check` in the same NAS client environment
when the normal process is stopped.

The check binds current device, inode, size and timestamps to the complete
SHA-256 readback round. It holds a Linux read lease on each file, watches
the approved path components with inotify, and verifies the pinned mount.
The process lock stays held through the API's acceptance. If the response is
lost, the saved request blocks other client writers until the server confirms
acceptance, cancellation or supersession, and it checks for a successor
request before admitting writers. Use the matching client release throughout
recovery. Pausing the handoff cancels its unanswered final check and lets the
normal client restart.

The controller confirms that the NAS client owns mutations to the approved
paths and their ancestors. Other users and jobs can read those paths during
cutover. If a separate writer can change them, keep the cutover paused.
The client must be able to open and lease one file descriptor per approved
output.

Export with `--export-file-list delivery-nas-file-list.json` after this
check completes. Export requires the complete approved roster and one
mounted-root identity in the current verification round. If a file is
missing or has changed, start a new final recheck to repair and certify it.
If publication is delayed, request another metadata check before cutover.

The list includes former file, playback, download and ticket links,
including public aliases for the same object. URL variants with extra query
parameters can be looked up by the decoded ID in the public file or ticket path.
Paths are relative to the NAS share and include size and SHA-256. Export
refuses an incomplete verification or a changed public registry.

The controller publishes the file list on stoarama.com and retires the old
page. The manifest gives recipients file locations for their existing NAS
handoff. It contains no new hosting endpoint, signed R2 token, or absolute
server path. Until the controller publishes it, keep the old page and its
holds in place. Assembly retirement is handled separately by the
controller. The controller can use `delivery_nas_handoff_certified` to check the exact
copy identity. Raw-source coverage and the existing retirement gates are
independent of these NAS delivery-copy receipts.
