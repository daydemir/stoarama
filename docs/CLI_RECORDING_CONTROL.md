# Recording controls through stoaramactl

The account pause API is exposed by stoaramactl recordings pause. It retains
existing clips and outputs, clears future admissions, and cancels pending/leased
capture jobs through the application transaction.

Both --account-id and an exact --recording-ids cohort are required (at most 50
distinct positive IDs). The default is a read-only dry run. Every target is
validated before the first pause. Add --apply to execute the reviewed cohort.

From backend:

    go run ./cmd/stoaramactl recordings pause --account-id 47 --recording-ids IDS --youtube-only

--youtube-only requires each selected binding to be a public YouTube URL.
The report identifies the authenticated account, auth_type and successful
preflight. It lists attempted, confirmed paused and already-paused IDs.
Pauses execute sequentially; an error stops the remaining requests. An attempted
ID without a confirmed response may need a status read before another operation.
Success re-reads the listing and verifies every selected recording is paused
with next_fire_at null. JSON is the default; --json=false prints a short summary.

## Intended operator consumer on Jelly

For MIT SCL / account 47, the documented existing operator consumer is
~/Build/stoarama/local/mitscl.env. Its MITSCL_API_TOKEN is the enrolled account
operator token; BACKEND_API_URL selects the configured API. Load that existing
consumer and map its token to the internal CLI's API_TOKEN variable:

    set -a
    . "$HOME/Build/stoarama/local/mitscl.env"
    set +a
    export API_TOKEN="$MITSCL_API_TOKEN"

Keep credential values local: do not print, copy, commit or mint them as part of
a pause. The generic recording-supervisor.env and youtube-relay-source.env
consumers are not substitutes for the account-47 operator context.

Verify the intended consumer's account and enrolled scope metadata, then run the
explicit account/cohort dry run. Do not infer a NAS scope from an HTTP 403 or a
read-only permission from the historical stoarama.read scope name. The browser
capabilities.can_toggle_recording flag describes browser UI capabilities; the
account API's authorization remains authoritative for CLI actions. A NAS pull
key is confined to NAS endpoints and cannot perform recording controls.

The command stops on denied requests, does not follow redirects, and requires
HTTPS except for a loopback API. It never retries with another credential, widens
a grant, edits a profile, writes the database, or deletes media. No login, key
minting or scope change is required when the intended operator is already enrolled.
