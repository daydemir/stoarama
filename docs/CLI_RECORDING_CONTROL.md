# Recording controls through stoaramactl

The account pause API is exposed by stoaramactl recordings pause. It retains
existing clips and outputs, clears future admissions, and cancels pending/leased
capture jobs through the application transaction.

Provide an already-enrolled account credential through API_TOKEN and the
configured BACKEND_API_URL. The CLI first verifies /api/v1/account/me matches
the required account and validates every selected recording. A NAS pull key is
not a recording-control credential; a denial stops the operation without retries,
credential changes, alternate hosts, or database writes.

From backend, review the exact account and recording selection:

    go run ./cmd/stoaramactl recordings pause --account-id 47 --recording-ids IDS --youtube-only

Add --apply to execute the same reviewed selection. --youtube-only rejects
any selected binding outside the public YouTube URL domains. The JSON report
lists completed and already-paused IDs, including partial progress if a later
request fails. Success verifies every selected recording is paused and its
next_fire_at is null. The CLI never deletes media or modifies other recordings.

The public stoarama auth configure command configures its own profile; it is
not required when the intended operator consumer already supplies API_TOKEN.
No login, key minting, or scope change is part of pausing recordings.
