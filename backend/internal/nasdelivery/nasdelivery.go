// Package nasdelivery holds the NAS delivery contract shared by the API and
// stoaramactl: which recording clips the NAS raw feed may hand out.
package nasdelivery

// Recording NAS delivery modes (recordings.nas_delivery_mode).
const (
	ModeRaw          = "raw"
	ModeCollatedOnly = "collated_only"
)

// HoldContractMode is the clip_storage_billing_contracts.mode frozen onto every
// clip a collated_only recording inserts. A held clip stays in managed staging
// (R2) until it is collated, is never offered to the NAS raw feed, never counts
// as NAS backlog, and is never metered as managed storage.
const HoldContractMode = "nas_collated_hold"

// ValidMode reports whether mode is a recording NAS delivery mode.
func ValidMode(mode string) bool { return mode == ModeRaw || mode == ModeCollatedOnly }

// notHeldPrefix begins a WHERE predicate that is true when a recording_clips row
// is not held for collated-only delivery. Every query that treats unreleased
// nas_pull clips as NAS work must include it; the constants below bind the
// clip alias so the predicate stays usable inside constant SQL.
const notHeldPrefix = "NOT EXISTS (SELECT 1 FROM clip_storage_billing_contracts nas_hold WHERE nas_hold.mode='" + HoldContractMode + "' AND nas_hold.clip_id="

// NotHeld predicates for the recording_clips aliases the NAS queries use.
const (
	NotHeldC         = notHeldPrefix + "c.id)"
	NotHeldRC        = notHeldPrefix + "rc.id)"
	NotHeldCandidate = notHeldPrefix + "candidate.id)"
)
