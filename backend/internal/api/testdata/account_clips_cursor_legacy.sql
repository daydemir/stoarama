
	SELECT c.id, c.recording_id, c.size_bytes, c.sha256, c.clip_start_at, c.clip_end_at, c.display_path,
	       c.recording_job_id, c.capture_lease_token, c.capture_sequence,
	       c.capture_attempt_id,c.timestamp_contract_version,c.timestamp_contract,c.timestamp_contract_status,c.timestamp_contract_reason
	FROM recording_clips c
	JOIN recordings r ON r.id = c.recording_id
	WHERE r.account_id = $1 AND c.purged_at IS NULL AND c.released_at IS NULL
	  AND r.delivery = 'nas_pull'
	  AND nas_delivery_allowed(CASE WHEN r.account_id=47 THEN 13 ELSE 0 END,'raw',c.id)
	  -- Retain zero-byte rows for audit, but never let one poison the forward
	  -- cursor page and block every later valid clip.
	  AND c.size_bytes > 0
	  -- Clips of collated_only recordings stay in R2 until collated; the NAS
	  -- receives them only as joined hours.
	  AND NOT EXISTS (SELECT 1 FROM clip_storage_billing_contracts nas_hold WHERE nas_hold.mode='nas_collated_hold' AND nas_hold.clip_id=c.id)
	  AND c.created_at < now() - interval '90 seconds'
	  AND c.id > $2
	ORDER BY c.id ASC
	LIMIT $3
