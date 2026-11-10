-- Optional NAS cleanup status reported by the app in its heartbeat (replay
-- failures, quarantined journal entries, batch-size fallbacks, last lease).
-- Diagnostics only; no authority. Nullable without default: metadata-only.
ALTER TABLE connections
 ADD COLUMN nas_cleanup_status jsonb,
 ADD COLUMN nas_cleanup_status_at timestamptz;

-- Redacted recent log lines uploaded by the NAS app (owner-approved remote
-- diagnostics, 2026-11). Append-only (UPDATE refused); the API deletes rows
-- older than 7 days and keeps at most 2000 uploads per connection.
CREATE TABLE nas_client_logs (
 id bigserial PRIMARY KEY,
 connection_id bigint NOT NULL REFERENCES connections(id),
 received_at timestamptz NOT NULL DEFAULT now(),
 client_version text NOT NULL CHECK (length(client_version) BETWEEN 1 AND 64),
 boot_id text NOT NULL CHECK (length(boot_id) <= 64),
 journal_entries integer,
 quarantined integer,
 dropped_lines integer NOT NULL DEFAULT 0,
 lines jsonb NOT NULL CHECK (jsonb_typeof(lines) = 'array')
);
CREATE INDEX nas_client_logs_connection_idx ON nas_client_logs (connection_id, received_at DESC);
CREATE FUNCTION guard_nas_client_logs() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
 RAISE EXCEPTION 'nas client logs are append-only';
END $$;
CREATE TRIGGER nas_client_logs_append_only BEFORE UPDATE ON nas_client_logs FOR EACH ROW EXECUTE FUNCTION guard_nas_client_logs();
