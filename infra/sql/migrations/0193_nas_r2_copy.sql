-- Copy queue only. No delivery/retirement/cleanup authority and no activation.
CREATE TABLE nas_r2_copy_items (
 id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
 connection_id bigint NOT NULL REFERENCES connections(id),
 kind text NOT NULL CHECK(kind IN ('raw','collated')),
 source_id bigint NOT NULL CHECK(source_id>0),
 manifest_sha256 text NOT NULL CHECK(manifest_sha256 ~ '^[a-f0-9]{64}$'),
 policy_sha256 text NOT NULL CHECK(policy_sha256 ~ '^[a-f0-9]{64}$'),
 item jsonb NOT NULL,
 state text NOT NULL DEFAULT 'pending' CHECK(state IN ('pending','leased','verified','blocked')),
 lease_token text, range_until timestamptz, range_id text, lease_expires_at timestamptz, etag text, version_id text,
 attempts integer NOT NULL DEFAULT 0,
 last_error text NOT NULL DEFAULT '', verified_at timestamptz,
 created_at timestamptz NOT NULL DEFAULT now(),
 UNIQUE(connection_id,kind,source_id)
);
CREATE INDEX nas_r2_copy_pending ON nas_r2_copy_items(connection_id,id) WHERE state IN ('pending','leased');
-- Admission uses a connection-wide advisory lock; these durable daily totals
-- charge every range grant, including abandoned grants and failed downloads.
CREATE TABLE nas_r2_copy_budgets (
 connection_id bigint NOT NULL REFERENCES connections(id), day date NOT NULL,
 charged_bytes bigint NOT NULL CHECK(charged_bytes BETWEEN 0 AND 100000000000),
 PRIMARY KEY(connection_id,day)
);
