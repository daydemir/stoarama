BEGIN;

-- A plaza number belongs to one catalog stream within one organization. The
-- API allocates the next number while holding an organization-scoped advisory
-- transaction lock, so concurrent recording creation cannot reuse a number.
CREATE TABLE account_stream_plaza_ids (
  account_id BIGINT NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
  stream_id BIGINT NOT NULL REFERENCES streams(id) ON DELETE CASCADE,
  plaza_id BIGINT NOT NULL CHECK (plaza_id > 0),
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (account_id, stream_id),
  UNIQUE (account_id, plaza_id)
);

COMMIT;
