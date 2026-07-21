CREATE TABLE relay_groups (
  id BIGSERIAL PRIMARY KEY,
  account_id BIGINT NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
  name TEXT NOT NULL CHECK (btrim(name) <> ''),
  max_streams INTEGER NOT NULL DEFAULT 8 CHECK (max_streams BETWEEN 1 AND 200),
  UNIQUE (account_id, id)
);

CREATE UNIQUE INDEX relay_groups_account_name_unique
  ON relay_groups (account_id, lower(name));

ALTER TABLE nodes
  ADD COLUMN relay_group_id BIGINT,
  ADD CONSTRAINT nodes_relay_group_account_fk
    FOREIGN KEY (account_id, relay_group_id)
    REFERENCES relay_groups (account_id, id);

CREATE INDEX nodes_relay_group_id_idx
  ON nodes (relay_group_id)
  WHERE relay_group_id IS NOT NULL;
