BEGIN;

CREATE TABLE IF NOT EXISTS account_stream_notes (
    account_id BIGINT NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
    stream_id BIGINT NOT NULL REFERENCES streams(id) ON DELETE CASCADE,
    note TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (account_id, stream_id)
);

COMMIT;
