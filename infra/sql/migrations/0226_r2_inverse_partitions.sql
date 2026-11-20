-- The latest sealed full-bucket inverse partition of each bucket endpoint, so a
-- restarted r2-inverse job reuses it instead of relisting the whole bucket.
-- It lives here, not in R2: while the inverse lanes run, the bucket's writer
-- fence refuses every new R2 write. A stored partition only proposes
-- candidates; every reservation and DELETE still checks its key live.
CREATE TABLE r2_inverse_partitions (
 bucket text NOT NULL CHECK(bucket<>''),
 endpoint_sha256 text NOT NULL CHECK(endpoint_sha256 ~ '^[0-9a-f]{64}$'),
 partition_sha256 text NOT NULL CHECK(partition_sha256 ~ '^[0-9a-f]{64}$'),
 summary bytea NOT NULL,
 partition_gzip bytea NOT NULL,
 PRIMARY KEY(bucket,endpoint_sha256)
);
-- Already gzip-compressed: skip TOAST's second compression pass.
ALTER TABLE r2_inverse_partitions ALTER COLUMN partition_gzip SET STORAGE EXTERNAL;
