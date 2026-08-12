CREATE TABLE recording_health_digest_deliveries (
  bucket_start_at TIMESTAMPTZ NOT NULL,
  recipient TEXT NOT NULL,
  attempted_at TIMESTAMPTZ,
  delivered_at TIMESTAMPTZ,
  PRIMARY KEY (bucket_start_at, recipient)
);
