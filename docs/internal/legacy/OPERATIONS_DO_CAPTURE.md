# DO Capture Operations

## Status Checks

```bash
cd backend
source ../local/operator.env

go run ./cmd/stoaramactl overview status --backend-api-url "$BACKEND_API_URL" --api-token "$API_TOKEN"
go run ./cmd/stoaramactl servers list --backend-api-url "$BACKEND_API_URL" --api-token "$API_TOKEN" --hours 168
go run ./cmd/stoaramactl servers capacity list --backend-api-url "$BACKEND_API_URL" --api-token "$API_TOKEN"
go run ./cmd/stoaramactl servers assignments --backend-api-url "$BACKEND_API_URL" --api-token "$API_TOKEN"
```

## Assign / Unassign Streams

```bash
cd backend
source ../local/operator.env

go run ./cmd/stoaramactl servers assign \
  --id <stream_id> \
  --server-id si-capture-01 \
  --reason "operator assign" \
  --actor "ops.do"

go run ./cmd/stoaramactl servers unassign \
  --id <stream_id> \
  --yes \
  --reason "operator stop" \
  --actor "ops.do"
```

## Drain an Execution Class on One Server

```bash
cd backend
source ../local/operator.env

go run ./cmd/stoaramactl servers capacity heartbeat \
  --server-id si-capture-01 \
  --capture-shared-capacity 6 \
  --draining-execution-classes video_live \
  --lease-sec 45
```

## Service-Level Debug on Droplet

```bash
ssh root@<droplet_ip>
systemctl status stoarama-capture.service --no-pager
journalctl -u stoarama-capture.service -n 200 --no-pager
```

## Safe Scale Out

1. Increase `droplet_count` in `infra/do-capture`.
2. `terraform apply`.
3. Wait for new servers to appear in `servers list`.
4. Assign additional streams to new `server_id`s.

## Safe Scale In

1. Unassign streams from target server(s).
2. Confirm no active assignments remain.
3. Reduce `droplet_count`.
4. `terraform apply`.
