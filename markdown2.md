# StreamForge Ingest: Data Ingestion Technical Details

This document outlines the design, configuration, and operational practices for StreamForge Ingest, the StreamForge Platform’s data ingestion subsystem.

Useful links: [Home](https://example.com/streamforge) • [Connectors](https://example.com/streamforge/connectors) • [API Reference](https://example.com/streamforge/api) • [CLI](https://example.com/streamforge/cli)

## Overview

StreamForge Ingest moves data from external systems into your lakehouse/warehouse with end-to-end encryption and resilient delivery.

- Supports batch, micro-batch, and streaming in a single runtime
- At-least-once transport; idempotent sinks with merge-on-key
- Schema-aware with optional enforcement and evolution
- Pluggable transforms (built-in and user-defined)

## Architecture

High-level data path:

1. Source Connector (read, snapshot/CDC/file)
2. Transport Layer (Kafka, SQS, or in-memory for dev)
3. Processor (validation, transforms, enrichment)
4. Sink (object store, warehouse, or message bus)

Data flow: Source → Transport → Processor → Sink

Key properties:

- Backpressure-aware flow control
- Checkpointed progress for streaming
- Write-ahead log for exactly-once sinks

## Supported Sources and Sinks

Sources:

- Files: S3, GCS, Azure Blob, SFTP
- Databases (CDC & bulk): Postgres, MySQL, SQL Server
- Event streams: Kafka, Kinesis, Pub/Sub
- SaaS: Salesforce, Zendesk, Shopify (via OAuth2)

Sinks:

- Data lake: S3, ABFS, GCS (Parquet/ORC)
- Warehouses: BigQuery, Snowflake, Redshift
- Streams: Kafka, Kinesis (for fan-out)

Connector catalog: [Browse connectors](https://example.com/streamforge/connectors)

## Data Model and Semantics

- Delivery: at-least-once with idempotent upserts
- Ordering: best-effort per key partition; global ordering not guaranteed
- Schema: optional enforcement; evolve with additive fields; reject breaking changes by default
- Deduplication: sink-side merge keys and optional hash-based dedupe

## Authentication and Authorization

- Cloud: IAM roles (AWS), Workload Identity (GCP), MSI (Azure)
- SaaS: OAuth2 with PKCE
- Databases: user/password + TLS, mTLS supported
- API: PAT or OAuth2 client credentials
- RBAC: project, pipeline, and secret scopes

Example AWS trust policy for OIDC:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": { "Federated": "arn:aws:iam::123456789012:oidc-provider/idp.example.com" },
      "Action": "sts:AssumeRoleWithWebIdentity",
      "Condition": { "StringEquals": { "idp.example.com:sub": "streamforge-ingest" } }
    }
  ]
}
```

## Pipeline Configuration

Pipelines can be defined in YAML or via the REST API.

```yaml
# pipeline: orders_cdc_v2.yaml
name: orders_cdc_v2
source:
  type: postgres
  connection:
    host: pg.prod.internal
    port: 5432
    database: commerce
    user: ingest
    passwordRef: secret://kv/pg_pwd
  cdc:
    publication: ingest_pub
    slot: ingest_slot_v2
    snapshotMode: initial
transport:
  type: kafka
  topic: sf.orders.raw
  brokers: ["kafka-1:9092","kafka-2:9092"]
processor:
  schema:
    mode: enforce
    onViolation: send_to_dlq
  transforms:
    - type: drop_columns
      columns: ["_debug", "_tmp"]
    - type: cast
      columns:
        amount: "decimal(12,2)"
        created_at: "timestamp"
    - type: hash
      input: ["email"]
      output: "email_hash"
sink:
  type: lakehouse
  bucket: s3://streamforge-lake/orders/
  format: parquet
  mergeKey: ["order_id"]
  partitionBy: ["dt"]
scheduling:
  mode: streaming
  checkpointInterval: 20s
```

Create via API:

```bash
curl -sS -X POST https://api.example.com/streamforge/v1/pipelines \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/yaml" \
  --data-binary @orders_cdc_v2.yaml
```

## Transformations

Built-in transforms:

- drop_columns, cast, filter, flatten
- hash, tokenize, redact
- enrich (lookup against reference tables)
- udf (Python, JS via sandbox)

Python UDF example:

```python
# file: udf_order_enrich.py
def transform(rec: dict) -> dict:
    rec["dt"] = rec["created_at"][:10] if rec.get("created_at") else None
    amount = float(rec.get("amount") or 0)
    rec["amount_usd"] = convert_to_usd(amount, (rec.get("currency") or "USD").upper())
    rec["is_high_value"] = amount >= 1000.0
    return rec
```

Register UDF:

```bash
sfctl udfs create --name order_enrich --runtime python3.10 --file udf_order_enrich.py
```

## Streaming and Checkpointing

- Exactly-once for supported sinks (Snowflake/Delta) when WAL + idempotent merge keys are enabled
- Checkpoint backends: S3, GCS, Azure Blob
- Offsets stored with atomic commits to prevent duplication

Tuning tips:

1. Align checkpointInterval with sink commit latency
2. Use partitioning keys with high cardinality for hot shards
3. Enable compaction for compactable topics or small files

## Error Handling and DLQ

- Retries: exponential backoff with jitter
- DLQ: per-pipeline topic with payload + error metadata
- Poison records: quarantined; pipeline continues

Example policy:

```yaml
processor:
  retry:
    maxAttempts: 7
    backoff: 250ms
    maxBackoff: 45s
    jitter: full
dlq:
  topic: sf.orders.dlq
  retention: 21d
```

## Observability

Metrics (Prometheus):

- streamforge_ingest_records_total
- streamforge_ingest_lag_seconds
- streamforge_processor_failures_total
- streamforge_sink_commits_total
- streamforge_dlq_records_total

Tracing: [OpenTelemetry](https://opentelemetry.io/) spans across source→transport→processor→sink.

Dashboards: [Grafana package](https://example.com/streamforge/grafana)

Structured logs include request_id, pipeline_id, offset, and sink_txn_id.

## Performance and Scaling

- Concurrency is controlled via readers, processors, and sink writers
- Auto-scaling based on lag and CPU utilization
- File sizing targets 128–512 MB Parquet blocks by default

To scale a streaming pipeline:

1. Increase source reader parallelism
2. Increase partition count (Kafka) or shards (Kinesis)
3. Tune batch size and linger for sink writers
4. Verify warehouse merge performance and clustering

Example:

```bash
sfctl pipelines scale orders_cdc_v2 \
  --readers 6 --processors 8 --writers 6 \
  --target-throughput-mbps 80
```

## Security

- TLS 1.2+ for all connectors
- At-rest encryption via KMS/CMK
- Secrets isolated in Vault-compatible backend
- Row/column-level controls via RBAC and policies
- PII protection via hash/tokenize/redact transforms

Security guide: [Hardening StreamForge](https://example.com/streamforge/security)

## Limits and Quotas

Defaults:

- Max pipelines per project: 250
- Max throughput per pipeline: ~75 MB/s (soft)
- Max event size: 1 MB (transport-limited)
- Max checkpoint size: 10 MB per partition

Request increases: [Support](https://example.com/support)

## Scheduling and Backfills

- Cron-style batch schedules
- Micro-batch windows by time or size
- Historical backfills by time range and predicate

Backfill example:

```bash
sfctl backfills start --pipeline orders_cdc_v2 \
  --from 2025-01-01T00:00:00Z \
  --to   2025-01-07T23:59:59Z \
  --filter 'status IN ("PAID","SHIPPED")'
```

## Local Development Quickstart

```bash
git clone https://example.com/streamforge/examples.git
cd examples/quickstart
docker compose up -d
sfctl pipelines create -f samples/pg_to_lake.yaml
sfctl pipelines logs orders_cdc_v2 -f
```

## Troubleshooting

Common issues:

- High lag: increase parallelism; check source partitioning
- Small files: raise batch size; enable file compaction
- Warehouse merge slow: add clustering keys; optimize dedupe
- Authentication failures: verify token/role mapping and clock skew

Diagnostic commands:

```bash
sfctl pipelines describe orders_cdc_v2
sfctl pipelines metrics orders_cdc_v2 --since 1h
sfctl dlq tail sf.orders.dlq --since 10m --pretty
```

Sample DLQ record:

```json
{
  "pipeline_id": "orders_cdc_v2",
  "error": "validation_failed: missing field 'order_id'",
  "payload": {"order_id": null, "amount": "19.99", "created_at": "2025-01-05T10:20:30Z"},
  "offset": {"topic": "sf.orders.raw", "partition": 3, "offset": 982114},
  "timestamp": "2025-01-05T10:21:03Z"
}
```

## FAQ

Q: How do I get near exactly-once semantics?  
A: Use merge keys on the sink, enable WAL, and ensure idempotent upserts are supported by your destination.

Q: Can I run StreamForge on-premises?  
A: Yes, via Helm with optional air-gapped images. See [Helm chart](https://example.com/streamforge/helm).

Q: How do I evolve schemas safely?  
A: Enable schema enforcement and allow additive fields; use canary pipelines for breaking changes.

Q: How do I pause/resume pipelines?  
A: Use the CLI or API: `sfctl pipelines pause <name>` and `sfctl pipelines resume <name>`.

Full documentation: [https://example.com/streamforge/docs](https://example.com/streamforge/docs)