# IMPORT INTO Full and Delta Upsert (DXF + S3)

- Author(s): Codex
- Discussion PR: TBD
- Tracking Issue: TBD

## Table of Contents

* [Introduction](#introduction)
* [Motivation or Background](#motivation-or-background)
* [Detailed Design](#detailed-design)
* [Test Design](#test-design)
    * [Functional Tests](#functional-tests)
    * [Scenario Tests](#scenario-tests)
    * [Compatibility Tests](#compatibility-tests)
    * [Benchmark Tests](#benchmark-tests)
* [Impacts & Risks](#impacts--risks)
* [Investigation & Alternatives](#investigation--alternatives)
* [Unresolved Questions](#unresolved-questions)

## Introduction

This document proposes incremental upsert on top of TiDB IMPORT INTO + DXF + Lightning global sort and ingest. The solution imports TSV or CSV from object storage, maintains a base version manifest on S3, merges base and delta by region, and automatically ingests only changed regions for delta imports. It also provides SHOW IMPORT METERING and SHOW IMPORT COST for auditing resource usage and cost.

## Motivation or Background

The primary workload is a wide table (including JSON columns) with a composite primary key (user ID + device ID). Daily delta data from multiple systems updates only a subset of columns. Requirements:

- Eventual consistency is acceptable, batch import is allowed.
- Delta import must upsert against the previous day base (sparse column updates).
- Cost sensitive; the import pipeline must be observable and estimable.
- No manual operations or extra commands; delta import should automatically ingest only changed ranges.

Current IMPORT INTO resolves conflicts by deleting conflicting KV, which cannot implement sparse column updates. A new delta pipeline and metadata system are required.

## Detailed Design

### User Syntax and Options

Reuse IMPORT INTO WITH option parsing and add:

- WITH full: keep sorted KV as base and generate base manifest.
- WITH delta: upsert against base, generate a new base, and auto-ingest changed regions. If base_version is missing, derive the base from current TiKV metadata (region ranges and SST metadata) and scan overlaps via remote cop; fail fast if it cannot be resolved (no backup cluster dependency). base_version is optional: it can be explicit via base_version/base_uri or auto-derived; run full once after a delta to pin a new base.
- base_version='v...': optional baseline version.
- base_uri='s3://...': optional baseline path.
- merge_strategy='last_write_wins'|'max_ts': resolve duplicate primary keys within a delta batch.
- base_merge_mode='reference': reuse files for unchanged regions during delta merge.
- merge_executor='tidb'|'tikv-worker': location of region merge execution (default tidb; tikv-worker is remote merge).

System variables:
- Remove apply_mode/apply_window/apply_rate_limit. Apply is immediate by default and controlled only by SLO guard.
- SLO guard config: mysql.tidb_import_slo_guard (JSON config) + ALTER/SHOW IMPORT SLO GUARD (per job_id or global job_id=0).

SLO guard statements:
- ALTER IMPORT SLO GUARD [JOB <id>] WITH config='{"enable":true,"pause_threshold":"10ms","slow_apply_rate_limit_mb_per_sec":64}'
- SHOW IMPORT SLO GUARD [JOB <id>]

Constraints:

- full and delta require global sort (CloudStorageURI must be set).
- If base_version is missing, WITH delta derives base from TiKV metadata; fail if unresolved.
- merge_executor=tikv-worker requires a base manifest; no fallback without a base manifest.

### Pipeline Design

#### Full mode

Reuse the existing global sort pipeline and add base manifest generation in PostProcess.

```
StepInit
  -> EncodeAndSort
  -> MergeSort
  -> WriteAndIngest
  -> CollectConflicts (optional)
  -> ConflictResolution (optional)
  -> PostProcess (write base manifest)
  -> StepDone
```

#### Delta mode

Introduce a delta pipeline. After RegionMerge, ingest changed regions immediately by default; if SLO guard triggers slow or pause, Apply is throttled or paused and then resumed:

```
StepInit
  -> DeltaEncodeAndSort
  -> PlanTouchedRegions
  -> RegionMergeAndRebuild
  -> IngestChangedRegions
  -> PostProcess
  -> StepDone
```

If SLO guard is disabled, Apply runs immediately without time windows or manual delay.

### SLO Guard (dynamic slow or pause by p99)

- Background SLO guard worker queries metrics_schema.tidb_query_duration with sql_type=Select p99 (5m window, 30s interval).
- State machine:
  - p99 >= slow (0.8 * pause_threshold) -> slow state (Apply rate limit override)
  - p99 >= pause_threshold -> PauseTask to pause import and preheat
  - p99 <= resume (0.6 * pause_threshold) -> ResumeTask
- Config via ALTER IMPORT SLO GUARD [JOB <id>] WITH config='{"enable":true,"pause_threshold":"10ms","slow_apply_rate_limit_mb_per_sec":64}'.
- Precedence: job-level config first, fallback to global job_id=0.
- If JOB <id> omitted, global config (job_id=0). enable=false disables global guard.

### Preheat Call Path

- Entry: executor/import_into.go -> importinto/SubmitTask -> importScheduler.GetNextStep
- PlanTouchedRegions: plan_touched_regions.go reads base manifest (ReadBaseManifest) or TiKV metadata (resolveTableRecordRanges) and loads delta KV meta (getSortedKVMetasOfEncodeStep)
- RegionMerge: NewRegionMergeStepExecutor.Init initializes TableImporter and encoder, then builds per-region iterators
  - S3 path: external.NewMergeKVIter prefetches object metadata and first KV batch
  - remote cop path: remote_base_iter.go builds DAG via buildBaseScanDAG and triggers distsql.Select to warm RegionCache
- IngestChangedRegions: generateIngestChangedRegionsSpecs pre-parses base manifest and changed regions to extract ranges and file lists
- Apply runs immediately by default and is controlled only by SLO guard

### Delta Row Encoding (sparse update)

Delta data may include only a subset of columns. To avoid overwriting missing columns, add a column bitmap to the data KV value:

- Write: prepend varint(bitmap_len) + bitmap bytes to data KV value.
- Merge read: apply delta only to columns marked in the bitmap; keep base values for others.

This encoding affects delta mode only and does not change full or default imports.

### Multi-source Conflict Merge

If multiple sources produce the same primary key within a batch, the merge order must be deterministic and traceable:

- Sort key: (primary_key, merge_ts, source_priority, file_order, row_offset)
- merge_ts from business version or time column; if missing or equal, use file_order + row_offset as a stable tie-breaker
- Merge multiple rows into one deterministic row across retries and replays

### RegionMerge Algorithm

For each changed region, read base and delta data KV, merge rows, and rebuild data and index KV. With a base manifest, use external.NewMergeKVIter to merge sorted KV on S3. Without a base manifest, build a runtime base view from TiKV metadata (region ranges + SST metadata), scan overlaps via remote cop, and merge (higher RPC and decode cost, fallback only).

- Unchanged regions: base_merge_mode controls reuse (reference).
- merge_executor:
  - tidb: default, supports base manifest and fallback.
  - tikv-worker: remote merge via /merge endpoint; requires base manifest.
- tikv-worker limits: no generated columns, no prefix index, no multi-valued or vector index, no common_handle_version > 1; index columns must be binary or numeric (no restored data).

Steps:
1. Merge by primary key order.
2. If base row exists, overwrite only columns present in delta.
3. If base row is missing, insert with defaults and generated columns.
4. Resolve multiple delta rows by merge_strategy.
5. Output new base data and index KV and index stats files.

### Changed Regions Planning and Auto Ingest

PlanTouchedRegions intersects delta KV key ranges with base region boundaries and outputs changed_regions.json. If base manifest is missing, derive region ranges from TiKV metadata and build changed ranges by overlap (no full-range fallback).

IngestChangedRegions reads changed_regions.json and the new base manifest, then ingests only changed region data and index KV. This step is automatic and does not require manual ADMIN INGEST commands.

### Manifest and Metadata

All detailed metadata lives in object storage; the database stores only pointers (job summary and parameters).

If base_version is missing and no base manifest exists, build a runtime base view from current TiKV metadata:

- Read region ranges and SST key bounds (smallest and biggest) for the target table.
- Keep only ranges overlapping delta key ranges, then build a snapshot via remote cop and scan.
- Assemble a runtime base view (optionally persisted) for subsequent merge.

Base Manifest example:

```json
{
  "table_id": 123,
  "base_id": "v20260130",
  "base_uri": "s3://bucket/db/table/base/v20260130",
  "create_time": "2026-01-30T12:00:00Z",
  "row_count": 1000000,
  "data_bytes": 123456789,
  "index_bytes": 12345678,
  "regions": [
    {
      "start_key": "...",
      "end_key": "...",
      "data_files": ["..."],
      "stat_files": ["..."],
      "index_files": ["..."],
      "kv_bytes": 1234
    }
  ]
}
```

Changed Regions Manifest example:

```json
{
  "job_id": 10001,
  "base_id": "v20260130",
  "regions": [
    {
      "start_key": "...",
      "end_key": "...",
      "changed_rows": 100,
      "kv_bytes": 456
    }
  ]
}
```

### SHOW Query Interfaces

- SHOW IMPORT BASES: list base versions and summary.
- SHOW IMPORT REGIONS BASE <id>: read base manifest and output region details.
- SHOW IMPORT CHANGED REGIONS JOB <job_id>: read changed regions manifest.
- SHOW IMPORT METERING JOB <job_id>: output raw metering data by time bucket.
- SHOW IMPORT COST JOB <job_id>: compute and summarize cost from metering.

### Metering and Cost Model

Metering data comes from DXF metering files in object storage. Filter by job ID and output:

- object store requests: GET and PUT
- object store bytes read and written
- cluster bytes read and written
- row_count, data and index KV bytes, required slots, duration, etc

Default price parameters:

- GET: $0.0004 / 1000
- PUT: $0.005 / 1000
- object store or cluster byte cost defaults to 0 (reserved for extension)
- S3 Standard storage tiers (US East, N. Virginia; 2026-01-30): 0-50TB $0.023 per GB-month, 50-500TB $0.022 per GB-month, >500TB $0.021 per GB-month (refresh via Price List API)
- In-region S3 to compute is free; cross-region or public egress is extra

Metering aggregation granularity follows the metering flush interval (currently per minute).

### 175 TB Storage Cost Example (S3 Standard, US East)

- Single base (175 TB ~= 175,000 GB): about $3,900 per month (tiered pricing)
- Base + daily delta (175 TB): about $7,750 per month
- 10-day delta retention (1,750 TB), total 1,925 TB: about $40,975 per month

### Node Sizing and Throughput (i8g.4xlarge)

Assume AWS peak network bandwidth and 40% to 60% effective throughput:
- i8g.4xlarge (16 vCPU, 128 GiB): 25 Gbps -> 4.5 to 6.8 TB/h per node

Target throughput to node count:
| Target | Required Throughput | i8g.4xlarge Nodes |
| --- | ---: | ---: |
| 35 TB / 2h | 17.5 TB/h | 3 to 4 |
| 175 TB / 2h | 87.5 TB/h | 13 to 20 |
| upsert (r=70%) | 201 TB/h | 30 to 45 |

Recommended roles:
- Control plane (TiDB 8c16G): fixed 3 nodes (HA)
- Compute plane (TiDB 16c32G): about 1 to 2x the remote-cop node count
- Remote cop (tikv-worker i8g.4xlarge): local 3.75TB NVMe, 25 Gbps network

### Spot and Savings Plans (cost reduction)
- Spot: lowest cost but interruptible; suitable for DXF and remote-cop batch workloads.
- Savings Plans: commit per hour for 1 or 3 years; suitable for control plane and steady compute nodes.

### Privileges and Security

Reuse IMPORT INTO privilege checks; SHOW IMPORT * is gated by job owner or SUPER.

### Compatibility and Rollback

- No change to default IMPORT INTO behavior.
- Delta logic only when WITH full or delta is used.
- On failure, do not replace the active base; keep the baseline unchanged.

### Version Retention and Periodic Compaction (background DXF task)

To support audit and rollback, retain multiple days of base and delta versions. After retention expires, run a background DXF compaction task:

- Retention window: default 10 days; configurable via tidb_import_into_base_retention_days.
- Trigger: DDL owner periodically scans import jobs and base manifests (create_time) and triggers compaction for expired bases; running imports are skipped.
- Input: latest base manifest (rewrite source); old versions only for validation and rollback.
- Output: new compacted base manifest (merge ranges and reduce small files), written as a new base version.
- Path: PlanTouchedRegions (ForceAllRegions) generates full range -> RegionMergeAndRebuild rewrites base; skip DeltaEncodeAndSort and IngestChangedRegions.
- Constraint: compaction requires base manifest; fail if missing.
- Consistency: keep old versions until compaction finishes; verify checksum, row_count, and bytes before GC.
- GC policy: only delete expired base versions and orphan objects if the latest base is a compaction product.

Storage estimate: Total ~= Base + RetentionDays * DailyChanged (changed-region files only, not temporary files).
Example (10 days, DailyChanged=175TB): Total ~= 175 + 10*175 = 1,925 TB.
This controls storage cost and avoids long-term merge fragmentation.

## Test Design

### Functional Tests

- Parse and validate WITH full/delta; delta without base_version derives base from TiKV metadata.
- Base and changed regions manifest read/write and correctness.
- Delta pipeline step order and auto ingest.
- SHOW IMPORT BASES/REGIONS/CHANGED output validation.
- SHOW IMPORT METERING/COST aggregation and field mapping.

### Scenario Tests

- Sparse updates on wide tables are correct.
- Duplicate primary keys within delta resolve by merge_strategy.
- Delta without base_version derives base from TiKV metadata; fail if unresolved.
- Retries do not break base version consistency.

### Compatibility Tests

- Compatible with existing IMPORT INTO (no full/delta).
- Consistency between global sort and local sort behaviors.
- Privilege and audit logic unchanged.

### Benchmark Tests

- Compare throughput against existing IMPORT INTO.
- Measure impact of ingesting only changed regions on TiKV write and compaction.

## Impacts & Risks

### Impacts

- Ingest only changed regions, significantly reducing unnecessary writes and compaction cost.
- Metering and cost queries provide observability and cost tracking.

### Risks

- RegionMerge requires decode and rebuild of data and index KV, high CPU and IO load.
- Sparse row encoding and merge logic is complex and needs thorough tests.
- If metering storage is missing or misconfigured, SHOW IMPORT METERING/COST may be unavailable.

## Investigation & Alternatives

- Row-level upsert inside TiKV would introduce heavy random writes and low throughput.
- Manual commands to ingest changed regions violate automation requirements and are replaced by automatic delta ingest.

## Unresolved Questions

- Should the cost model be a configurable price list (multi-cloud and multi-region)?
- Should base retention policy (how many versions and cleanup strategy) be standardized further?
