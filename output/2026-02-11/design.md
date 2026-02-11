# IMPORT INTO Full/Delta Upsert Design (Replanned on 2026-02-11)

## 1. Background and Goals

### 1.1 Business background (from `project1.md`)
- The target table is wide (1KB~14KB per row) and includes flexible JSON columns.
- Primary key is `(user_id, device_id)`. Daily updates come from 6 upstream systems and usually touch only partial columns.
- Upsert must be performed on top of the previous-day baseline, with eventual consistency accepted.
- Input files are TSV/CSV on S3 and execution must be distributed via DXF, with region-level merge semantics.

### 1.2 Constraints and additional requirements (from `merge.md`)
- Capacity baseline is unified as **DailyChanged=175TB**.
- Cost target is <= 500K USD/month and must be explainable for non-technical readers.
- `delta` must still run when `base_version` is missing (safety-first fallback).
- Base retention and compaction are required (default 10 days, configurable).
- Outputs must be auditable: design, detailed design, task split, and code mapping.

### 1.3 Goals for this replanning round
- Preserve the existing IMPORT INTO main path while completing full baseline + delta upsert + region rebuild + changed-region ingest.
- Make data structures and interfaces explicit so implementation is evolvable, observable, and rollback-safe.
- Keep changes minimally invasive and aligned with existing TiDB/DXF/Lightning responsibilities.

---

## 2. Design Approach and Rationale

### 2.1 Mode layering
- **normal mode**: keep current IMPORT INTO behavior.
- **full mode**: run a full import and persist reusable base metadata (manifest).
- **delta mode**: sparsely encode incoming updates, merge with base by region, produce a new base, and ingest only changed regions.

### 2.2 Why region-level granularity
- Region is the natural partition unit in TiKV and aligns with ingest boundaries.
- Rebuilding only changed regions significantly reduces write amplification and compaction pressure.
- Under 175TB daily change, region-level execution improves throughput control, recovery, and resource throttling.

### 2.3 Why manifest-driven execution
- Manifest decouples logical versioning from physical file layout, improving auditability and recovery.
- In DXF, manifest is the minimal sufficient contract across steps (base_id, key ranges, file lists).
- With base manifest present, changed-region planning is precise; without it, fallback is still safe.

### 2.4 Why keep a fallback path
- In production, base manifests may be missing due to migration/history inconsistencies.
- `delta` must remain executable; fallback prioritizes correctness and prevents missed updates.
- Later full/compaction runs can move back to the optimized precise path.

---

## 3. Architecture and Flow

### 3.1 Full flow (baseline creation)
1. `EncodeAndSort` generates external KV files.
2. `MergeSort` / `WriteAndIngest` finishes import.
3. `PostProcess` writes `BaseManifest` (base_id, regions, file lists, stats).

### 3.2 Delta flow (incremental upsert)
1. `DeltaEncodeAndSort`: sparse column encoding to delta KV.
2. `PlanTouchedRegions`: compute changed regions from delta ranges and base manifest.
3. `RegionMergeAndRebuild`: merge base+delta and rebuild KV by changed region.
4. `IngestChangedRegions`: ingest changed regions only.
5. `PostProcess`: update job summary and base metadata.

### 3.3 Runtime protection
- SLO guard reads policy from `mysql.tidb_import_slo_guard`:
  - enters slow/pause when read p99 approaches threshold, and resumes after recovery.
- Compaction worker periodically checks retention and triggers compaction + old version cleanup.

---

## 4. Core Data Structure Design

### 4.1 Base Manifest
- `BaseID`: logical baseline version.
- `BaseURI`: object-storage root for the base.
- `Regions[]`: region key ranges with data/index file references.
- `TotalRows/DataBytes/IndexBytes`: capacity and cost estimation inputs.
- `CreateTime`: lifecycle management anchor.

### 4.2 Changed Regions Manifest
- `JobID`: delta job identifier.
- `BaseID`: baseline used by this delta run.
- `Regions[]`: changed key-range set (hex ranges).

### 4.3 Delta row encoding structure
- Column bitmap: tracks which columns are present in delta.
- Row payload: encodes only present columns; missing columns keep base values during merge.
- Value: supports partial-column updates without accidental overwrite.

### 4.4 Job Summary extensions
- `BaseID/BaseURI/BaseManifestPath`
- `ChangedRegionsPath/DeltaURI`
- `ImportedRows/ImportedBytes`
- Metering aggregates (object-store requests, bytes, window)

---

## 5. Interface Design

### 5.1 SQL interfaces
- `IMPORT INTO ... WITH full`
- `IMPORT INTO ... WITH delta`
- `SHOW IMPORT BASES`
- `SHOW IMPORT REGIONS BASE <base_id>`
- `SHOW IMPORT CHANGED REGIONS JOB <job_id>`
- `SHOW IMPORT COST JOB <job_id>`
- `SHOW IMPORT METERING JOB <job_id>`
- `ALTER/SHOW IMPORT SLO GUARD ...`

### 5.2 Execution interfaces (DXF step executors)
- `NewPlanTouchedRegionsStepExecutor(...)`
- `NewRegionMergeAndRebuildStepExecutor(...)`
- `RunSubtask(ctx, subtask)`
- `OnFinished(ctx, summary)`

### 5.3 Metadata interfaces
- `ReadBaseManifest / WriteBaseManifest`
- `ReadChangedRegionsManifest / WriteChangedRegionsManifest`
- `ResolveLatestBaseManifest`
- `RemoveBaseVersion`

### 5.4 Job interfaces
- `SubmitTask` (full/delta)
- `SubmitCompactionTask`
- `StartJob / Job2Step / FinishJob / FailJob / CancelJob`

---

## 6. Cost Model and Business-friendly Output

### 6.1 Cost split dimensions
- **Cluster resource cost**: nodes, block storage, LB, NAT, EKS, VPC endpoints, KMS.
- **Transfer cost**: intra-region/cross-AZ transfer.
- **Object storage cost**: capacity + request charges.

### 6.2 Measurement baseline
- DailyChanged = 175TB.
- `SHOW IMPORT METERING/COST` provides auditable cost inputs.
- For non-technical stakeholders, output uses a monthly three-part summary: resources / transfer / storage+requests.

### 6.3 Risk conclusion
- `DataTransfer-Regional-Bytes` is the dominant variance driver.
- Budget feasibility depends on cross-AZ transfer control.

---

## 7. Compatibility and Risk Controls

### 7.1 Compatibility
- No semantic change to normal import.
- full/delta are explicit opt-in switches.
- fallback keeps delta executable when base metadata is missing.

### 7.2 Risks and mitigations
- **Risk**: checksum tests sensitive to table-id layout drift.
  - **Mitigation**: assert stable fields (kvs/size) + non-zero sum.
- **Risk**: upstream structural changes during rebase.
  - **Mitigation**: preserve master structure and re-apply importinto semantics minimally.

---

## 8. Scope Completed in This Round (2026-02-11)
- Rebased to latest `origin/master`.
- Resolved conflicts and fixed build blockers.
- Added rationale-level comments on critical paths.
- Passed targeted key test set (detailed list in detailed design doc).

