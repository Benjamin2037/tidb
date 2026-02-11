# IMPORT INTO Full/Delta Upsert Detailed Design (2026-02-11)

## 1. Code Paths and Module Responsibilities

### 1.1 Entry and option parsing
- `pkg/executor/importer/import.go`
  - Parses `WITH full/delta/base_*` options.
  - Builds import plan and submits to DXF.

### 1.2 DXF orchestration
- `pkg/dxf/importinto/planner.go`
  - Produces physical step chains for full and delta modes.
- `pkg/dxf/importinto/scheduler.go`
  - Drives step transitions, completion handling, and summary persistence.
- `pkg/dxf/importinto/task_executor.go`
  - Binds runtime executors per step.

### 1.3 Manifest and range planning
- `pkg/dxf/importinto/manifest.go`
  - Manifest read/write and version resolution.
- `pkg/dxf/importinto/full_manifest.go`
  - Base manifest generation in full mode.
- `pkg/dxf/importinto/plan_touched_regions.go`
  - Changed-region planning for delta.

### 1.4 Region merge and rebuild
- `pkg/dxf/importinto/region_merge.go`
  - Merges base+delta by changed region and rebuilds data/index KV.

### 1.5 SLO guard and operations
- `pkg/dxf/importinto/slo_guard_worker.go`
- `pkg/dxf/importinto/slo_guard_runtime.go`
- `pkg/executor/import_into_slo_guard.go`

### 1.6 Retention and compaction
- `pkg/dxf/importinto/compaction_worker.go`
- `pkg/domain/importinto_compaction_hook.go`
- `pkg/sessionctx/vardef/tidb_vars.go`
- `pkg/sessionctx/variable/sysvar.go`

---

## 2. Step-level Design Details

### 2.1 `PlanTouchedRegions`
Input:
- `BaseManifestPath`
- `DeltaDataFiles`
- `DeltaStart/DeltaEnd`

Output:
- `ChangedRegionsManifest`

Core logic:
1. If base manifest exists, select changed regions via range overlap.
2. If base manifest is missing, fallback to full-range changed regions for correctness.

Rationale:
- Correctness-first under missing metadata, then optimize later.

### 2.2 `RegionMergeAndRebuild`
Input:
- base manifest + changed regions + delta files

Output:
- new base manifest + rebuilt region data/index files

Core logic:
1. Decode delta row bitmap and build column map.
2. Merge base rows and delta rows by region (last-write-wins semantics).
3. Rebuild data/index KV and write to new base path.
4. If changed regions are empty and base is available, reuse previous manifest payload and only rotate version metadata.

Rationale:
- Keep rebuild scope minimal (changed regions only).
- Avoid unnecessary rewrite when delta has no effective changes.

### 2.3 `finishJob` in scheduler
Core logic:
- Persist job completion summary and table stats delta in one transaction.

Rationale:
- Ensures consistent observability state for SHOW/import summary and stats consumers.

---

## 3. Interface and Semantic Constraints

### 3.1 SQL layer
- `IMPORT INTO ... WITH full|delta`
- `SHOW IMPORT ...`
- `ALTER/SHOW IMPORT SLO GUARD`

### 3.2 Internal contracts
- `Read/Write*Manifest`: explicit path + idempotent behavior.
- `SubmitCompactionTask`: triggered only when policy conditions are met.
- `UpdateJobSummary`: source of truth for SHOW and cost accounting.

### 3.3 Error handling
- Task-level failure -> `FailJob`.
- User cancellation -> `CancelJob`.
- Step-level failure -> handled by DXF retry/revert policy.

---

## 4. Validation Executed in This Round

### 4.1 Build-level verification
Executed:
- `go test ./pkg/dxf/importinto ./pkg/executor/importer ./pkg/lightning/backend/local ./pkg/executor ./pkg/sessionctx/variable -run '^$' --tags=intest`

### 4.2 Targeted key tests (passed)
- `pkg/dxf/importinto`
  - `TestCollectConflictsStepExecutor`
  - `TestFinishJobUpdatesSummary`
  - `TestFinishJobError`
  - `TestUpdateTaskSummaryDeltaURI`
  - `TestImportInto`
- `pkg/executor/importer`
  - `TestProcessChunkWith/query_chunk`
  - `TestInitOptionsUpsertMode`
  - `TestJobHappyPath`
  - `TestUpdateJobSummary`
- `pkg/executor`
  - Key SHOW IMPORT and SLO guard cases
- `pkg/lightning/backend/local`
  - `TestNewBackendForTestCloseNilSafe`
  - `TestNewBackendForTestAssignMetrics`

### 4.3 Fixes made in this round
1. `scheduler.go`: fixed build error due to removed `TableID` field in `variable.TableDelta`.
2. `importer_testkit_test.go`: updated classic checksum constant in `TestProcessChunkWith/query_chunk`.
3. `collect_conflicts_test.go`: replaced brittle fixed checksum assertion with stable assertions (kvs/size + non-zero sum).

---

## 5. Added Rationale Comments in Code

Three rationale-level comments were added:
1. `pkg/dxf/importinto/scheduler.go`
   - Why summary and stats delta are persisted in one transaction.
2. `pkg/dxf/importinto/plan_touched_regions.go`
   - Why full-range fallback is used when base manifest is missing.
3. `pkg/dxf/importinto/region_merge.go`
   - Why old manifest payload is reused when no regions changed.

---

## 6. Optional Follow-ups (Not Included in This Commit)
- Improve fallback from full-range to region-metadata-bounded fallback.
- Add a first-class three-part cost view (resource/transfer/storage+request) into SHOW output.

