# Development Task Breakdown and Code Mapping (2026-02-11)

## 1. Split Principles
- Each task must be independently implementable, verifiable, and revertible.
- Every task maps to concrete code paths and test entry points.
- Documentation and code mapping are bidirectional for review and audit.

---

## 2. Minimal Independent Task List

### T01 Parser and option wiring (full/delta)
- Purpose: provide entry capabilities without changing default semantics.
- Main code:
  - `pkg/parser/parser.y`
  - `pkg/parser/parser.go`
  - `pkg/executor/importer/import.go`
- Validation: parser UT + import option UT.

### T02 Delta sparse row encoding
- Purpose: support partial-column updates without overwriting untouched columns.
- Main code:
  - `pkg/executor/importer/delta_encoding.go`
  - `pkg/executor/importer/kv_encode.go`
- Validation: `delta_encoding_test.go` / `kv_encode_test.go`.

### T03 Base/Changed manifest structures
- Purpose: establish versioned metadata and changed-range manifests.
- Main code:
  - `pkg/dxf/importinto/manifest.go`
  - `pkg/dxf/importinto/full_manifest.go`
- Validation: `manifest_test.go` / `full_manifest_test.go`.

### T04 Delta range planning (`PlanTouchedRegions`)
- Purpose: identify changed regions accurately.
- Main code:
  - `pkg/dxf/importinto/plan_touched_regions.go`
  - `pkg/dxf/importinto/delta_ranges.go`
- Validation: `plan_touched_regions_test.go` / `delta_ranges_test.go`.

### T05 Region merge and rebuild
- Purpose: merge base+delta by region and emit new base files.
- Main code:
  - `pkg/dxf/importinto/region_merge.go`
- Validation: `region_merge_test.go`.

### T06 Delta pipeline orchestration and finalize path
- Purpose: connect T02~T05 into DXF execution.
- Main code:
  - `pkg/dxf/importinto/planner.go`
  - `pkg/dxf/importinto/scheduler.go`
  - `pkg/dxf/importinto/task_executor.go`
- Validation: `planner_test.go` / `scheduler_test.go`.

### T07 SHOW IMPORT observability
- Purpose: expose base/regions/changed/cost/metering state.
- Main code:
  - `pkg/executor/show.go`
  - `pkg/executor/show_import_test.go`
  - `pkg/executor/show_import_integration_test.go`
- Validation: SHOW IMPORT test cases.

### T08 SLO guard semantics and runtime behavior
- Purpose: protect read latency during single-cluster import/apply.
- Main code:
  - `pkg/executor/import_into_slo_guard.go`
  - `pkg/dxf/importinto/slo_guard_worker.go`
  - `pkg/dxf/importinto/slo_guard_runtime.go`
- Validation: SLO guard parser/executor/worker tests.

### T09 Base retention and compaction
- Purpose: control long-term storage fragmentation and baseline sprawl.
- Main code:
  - `pkg/dxf/importinto/compaction_worker.go`
  - `pkg/domain/importinto_compaction_hook.go`
  - `pkg/sessionctx/vardef/tidb_vars.go`
  - `pkg/sessionctx/variable/sysvar.go`
- Validation: compaction worker and sysvar tests.

### T10 Rebase compatibility and regression hardening
- Purpose: fix rebase breakages and stabilize fragile assertions.
- Main code:
  - `pkg/dxf/importinto/scheduler.go`
  - `pkg/executor/importer/importer_testkit_test.go`
  - `pkg/dxf/importinto/collect_conflicts_test.go`
- Validation: targeted critical-path tests.

---

## 3. Document-to-Feature Mapping

### 3.1 Design document mapping
- `output/2026-02-11/design.md`
  - Covers goals, architecture, rationale, data structures, and interfaces for T01~T10.

### 3.2 Detailed design mapping
- `output/2026-02-11/detailed_design.md`
  - Covers step-level implementation and validation details for T03~T10.

### 3.3 Task mapping document
- This file: `output/2026-02-11/dev_subtasks.md`
  - Defines scope/purpose/code/tests for T01~T10.

---

## 4. File-to-Task Mapping (Representative)
- `pkg/executor/importer/import.go` -> T01
- `pkg/executor/importer/delta_encoding.go` -> T02
- `pkg/dxf/importinto/manifest.go` -> T03
- `pkg/dxf/importinto/plan_touched_regions.go` -> T04
- `pkg/dxf/importinto/region_merge.go` -> T05
- `pkg/dxf/importinto/scheduler.go` -> T06 + T10
- `pkg/executor/show.go` -> T07
- `pkg/executor/import_into_slo_guard.go` -> T08
- `pkg/dxf/importinto/compaction_worker.go` -> T09
- `pkg/executor/importer/importer_testkit_test.go` -> T10

---

## 5. Objective of This Round
- Deliver a reviewable and executable end-to-end chain:
  design -> task split -> code mapping -> validation.

