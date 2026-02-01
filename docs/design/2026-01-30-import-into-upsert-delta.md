# IMPORT INTO 全量/增量 Upsert（基于 DXF + S3）

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

本文档描述在 TiDB 的 IMPORT INTO + DXF + Lightning global sort/ingest 基础上，新增“全量基线 + 增量 upsert”的导入能力。方案支持从对象存储导入 TSV/CSV，在 S3 上维护 base 版本元数据，按 region 进行 base+delta 合并，并在 delta 导入时自动仅 ingest 变更 region。同时提供 `SHOW IMPORT METERING` 与 `SHOW IMPORT COST` 以便审计导入资源消耗与成本。

## Motivation or Background

业务场景为宽表（含 JSON 列），主键为（用户 ID + 设备 ID），每天来自多个系统的增量数据仅更新部分列。需求要求：

- 仅需要最终一致性，允许批量导入。
- 增量导入必须基于前一日基线数据做 upsert（部分列更新）。
- 成本敏感，导入链路需要可观测与可估算。
- 不希望人工操作或额外命令，增量导入应自动仅 ingest 变更范围。

现有 IMPORT INTO 的冲突处理以“删除冲突 KV”为主，无法完成“稀疏列更新”的 upsert，因此需要新的增量流程与元数据体系。

## Detailed Design

### 用户语法与选项

复用 IMPORT INTO 的 WITH 选项解析，新增如下选项：

- `WITH full`：一次导入后保留 sorted KV 为 base，生成 Base Manifest。
- `WITH delta`：基于 base 做增量 upsert，生成新 base，并自动 ingest 变更 region；若缺少 base_version 则自动从**目标表当前 TiKV 元数据**解析 base（region 范围 + SST 元信息），并通过 remote cop 扫描重叠范围；解析失败则报错，不依赖 backupCluster。base_version 非强制：可显式指定 base_version/base_uri；未指定则自动解析；需要固定基线时可在首次 delta 后执行一次 full 生成新 base。
- `base_version='v...'`：指定基线版本（可选）。
- `base_uri='s3://...'`：指定基线路径（可选）。
- `merge_strategy='last_write_wins'|'max_ts'`：delta 内同主键多条记录的归并策略。
- `base_merge_mode='reference'|'copy'`：delta 合并时对未变更 region 的处理方式（reference 复用旧文件；copy 复制到新 base）。
- `merge_executor='tidb'|'tikv-worker'`：region 合并执行位置（默认 tidb；选择 tikv-worker 走远端合并）。

约束：

- full/delta 只能走 global sort（`CloudStorageURI` 必须存在）。
- 若缺少 base_version，则 `WITH delta` 自动解析目标表 TiKV 元数据生成 base 视图；解析失败则直接报错。
- `merge_executor=tikv-worker` 需要 base manifest 已存在，不支持无 base manifest 回退路径。

### 流水线设计

#### Full 模式

沿用现有 global sort 流程，新增 PostProcess 生成 base manifest。

```
StepInit
  -> EncodeAndSort
  -> MergeSort
  -> WriteAndIngest
  -> CollectConflicts (可选)
  -> ConflictResolution (可选)
  -> PostProcess (生成 base manifest)
  -> StepDone
```

#### Delta 模式

新增增量 pipeline，RegionMerge 后直接 ingest 变更 region，无需人工命令：

```
StepInit
  -> DeltaEncodeAndSort
  -> PlanTouchedRegions
  -> RegionMergeAndRebuild
  -> IngestChangedRegions
  -> PostProcess
  -> StepDone
```

### 预热加载路径（调用链路）

- 入口：`executor/import_into.go` -> `importinto/SubmitTask` -> `importScheduler.GetNextStep`
- PlanTouchedRegions：`plan_touched_regions.go` 读取 base manifest（`ReadBaseManifest`）或 TiKV 元数据（`resolveTableRecordRanges`），并加载 delta KV meta（`getSortedKVMetasOfEncodeStep`）
- RegionMerge：`NewRegionMergeStepExecutor.Init` 初始化 `TableImporter` 与编码器；随后按 region 建立迭代器  
  - S3 路径：`external.NewMergeKVIter` 预取对象存储元信息并读取首批 KV  
  - remote cop 路径：`remote_base_iter.go` 通过 `buildBaseScanDAG` + `distsql.Select` 触发 RegionCache 预热
- IngestChangedRegions：`generateIngestChangedRegionsSpecs` 预解析 base manifest 与 changed regions，提取范围与文件列表

### Delta 行编码（稀疏更新）

增量数据可能只包含部分列。为保证“缺失列不覆盖”，对 data KV value 追加列位图：

- 写入：在 data KV value 前增加 `varint(bitmap_len)` + `bitmap bytes`。
- 读取合并：只对 bitmap 标记的列应用 delta 值，未出现列保持 base 值。

该编码只影响 delta 模式，不影响原有 full/默认导入。

### 多源数据冲突合并

当同一批次来自多源系统的数据出现相同主键，合并顺序需确定且可追踪：

- 排序键：`(primary_key, merge_ts, source_priority, file_order, row_offset)`  
- merge_ts 由业务版本列/时间列提供；缺失或相等时使用 file_order + row_offset 作为稳定 tie-breaker  
- 同主键多源记录归并为一条，保证跨重试/重放的确定性

### RegionMerge 合并算法

对每个变更 region，读取 base + delta 的 data KV 进行行级合并，并重建 data/index KV。base manifest 存在时使用 `external.NewMergeKVIter` 顺序合并 S3 上的 sorted KV；缺失时从目标表 TiKV 元数据构造运行时 base 视图（region 范围 + SST 元信息），通过 remote cop 扫描重叠范围并合并（RPC/解码开销更高，仅作为回退路径）：

- 未变更 region：由 `base_merge_mode` 决定复用旧文件（reference）或复制到新 base（copy）。
- `merge_executor`：
  - `tidb`：当前默认路径；支持 base manifest 与无 base manifest 回退。
  - `tikv-worker`：通过 `/merge` 在 tikv-worker 内合并并产出外部 KV；要求 base manifest。
- tikv-worker 限制：不支持 generated 列、prefix index、multi‑valued/vector index、common_handle_version>1；索引列需为二进制/数值类（无 restored data）。

1. 根据主键排序并合并。
2. 若 base 行存在，delta 行仅覆盖出现列。
3. 若 base 行不存在，按列默认值/生成列补齐后插入。
4. delta 内同主键多条记录按 `merge_strategy` 归并为一条。
5. 输出新 base 的 data/index KV，同时生成 index 统计文件。

### Changed Regions 规划与自动 ingest

PlanTouchedRegions 基于 delta KV key ranges 与 base manifest 的 region 边界求交，输出 `changed_regions.json`。若缺少 base manifest，则从目标表 TiKV 元数据获取 region 范围，按 overlap 生成变更范围（不退化为全量范围）。

IngestChangedRegions 读取 `changed_regions.json` 与新 base manifest，按 region 范围仅 ingest 变更 region 的 data/index KV。该步骤自动执行，不引入 `ADMIN INGEST` 等人工命令。

### Manifest 与元数据

所有明细元数据落在对象存储，数据库仅保存指针字段（作业 summary/parameters）：

当未提供 base_version 且 base manifest 不存在时，使用**目标表当前 TiKV 元数据**构建运行时 base 视图：

- 读取目标表对应的 Region 范围与 SST 元信息的 key bounds（smallest/biggest）。
- 仅保留与 delta key-range overlap 的范围，交由 remote cop 构建 snapshot 并扫描。
- 组装为运行时 base 视图（可选落盘到外部存储），供后续 merge 使用。

Base Manifest：

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

Changed Regions Manifest：

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

### SHOW 查询接口

- `SHOW IMPORT BASES`：列出 base 版本与汇总信息。
- `SHOW IMPORT REGIONS BASE <id>`：读取 base manifest 输出 region 明细。
- `SHOW IMPORT CHANGED REGIONS JOB <job_id>`：读取 changed regions manifest。
- `SHOW IMPORT METERING JOB <job_id>`：输出导入任务的原始计量数据（按时间桶）。
- `SHOW IMPORT COST JOB <job_id>`：基于计量数据计算成本并汇总。

### 计量与成本模型

计量数据来自 DXF metering 文件（对象存储）。按任务 ID 过滤并输出：

- objstore 请求数：GET/PUT
- objstore 读写字节数
- cluster 读写字节数
- row_count、data/index KV bytes、required slots、duration 等

成本计算采用默认价格参数：

- GET：$0.0004 / 1000
- PUT：$0.005 / 1000
- 基于字节数的 objstore/cluster 成本默认 0（预留扩展）
- S3 Standard 存储分层价（US East, N. Virginia；2026-01-30）：0-50TB $0.023/GB-月、50-500TB $0.022/GB-月、>500TB $0.021/GB-月（实际以 Price List API 刷新）
- 同区域 S3 到计算节点内网传输不计费；跨区域/公网出流另计

计量聚合粒度为 metering flush interval（当前实现按分钟）。

### 175TB 存储成本示例（S3 Standard, US East）
- 单份 base（175 TB ≈ 175,000 GB）：约 $3,900/月（按分层价）
- 若保留 1 份 base + 当日 delta（175 TB）：约 $7,750/月
- 若保留 10 天 delta（1,750 TB），总量 1,925 TB：约 $40,975/月

### 节点规格与吞吐 sizing（8c16G/16c32G）
按 AWS 官方网络带宽上限估算（40%~60% 有效吞吐）：
- **c6i.4xlarge（16c32G）**：12.5Gbps → 2.2–3.4 TB/h/节点
- **c6in.4xlarge（16c32G，网络优化）**：50Gbps → 9–13.5 TB/h/节点
目标吞吐下的节点数区间：
| 目标 | 需要吞吐 | c6i.4xlarge | c6in.4xlarge |
| --- | ---: | ---: | ---: |
| 35TB / 2h | 17.5 TB/h | 6–8 台 | 2–3 台 |
| 175TB / 2h | 87.5 TB/h | 26–40 台 | 7–10 台 |
| upsert(r=70%) | 201 TB/h | 60–90 台 | 15–23 台 |

推荐角色划分：
- **控制面（TiDB 8c16G）**：固定 3 台（HA）
- **计算面（TiDB 16c32G）**：约为 remote-cop 节点数 1–2 倍
- **remote-cop（tikv-worker 16c32G）**：优先 c6in 以减少节点数并吃满 S3 带宽

### Spot / Savings Plans（降本思路）
- **Spot**：价格最低但可中断，适合 DXF/remote-cop 批处理，可显著降低导入成本。
- **Savings Plans**：按 $/小时承诺 1 或 3 年，适合控制面与常驻计算面节点。

### 权限与安全

复用 IMPORT INTO 权限检查；`SHOW IMPORT *` 通过作业所有者或 super 权限访问。

### 兼容性与回滚

- 不影响现有 IMPORT INTO 默认行为。
- 仅在 `WITH full/delta` 时启用增量逻辑。
- 任务失败不替换 active base，保持基线不变。

### 版本保留与周期 Compaction（后台 DXF 任务）

为支持审计/回滚与追溯风险控制，允许保留多天 base+delta 版本；保留期结束后，通过后台 DXF 任务做版本 compaction：

- **保留窗口**：默认保留 10 天版本，可通过系统变量 `tidb_import_into_base_retention_days` 调整。
- **触发方式**：仅由 DDL owner 定时扫描 import job + base manifest（create_time），存在超过保留期的 base 时触发，对用户无感知；运行中的导入任务会跳过。
- **输入**：最新 base manifest（作为重写源），旧版本仅用于校验与回滚。
- **输出**：生成新的 compacted base manifest（合并范围/减少小文件），写入新的 base 版本。
- **实现路径**：PlanTouchedRegions（ForceAllRegions）生成全量变更范围 -> RegionMergeAndRebuild 重写 base；不走 DeltaEncodeAndSort 与 IngestChangedRegions。
- **约束**：compaction 需 base manifest 已存在，缺失则任务失败。
- **一致性保障**：compaction 完成前不删除旧版本；完成后对新 manifest 做校验（checksum/row_count/bytes），再执行 GC。
- **GC 策略**：仅当最新 base 为 compaction 产物时，删除超过保留期的 base 版本与孤儿对象，确保回滚窗口内数据可追溯。

存储量近似公式：`Total ≈ Base + RetentionDays × DailyChanged`（仅统计变更 region 文件，不含临时文件开销）。
示例（默认 10 天、DailyChanged=175TB）：`Total ≈ 175 + 10×175 = 1,925 TB`。
该策略在控制存储成本的同时，避免频繁回写导致的版本碎片化，降低长期 merge 风险。

## Test Design

### Functional Tests

- `WITH full/delta` 选项解析与互斥检查；delta 无 base_version 时自动解析目标表 TiKV 元数据生成 base 视图。
- Base/Changed Regions manifest 读写与内容正确性。
- Delta pipeline 步骤序列与自动 ingest。
- `SHOW IMPORT BASES/REGIONS/CHANGED` 输出校验。
- `SHOW IMPORT METERING/COST` 聚合与字段映射校验。

### Scenario Tests

- 宽表部分列更新（稀疏更新）正确性。
- delta 内重复主键记录按 `merge_strategy` 归并。
- 无 base_version 时自动解析目标表 TiKV 元数据；解析失败则报错。
- 失败重试不影响 base 版本一致性。

### Compatibility Tests

- 与现有 IMPORT INTO（无 full/delta）兼容。
- 与 global sort/local sort 的行为一致性验证。
- 权限与审计逻辑不回归。

### Benchmark Tests

- 对比现有 IMPORT INTO 吞吐与 delta 流水线吞吐。
- 评估仅 ingest 变更 region 对 TiKV 写入与 compaction 的影响。

## Impacts & Risks

### Impacts

- 导入链路可实现“只 ingest 变更 region”，显著降低无效写入与 compaction 成本。
- 可通过 metering/cost 查询实现导入可观测与成本追踪。

### Risks

- RegionMerge 需要解码与重建 data/index KV，CPU 与 IO 压力较大。
- 稀疏列编码/合并逻辑复杂，需完善测试覆盖。
- 计量存储缺失或配置错误时，`SHOW IMPORT METERING/COST` 可能不可用。

## Investigation & Alternatives

- 直接在 TiKV 做逐行 upsert 会引入大量随机写，吞吐不足。
- 通过人工命令触发变更 region ingest 违背“自动化”需求，已被替换为 delta 自动 ingest。

## Unresolved Questions

- 是否需要将成本模型做成可配置价格表（多云厂商、多 region）？
- Base 保留策略（保留多少版本、自动清理策略）是否需要进一步标准化？
