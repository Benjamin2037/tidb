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
- `WITH delta`：基于 base 做增量 upsert，生成新 base，并自动 ingest 变更 region；若缺少 base_version 则告警并回退为 remote coprocessor 读取 S3 SST。
- `base_version='v...'`：指定基线版本（可选）。
- `base_uri='s3://...'`：指定基线路径（可选）。
- `merge_strategy='last_write_wins'|'max_ts'`：delta 内同主键多条记录的归并策略。

约束：

- full/delta 只能走 global sort（`CloudStorageURI` 必须存在）。
- 若缺少 base_version，则 `WITH delta` 产生告警并回退为 remote coprocessor 读取 S3 SST。

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

### Delta 行编码（稀疏更新）

增量数据可能只包含部分列。为保证“缺失列不覆盖”，对 data KV value 追加列位图：

- 写入：在 data KV value 前增加 `varint(bitmap_len)` + `bitmap bytes`。
- 读取合并：只对 bitmap 标记的列应用 delta 值，未出现列保持 base 值。

该编码只影响 delta 模式，不影响原有 full/默认导入。

### RegionMerge 合并算法

对每个变更 region，读取 base + delta 的 data KV 进行行级合并，并重建 data/index KV。若缺少 base manifest，则通过 remote coprocessor 直接扫描 S3 上对应 region 的 SST：

1. 根据主键排序并合并。
2. 若 base 行存在，delta 行仅覆盖出现列。
3. 若 base 行不存在，按列默认值/生成列补齐后插入。
4. delta 内同主键多条记录按 `merge_strategy` 归并为一条。
5. 输出新 base 的 data/index KV，同时生成 index 统计文件。

### Changed Regions 规划与自动 ingest

PlanTouchedRegions 基于 delta KV key ranges 与 base manifest 的 region 边界求交，输出 `changed_regions.json`。若缺少 base manifest，则退化为全量范围（空 start/end），确保输出完整 base。

IngestChangedRegions 读取 `changed_regions.json` 与新 base manifest，按 region 范围仅 ingest 变更 region 的 data/index KV。该步骤自动执行，不引入 `ADMIN INGEST` 等人工命令。

### Manifest 与元数据

所有明细元数据落在对象存储，数据库仅保存指针字段（作业 summary/parameters）：

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
- S3 Standard 存储基线：约 $0.023/GB-月（不同 region/阶梯价格可能有差异）
- 同区域 S3 到计算节点内网传输不计费；跨区域/公网出流另计

计量聚合粒度为 metering flush interval（当前实现按分钟）。

### 175TB 存储成本示例（S3 Standard）
- 单份 base（175 TB ≈ 175,000 GB）：约 $4,025/月
- 若保留 1 份 base + 当日 delta（35 TB）：约 $4,830/月
- 若保留 10 天 delta（350 TB）：约 $12,075/月

### 权限与安全

复用 IMPORT INTO 权限检查；`SHOW IMPORT *` 通过作业所有者或 super 权限访问。

### 兼容性与回滚

- 不影响现有 IMPORT INTO 默认行为。
- 仅在 `WITH full/delta` 时启用增量逻辑。
- 任务失败不替换 active base，保持基线不变。

## Test Design

### Functional Tests

- `WITH full/delta` 选项解析与互斥检查；delta 无 base_version 时告警并启用 S3 SST remote coprocessor 扫描。
- Base/Changed Regions manifest 读写与内容正确性。
- Delta pipeline 步骤序列与自动 ingest。
- `SHOW IMPORT BASES/REGIONS/CHANGED` 输出校验。
- `SHOW IMPORT METERING/COST` 聚合与字段映射校验。

### Scenario Tests

- 宽表部分列更新（稀疏更新）正确性。
- delta 内重复主键记录按 `merge_strategy` 归并。
- 无 base_version 时告警并回退 S3 SST remote coprocessor 扫描。
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
