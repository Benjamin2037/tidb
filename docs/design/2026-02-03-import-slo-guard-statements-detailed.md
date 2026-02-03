# SLO Guard Statements Detailed Design

> This document focuses on SLO guard statements and their config and execution path. It does not cover other IMPORT INTO syntax or pipeline details.

## 1. Design Constraints
- No new system variables (apply_mode / apply_window / apply_rate_limit were removed).
- Keep only minimal config items: enable, pause_threshold, slow_apply_rate_limit_mb_per_sec.
- All configuration is done through a system table and SQL statements.

## 2. SQL Syntax
```
ALTER IMPORT SLO GUARD [JOB <id>] WITH
  config='{"enable":true,"pause_threshold":"10ms","slow_apply_rate_limit_mb_per_sec":64}'

SHOW IMPORT SLO GUARD [JOB <id>]
```
- If JOB <id> is omitted: global config (job_id=0).
- enable=false disables the scope (global or job).
- SHOW ... JOB <id>: returns the effective config (job preferred, fallback to global).
- SHOW without JOB: returns the visible config list (SUPER can see the global row).

## 3. System Table Design
Table: mysql.tidb_import_slo_guard

Suggested fields:
- job_id bigint primary key
- config JSON (or LONGTEXT with raw JSON)

Compatibility fields (optional during transition):
- enable tinyint(1)
- pause_threshold varchar(32)
- slow_apply_rate_limit_mb_per_sec bigint

Migration strategy:
- During upgrade: if legacy fields exist and config is empty, build JSON and write to config.
- During read: prefer config; if config is empty, fallback to legacy fields and generate default JSON.

## 4. JSON Schema and Validation
Minimal schema:
- enable: bool, default false
- pause_threshold: duration string, must be > 0
- slow_apply_rate_limit_mb_per_sec: integer, must be >= 0
- Unknown fields are stored but ignored.

Defaults:
- enable=false
- pause_threshold=10ms
- slow_apply_rate_limit_mb_per_sec=64

## 5. Privilege and Security
- Job-level config: same as IMPORT INTO privileges (job owner or SUPER).
- Global config: SUPER required.

## 6. Execution Path
### 6.1 ALTER execution
- Parse config JSON -> structured config -> validate -> upsert system table
- job_id=0 represents global config

### 6.2 SHOW execution
- SUPER: returns all configs (including global)
- Non-SUPER: returns only viewable job configs
- SHOW ... JOB <id>: if the job has no config, return the global config as the effective value

## 7. Runtime Logic (Worker)
### 7.1 Metric query
- metrics_schema.tidb_query_duration
- sql_type=Select
- window=5m, interval=30s

### 7.2 Threshold derivation
- slow = 0.8 * pause_threshold
- resume = 0.6 * pause_threshold

### 7.3 State machine
- p99 >= slow -> slow
- p99 >= pause -> pause
- p99 <= resume -> normal

### 7.4 Actions
- slow or pause: Apply rate limit is min(baseLimit, slow_apply_rate_limit_mb_per_sec)
- pause: PauseTask only for tasks tagged by SLO guard
- resume: Resume tasks paused by SLO guard

## 8. Edge Cases and Fallbacks
- No metric data: skip this iteration
- No config: use defaults (enable=false)
- Global disabled: only explicitly enabled jobs are affected

## 9. Test Coverage Recommendations
- Parsing and validation: valid and invalid JSON, duplicate fields, negative or invalid duration
- Privileges: global requires SUPER, job requires owner or SUPER
- SHOW: job fallback to global, list visibility
- Worker: threshold derivation, state transitions, pause and resume, rate-limit override
