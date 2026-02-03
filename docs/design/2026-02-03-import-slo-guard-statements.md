# SLO Guard Statements Design

> Goal: Provide a minimal, extensible, auditable SLO guard control surface to protect read p99. Apply runs immediately by default and is slowed or paused only by SLO guard.

## 1. Background and Goals
- Read p99 must be strictly constrained; import and ingest must be slowable or pausable.
- Avoid many system variables; use a system table plus SQL statements.
- Keep syntax consistent with IMPORT INTO (privileges, SHOW semantics, audit entry).

## 2. Non-goals
- No apply_mode / manual / schedule or time-window scheduling.
- No dynamic auto-tuning based on p99 (fixed thresholds only).
- No multi-metric, multi-window, or custom label configuration.

## 3. Statements and Usage
### 3.1 ALTER (write config)
```
ALTER IMPORT SLO GUARD [JOB <id>] WITH
  config='{"enable":true,"pause_threshold":"10ms","slow_apply_rate_limit_mb_per_sec":64}'
```
- If JOB <id> is omitted, this is a global config (job_id=0).
- enable=false can disable the global guard.

### 3.2 SHOW (read config)
```
SHOW IMPORT SLO GUARD [JOB <id>]
```
- JOB <id>: return the effective config (job config preferred, fallback to global).
- Without JOB: return the visible config list (SUPER can see the global row).

## 4. Config Model (JSON)
- enable: boolean, whether SLO guard is enabled.
- pause_threshold: duration string (for example 10ms or 50ms), pause when p99 exceeds this value.
- slow_apply_rate_limit_mb_per_sec: integer (MB/s), Apply rate cap when in slow or pause state.
- Unknown fields: stored but ignored (reserved for future extension).

Defaults (when no config row exists):
- enable=false
- pause_threshold=10ms
- slow_apply_rate_limit_mb_per_sec=64

## 5. Privilege Model
- Job-level config: same as IMPORT INTO privileges (job owner or SUPER).
- Global config (job_id=0): SUPER required.

## 6. Runtime Behavior (Overview)
- The background SLO guard worker queries metrics_schema.tidb_query_duration with sql_type=Select, p99, 5m window, 30s interval.
- Threshold derivation:
  - slow = 0.8 * pause_threshold
  - resume = 0.6 * pause_threshold
- State machine:
  - p99 >= slow -> slow (rate limit)
  - p99 >= pause -> pause (pause import and preheat)
  - p99 <= resume -> normal (resume)
- If SLO guard is not configured (global and job both disabled): Apply runs immediately by default.

## 7. Config Precedence
- Job config takes priority; fallback to global if missing.
- Global enable=false disables any job that is not explicitly enabled.

## 8. Compatibility and Migration
- System table stores JSON config (new config column or equivalent field).
- If legacy fields exist (enable / pause_threshold / slow_apply_rate_limit_mb_per_sec), upgrade should write JSON and gradually deprecate old fields.

## 9. Examples
- Global enable:
  `ALTER IMPORT SLO GUARD WITH config='{"enable":true,"pause_threshold":"10ms","slow_apply_rate_limit_mb_per_sec":64}'`
- Global disable:
  `ALTER IMPORT SLO GUARD WITH config='{"enable":false}'`
- Job-only enable (job=123):
  `ALTER IMPORT SLO GUARD JOB 123 WITH config='{"enable":true,"pause_threshold":"8ms"}'`
