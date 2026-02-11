// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package importer

import (
	"context"
	"encoding/json"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
	"github.com/tikv/client-go/v2/util"
)

const (
	// SLOGuardGlobalJobID is the default job id for global SLO guard settings.
	SLOGuardGlobalJobID int64 = 0

	// DefaultSLOGuardPauseThreshold is the default pause threshold string.
	DefaultSLOGuardPauseThreshold = "10ms"
	// DefaultSLOGuardSlowApplyRateLimitMB is the default slow apply rate limit (MB/s).
	DefaultSLOGuardSlowApplyRateLimitMB = int64(64)

	sloGuardConfigEnableKey                   = "enable"
	sloGuardConfigPauseThresholdKey           = "pause_threshold"
	sloGuardConfigSlowApplyRateLimitMBPerSecKey = "slow_apply_rate_limit_mb_per_sec"
)

// SLOGuardConfig stores SLO guard configuration for IMPORT INTO.
type SLOGuardConfig struct {
	JobID                      int64
	Enable                     bool
	PauseThreshold             string
	SlowApplyRateLimitMBPerSec int64
	ConfigJSON                 string
	UpdatedAt                  types.Time
	UpdatedBy                  string
}

func defaultSLOGuardConfig(jobID int64) *SLOGuardConfig {
	return &SLOGuardConfig{
		JobID:                      jobID,
		Enable:                     false,
		PauseThreshold:             DefaultSLOGuardPauseThreshold,
		SlowApplyRateLimitMBPerSec: DefaultSLOGuardSlowApplyRateLimitMB,
	}
}

// GetSLOGuardConfig returns the SLO guard config for the given job id.
func GetSLOGuardConfig(ctx context.Context, exec sqlexec.SQLExecutor, jobID int64) (*SLOGuardConfig, bool, error) {
	ctx = util.WithInternalSourceType(ctx, kv.InternalImportInto)
	sql := `SELECT job_id, config, enable, pause_threshold, slow_apply_rate_limit_mb_per_sec, updated_at, updated_by
		FROM mysql.tidb_import_slo_guard WHERE job_id = %?`
	rs, err := exec.ExecuteInternal(ctx, sql, jobID)
	if err != nil {
		return nil, false, err
	}
	defer terror.Call(rs.Close)
	rows, err := sqlexec.DrainRecordSet(ctx, rs, 1)
	if err != nil {
		return nil, false, err
	}
	if len(rows) == 0 {
		return nil, false, nil
	}
	cfg, err := rowToSLOGuardConfig(rows[0])
	if err != nil {
		return nil, false, err
	}
	return cfg, true, nil
}

// GetSLOGuardConfigs returns SLO guard configs for the given job ids.
// If jobIDs is nil, it returns all rows.
func GetSLOGuardConfigs(ctx context.Context, exec sqlexec.SQLExecutor, jobIDs []int64) (map[int64]*SLOGuardConfig, error) {
	if jobIDs != nil && len(jobIDs) == 0 {
		return map[int64]*SLOGuardConfig{}, nil
	}
	ctx = util.WithInternalSourceType(ctx, kv.InternalImportInto)
	var (
		args []any
		sb   strings.Builder
	)
	sb.WriteString(`SELECT job_id, config, enable, pause_threshold, slow_apply_rate_limit_mb_per_sec, updated_at, updated_by
		FROM mysql.tidb_import_slo_guard`)
	if jobIDs != nil {
		sb.WriteString(" WHERE job_id IN (")
		for i, jobID := range jobIDs {
			if i > 0 {
				sb.WriteString(",")
			}
			sb.WriteString("%?")
			args = append(args, jobID)
		}
		sb.WriteString(")")
	}
	sb.WriteString(" ORDER BY job_id")
	rs, err := exec.ExecuteInternal(ctx, sb.String(), args...)
	if err != nil {
		return nil, err
	}
	defer terror.Call(rs.Close)
	rows, err := sqlexec.DrainRecordSet(ctx, rs, 1024)
	if err != nil {
		return nil, err
	}
	configs := make(map[int64]*SLOGuardConfig, len(rows))
	for _, row := range rows {
		cfg, err := rowToSLOGuardConfig(row)
		if err != nil {
			return nil, err
		}
		configs[cfg.JobID] = cfg
	}
	return configs, nil
}

// UpsertSLOGuardConfig inserts or updates SLO guard config for the job.
func UpsertSLOGuardConfig(ctx context.Context, exec sqlexec.SQLExecutor, cfg *SLOGuardConfig) error {
	if cfg == nil {
		return errors.New("slo guard config is nil")
	}
	if err := normalizeSLOGuardConfig(cfg); err != nil {
		return err
	}
	ctx = util.WithInternalSourceType(ctx, kv.InternalImportInto)
	sql := `INSERT INTO mysql.tidb_import_slo_guard
		(job_id, config, enable, pause_threshold, slow_apply_rate_limit_mb_per_sec, updated_by)
		VALUES (%?, %?, %?, %?, %?, %?)
		ON DUPLICATE KEY UPDATE
			config=VALUES(config),
			enable=VALUES(enable),
			pause_threshold=VALUES(pause_threshold),
			slow_apply_rate_limit_mb_per_sec=VALUES(slow_apply_rate_limit_mb_per_sec),
			updated_by=VALUES(updated_by)`
	rs, err := exec.ExecuteInternal(
		ctx,
		sql,
		cfg.JobID,
		cfg.ConfigJSON,
		boolToInt64(cfg.Enable),
		cfg.PauseThreshold,
		cfg.SlowApplyRateLimitMBPerSec,
		cfg.UpdatedBy,
	)
	if err != nil {
		return err
	}
	if rs != nil {
		terror.Call(rs.Close)
	}
	return nil
}

func rowToSLOGuardConfig(row chunk.Row) (*SLOGuardConfig, error) {
	if row.Len() < 7 {
		return nil, errors.New("invalid slo guard config row")
	}
	jobID := row.GetInt64(0)
	configRaw := ""
	if !row.IsNull(1) {
		configRaw = strings.TrimSpace(row.GetJSON(1).String())
	}
	updatedAt := row.GetTime(5)
	updatedBy := row.GetString(6)
	if configRaw != "" {
		cfg, err := configFromJSON(configRaw)
		if err != nil {
			return nil, err
		}
		cfg.JobID = jobID
		cfg.UpdatedAt = updatedAt
		cfg.UpdatedBy = updatedBy
		return cfg, nil
	}
	cfg := &SLOGuardConfig{
		JobID:                      jobID,
		Enable:                     row.GetInt64(2) != 0,
		PauseThreshold:             row.GetString(3),
		SlowApplyRateLimitMBPerSec: row.GetInt64(4),
		UpdatedAt:                  updatedAt,
		UpdatedBy:                  updatedBy,
	}
	if err := normalizeSLOGuardConfig(cfg); err != nil {
		return nil, err
	}
	return cfg, nil
}

func boolToInt64(v bool) int64 {
	if v {
		return 1
	}
	return 0
}

func normalizeSLOGuardConfig(cfg *SLOGuardConfig) error {
	if cfg == nil {
		return errors.New("slo guard config is nil")
	}
	if strings.TrimSpace(cfg.PauseThreshold) == "" {
		cfg.PauseThreshold = DefaultSLOGuardPauseThreshold
	}
	threshold, err := time.ParseDuration(cfg.PauseThreshold)
	if err != nil || threshold <= 0 {
		return errors.New("invalid slo guard pause_threshold")
	}
	if cfg.SlowApplyRateLimitMBPerSec < 0 {
		return errors.New("invalid slo guard slow_apply_rate_limit_mb_per_sec")
	}
	if strings.TrimSpace(cfg.ConfigJSON) == "" {
		raw, err := buildSLOGuardConfigJSON(cfg)
		if err != nil {
			return err
		}
		cfg.ConfigJSON = raw
	}
	return nil
}

func defaultSLOGuardConfigMap() map[string]any {
	return map[string]any{
		sloGuardConfigEnableKey:                     false,
		sloGuardConfigPauseThresholdKey:             DefaultSLOGuardPauseThreshold,
		sloGuardConfigSlowApplyRateLimitMBPerSecKey: DefaultSLOGuardSlowApplyRateLimitMB,
	}
}

func mergeSLOGuardConfigMap(base, overlay map[string]any) map[string]any {
	if base == nil && overlay == nil {
		return map[string]any{}
	}
	out := make(map[string]any, len(base)+len(overlay))
	for k, v := range base {
		out[k] = v
	}
	for k, v := range overlay {
		out[k] = v
	}
	return out
}

func parseSLOGuardConfigJSON(raw string) (map[string]any, error) {
	if strings.TrimSpace(raw) == "" {
		return nil, errors.New("slo guard config is empty")
	}
	var cfg map[string]any
	if err := json.Unmarshal([]byte(raw), &cfg); err != nil {
		return nil, errors.New("invalid slo guard config json")
	}
	if cfg == nil {
		return nil, errors.New("invalid slo guard config json")
	}
	return cfg, nil
}

func configFromJSON(raw string) (*SLOGuardConfig, error) {
	cfgMap, err := parseSLOGuardConfigJSON(raw)
	if err != nil {
		return nil, err
	}
	merged := mergeSLOGuardConfigMap(defaultSLOGuardConfigMap(), cfgMap)
	cfg, err := sloGuardConfigFromMap(merged)
	if err != nil {
		return nil, err
	}
	cfg.ConfigJSON = strings.TrimSpace(raw)
	return cfg, nil
}

func sloGuardConfigFromMap(cfg map[string]any) (*SLOGuardConfig, error) {
	out := defaultSLOGuardConfig(0)
	if cfg == nil {
		return out, nil
	}
	if val, ok := cfg[sloGuardConfigEnableKey]; ok {
		enabled, err := parseSLOGuardBoolValue(val)
		if err != nil {
			return nil, err
		}
		out.Enable = enabled
	}
	if val, ok := cfg[sloGuardConfigPauseThresholdKey]; ok {
		text, err := parseSLOGuardStringValue(val)
		if err != nil {
			return nil, err
		}
		threshold, err := time.ParseDuration(text)
		if err != nil || threshold <= 0 {
			return nil, errors.New("invalid slo guard pause_threshold")
		}
		out.PauseThreshold = text
	}
	if val, ok := cfg[sloGuardConfigSlowApplyRateLimitMBPerSecKey]; ok {
		rate, err := parseSLOGuardInt64Value(val)
		if err != nil || rate < 0 {
			return nil, errors.New("invalid slo guard slow_apply_rate_limit_mb_per_sec")
		}
		out.SlowApplyRateLimitMBPerSec = rate
	}
	return out, nil
}

func buildSLOGuardConfigJSON(cfg *SLOGuardConfig) (string, error) {
	if cfg == nil {
		return "", errors.New("slo guard config is nil")
	}
	payload := map[string]any{
		sloGuardConfigEnableKey:                     cfg.Enable,
		sloGuardConfigPauseThresholdKey:             cfg.PauseThreshold,
		sloGuardConfigSlowApplyRateLimitMBPerSecKey: cfg.SlowApplyRateLimitMBPerSec,
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		return "", err
	}
	return string(raw), nil
}

func parseSLOGuardBoolValue(val any) (bool, error) {
	switch v := val.(type) {
	case bool:
		return v, nil
	case float64:
		if v == 0 || v == 1 {
			return v == 1, nil
		}
	case int:
		if v == 0 || v == 1 {
			return v == 1, nil
		}
	case int64:
		if v == 0 || v == 1 {
			return v == 1, nil
		}
	case json.Number:
		i, err := v.Int64()
		if err == nil && (i == 0 || i == 1) {
			return i == 1, nil
		}
	}
	return false, errors.New("invalid slo guard enable")
}

func parseSLOGuardInt64Value(val any) (int64, error) {
	switch v := val.(type) {
	case int:
		return int64(v), nil
	case int64:
		return v, nil
	case float64:
		if math.Trunc(v) != v {
			return 0, errors.New("invalid slo guard int64")
		}
		maxInt64 := float64(^uint64(0) >> 1)
		minInt64 := -maxInt64 - 1
		if v < minInt64 || v > maxInt64 {
			return 0, errors.New("invalid slo guard int64")
		}
		return int64(v), nil
	case json.Number:
		return v.Int64()
	case string:
		return strconv.ParseInt(strings.TrimSpace(v), 10, 64)
	}
	return 0, errors.New("invalid slo guard int64")
}

func parseSLOGuardStringValue(val any) (string, error) {
	switch v := val.(type) {
	case string:
		return strings.TrimSpace(v), nil
	case []byte:
		return strings.TrimSpace(string(v)), nil
	}
	return "", errors.New("invalid slo guard string")
}

// MergeSLOGuardConfigJSON merges the input config with a base config JSON and returns the new config.
// baseJSON can be empty to indicate defaults.
func MergeSLOGuardConfigJSON(baseJSON, patchJSON string) (*SLOGuardConfig, string, error) {
	baseMap := defaultSLOGuardConfigMap()
	if strings.TrimSpace(baseJSON) != "" {
		parsedBase, err := parseSLOGuardConfigJSON(baseJSON)
		if err != nil {
			return nil, "", err
		}
		baseMap = mergeSLOGuardConfigMap(baseMap, parsedBase)
	}
	patchMap, err := parseSLOGuardConfigJSON(patchJSON)
	if err != nil {
		return nil, "", err
	}
	merged := mergeSLOGuardConfigMap(baseMap, patchMap)
	cfg, err := sloGuardConfigFromMap(merged)
	if err != nil {
		return nil, "", err
	}
	raw, err := json.Marshal(merged)
	if err != nil {
		return nil, "", err
	}
	cfg.ConfigJSON = string(raw)
	return cfg, cfg.ConfigJSON, nil
}
