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
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMergeSLOGuardConfigJSON(t *testing.T) {
	cfg, raw, err := MergeSLOGuardConfigJSON("", `{"enable":true}`)
	require.NoError(t, err)
	require.True(t, cfg.Enable)
	require.Equal(t, DefaultSLOGuardPauseThreshold, cfg.PauseThreshold)
	require.Equal(t, DefaultSLOGuardSlowApplyRateLimitMB, cfg.SlowApplyRateLimitMBPerSec)
	require.NotEmpty(t, raw)

	var merged map[string]any
	require.NoError(t, json.Unmarshal([]byte(raw), &merged))
	require.Equal(t, true, merged["enable"])
	require.Equal(t, DefaultSLOGuardPauseThreshold, merged["pause_threshold"])
	require.Equal(t, float64(DefaultSLOGuardSlowApplyRateLimitMB), merged["slow_apply_rate_limit_mb_per_sec"])

	base := `{"enable":false,"pause_threshold":"8ms","slow_apply_rate_limit_mb_per_sec":32,"extra":"keep"}`
	cfg, raw, err = MergeSLOGuardConfigJSON(base, `{"pause_threshold":"20ms","extra":123}`)
	require.NoError(t, err)
	require.False(t, cfg.Enable)
	require.Equal(t, "20ms", cfg.PauseThreshold)
	require.Equal(t, int64(32), cfg.SlowApplyRateLimitMBPerSec)

	merged = map[string]any{}
	require.NoError(t, json.Unmarshal([]byte(raw), &merged))
	require.Equal(t, false, merged["enable"])
	require.Equal(t, "20ms", merged["pause_threshold"])
	require.Equal(t, float64(32), merged["slow_apply_rate_limit_mb_per_sec"])
	require.Equal(t, float64(123), merged["extra"])

	_, _, err = MergeSLOGuardConfigJSON("", `{"enable":1}`)
	require.NoError(t, err)
}

func TestMergeSLOGuardConfigJSONInvalid(t *testing.T) {
	_, _, err := MergeSLOGuardConfigJSON("", "not-json")
	require.Error(t, err)
}
