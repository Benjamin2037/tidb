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

package importinto

import (
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/executor/importer"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestSLOGuardThresholdsFromConfig(t *testing.T) {
	threshold := "20ms"
	cfg := &importer.SLOGuardConfig{
		JobID:                      importer.SLOGuardGlobalJobID,
		Enable:                     true,
		PauseThreshold:             threshold,
		SlowApplyRateLimitMBPerSec: 128,
	}
	thresholds := sloGuardThresholdsFromConfig(cfg, zap.NewNop())
	require.Equal(t, 20*time.Millisecond, thresholds.pause)
	require.Equal(t, 16*time.Millisecond, thresholds.slow)
	require.Equal(t, 12*time.Millisecond, thresholds.resume)
}

func TestDecideSLOGuardState(t *testing.T) {
	thresholds := sloGuardThresholds{
		resume: 6 * time.Millisecond,
		slow:   8 * time.Millisecond,
		pause:  10 * time.Millisecond,
	}

	require.Equal(t, sloGuardStateNormal, decideSLOGuardState(sloGuardStateNormal, 5*time.Millisecond, thresholds))
	require.Equal(t, sloGuardStateSlow, decideSLOGuardState(sloGuardStateNormal, 9*time.Millisecond, thresholds))
	require.Equal(t, sloGuardStatePause, decideSLOGuardState(sloGuardStateNormal, 12*time.Millisecond, thresholds))

	require.Equal(t, sloGuardStateSlow, decideSLOGuardState(sloGuardStatePause, 9*time.Millisecond, thresholds))
	require.Equal(t, sloGuardStateNormal, decideSLOGuardState(sloGuardStatePause, 5*time.Millisecond, thresholds))

	require.Equal(t, sloGuardStatePause, decideSLOGuardState(sloGuardStateSlow, 11*time.Millisecond, thresholds))
	require.Equal(t, sloGuardStateNormal, decideSLOGuardState(sloGuardStateSlow, 5*time.Millisecond, thresholds))
}
