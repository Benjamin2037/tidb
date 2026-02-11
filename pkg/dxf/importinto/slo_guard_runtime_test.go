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

	"github.com/pingcap/tidb/pkg/executor/importer"
	"github.com/pingcap/tidb/pkg/lightning/config"
	"github.com/stretchr/testify/require"
)

func TestEffectiveApplyRateLimit(t *testing.T) {
	restore := resetSLOGuardRuntimes()
	t.Cleanup(restore)

	baseLimit := config.ByteSize(128 << 20)

	require.Equal(t, baseLimit, effectiveApplyRateLimit(1, baseLimit))
	require.Equal(t, config.ByteSize(0), effectiveApplyRateLimit(1, 0))

	setSLOGuardRuntime(importer.SLOGuardGlobalJobID, sloGuardStateSlow, config.ByteSize(64<<20))
	require.Equal(t, config.ByteSize(64<<20), effectiveApplyRateLimit(1, baseLimit))
	require.Equal(t, config.ByteSize(64<<20), effectiveApplyRateLimit(1, 0))

	setSLOGuardRuntime(1, sloGuardStatePause, config.ByteSize(32<<20))
	require.Equal(t, config.ByteSize(32<<20), effectiveApplyRateLimit(1, baseLimit))

	setSLOGuardRuntime(1, sloGuardStateNormal, config.ByteSize(32<<20))
	require.Equal(t, baseLimit, effectiveApplyRateLimit(1, baseLimit))
}

func resetSLOGuardRuntimes() func() {
	sloGuardRuntimes.mu.Lock()
	prev := sloGuardRuntimes.jobs
	sloGuardRuntimes.jobs = make(map[int64]sloGuardRuntime)
	sloGuardRuntimes.mu.Unlock()

	return func() {
		sloGuardRuntimes.mu.Lock()
		sloGuardRuntimes.jobs = prev
		sloGuardRuntimes.mu.Unlock()
	}
}
