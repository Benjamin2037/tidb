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
)

func TestPlanCompaction(t *testing.T) {
	now := time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC)
	oldBase := baseVersion{baseID: "base-1", createTime: now.Add(-11 * 24 * time.Hour)}
	newBase := baseVersion{baseID: "base-2", createTime: now.Add(-1 * 24 * time.Hour)}

	t.Run("retention disabled", func(t *testing.T) {
		compact, gc := planCompaction(now, 0, []baseVersion{oldBase, newBase}, false, false)
		require.Nil(t, compact)
		require.Empty(t, gc)
	})

	t.Run("no old base", func(t *testing.T) {
		compact, gc := planCompaction(now, 10, []baseVersion{newBase}, false, false)
		require.Nil(t, compact)
		require.Empty(t, gc)
	})

	t.Run("schedule compaction", func(t *testing.T) {
		compact, gc := planCompaction(now, 10, []baseVersion{oldBase, newBase}, false, false)
		require.NotNil(t, compact)
		require.Equal(t, "base-2", compact.baseID)
		require.Empty(t, gc)
	})

	t.Run("gc after compaction", func(t *testing.T) {
		compacted := newBase
		compacted.isCompaction = true
		compact, gc := planCompaction(now, 10, []baseVersion{oldBase, compacted}, false, false)
		require.Nil(t, compact)
		require.Len(t, gc, 1)
		require.Equal(t, "base-1", gc[0].baseID)
	})

	t.Run("active job blocks", func(t *testing.T) {
		compact, gc := planCompaction(now, 10, []baseVersion{oldBase, newBase}, true, false)
		require.Nil(t, compact)
		require.Empty(t, gc)
	})

	t.Run("running compaction blocks", func(t *testing.T) {
		compact, gc := planCompaction(now, 10, []baseVersion{oldBase, newBase}, false, true)
		require.Nil(t, compact)
		require.Empty(t, gc)
	})
}

func TestIsCompactionJob(t *testing.T) {
	job := &importer.JobInfo{Step: importer.JobStepCompacting}
	require.True(t, isCompactionJob(job))

	job = &importer.JobInfo{Parameters: importer.ImportParameters{Options: map[string]any{"compaction": true}}}
	require.True(t, isCompactionJob(job))

	job = &importer.JobInfo{Parameters: importer.ImportParameters{Options: map[string]any{"compaction": "true"}}}
	require.True(t, isCompactionJob(job))

	job = &importer.JobInfo{Parameters: importer.ImportParameters{Options: map[string]any{"compaction": false}}}
	require.False(t, isCompactionJob(job))
}
