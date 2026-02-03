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
	"context"
	"path/filepath"
	"testing"

	"github.com/pingcap/tidb/pkg/dxf/framework/proto"
	"github.com/pingcap/tidb/pkg/dxf/framework/taskexecutor/execute"
	"github.com/pingcap/tidb/pkg/executor/importer"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestPlanTouchedRegions(t *testing.T) {
	ctx := context.Background()

	newExecutor := func(t *testing.T, meta *PlanTouchedRegionsStepMeta) *planTouchedRegionsStepExecutor {
		taskMeta := &TaskMeta{
			JobID: 1,
			Plan: importer.Plan{
				CloudStorageURI: meta.DeltaStoreURI,
				BaseURI:         meta.BaseURI,
			},
		}
		task := &proto.Task{TaskBase: proto.TaskBase{Step: proto.ImportStepPlanTouchedRegions}}
		exec := NewPlanTouchedRegionsStepExecutor(&task.TaskBase, taskMeta, zap.NewNop()).(*planTouchedRegionsStepExecutor)
		execute.SetFrameworkInfo(exec, task, nil, nil, nil)
		return exec
	}

	t.Run("overlap with base regions", func(t *testing.T) {
		baseDir := t.TempDir()
		baseURI := "local://" + filepath.ToSlash(baseDir)
		store, err := importer.GetSortStore(ctx, baseURI)
		require.NoError(t, err)
		defer store.Close()

		baseID := "base-1"
		baseManifestPath := BaseManifestPath(baseID)
		baseManifest := &BaseManifest{
			TableID: 1,
			BaseID:  baseID,
			BaseURI: baseURI,
			Regions: []BaseRegionMeta{
				{StartKey: "00", EndKey: "10"},
				{StartKey: "10", EndKey: "20"},
			},
		}
		require.NoError(t, WriteBaseManifest(ctx, store, baseManifestPath, baseManifest))

		meta := &PlanTouchedRegionsStepMeta{
			BaseID:             baseID,
			BaseURI:            baseURI,
			BaseManifestPath:   baseManifestPath,
			DeltaStoreURI:      baseURI,
			DeltaStartKey:      "05",
			DeltaEndKey:        "08",
			ChangedRegionsPath: ChangedRegionsPath(1),
		}
		bytes, err := meta.Marshal()
		require.NoError(t, err)

		exec := newExecutor(t, meta)
		require.NoError(t, exec.RunSubtask(ctx, &proto.Subtask{Meta: bytes}))

		out, err := ReadChangedRegionsManifest(ctx, store, meta.ChangedRegionsPath)
		require.NoError(t, err)
		require.Len(t, out.Regions, 1)
		require.Equal(t, "00", out.Regions[0].StartKey)
		require.Equal(t, "10", out.Regions[0].EndKey)
	})

	t.Run("no overlap uses delta range", func(t *testing.T) {
		baseDir := t.TempDir()
		baseURI := "local://" + filepath.ToSlash(baseDir)
		store, err := importer.GetSortStore(ctx, baseURI)
		require.NoError(t, err)
		defer store.Close()

		baseID := "base-1"
		baseManifestPath := BaseManifestPath(baseID)
		baseManifest := &BaseManifest{
			TableID: 1,
			BaseID:  baseID,
			BaseURI: baseURI,
			Regions: []BaseRegionMeta{
				{StartKey: "00", EndKey: "10"},
			},
		}
		require.NoError(t, WriteBaseManifest(ctx, store, baseManifestPath, baseManifest))

		meta := &PlanTouchedRegionsStepMeta{
			BaseID:             baseID,
			BaseURI:            baseURI,
			BaseManifestPath:   baseManifestPath,
			DeltaStoreURI:      baseURI,
			DeltaStartKey:      "30",
			DeltaEndKey:        "40",
			ChangedRegionsPath: ChangedRegionsPath(2),
		}
		bytes, err := meta.Marshal()
		require.NoError(t, err)

		exec := newExecutor(t, meta)
		require.NoError(t, exec.RunSubtask(ctx, &proto.Subtask{Meta: bytes}))

		out, err := ReadChangedRegionsManifest(ctx, store, meta.ChangedRegionsPath)
		require.NoError(t, err)
		require.Len(t, out.Regions, 1)
		require.Equal(t, "30", out.Regions[0].StartKey)
		require.Equal(t, "40", out.Regions[0].EndKey)
	})

	t.Run("missing base manifest falls back to full range", func(t *testing.T) {
		baseDir := t.TempDir()
		baseURI := "local://" + filepath.ToSlash(baseDir)
		store, err := importer.GetSortStore(ctx, baseURI)
		require.NoError(t, err)
		defer store.Close()

		meta := &PlanTouchedRegionsStepMeta{
			BaseID:             "base-1",
			BaseURI:            baseURI,
			BaseManifestPath:   "",
			DeltaStoreURI:      baseURI,
			ChangedRegionsPath: ChangedRegionsPath(3),
		}
		bytes, err := meta.Marshal()
		require.NoError(t, err)

		exec := newExecutor(t, meta)
		require.NoError(t, exec.RunSubtask(ctx, &proto.Subtask{Meta: bytes}))

		out, err := ReadChangedRegionsManifest(ctx, store, meta.ChangedRegionsPath)
		require.NoError(t, err)
		require.Len(t, out.Regions, 1)
		require.Equal(t, "", out.Regions[0].StartKey)
		require.Equal(t, "", out.Regions[0].EndKey)
	})
}
