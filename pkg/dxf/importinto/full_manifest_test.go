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

package importinto_test

import (
	"encoding/json"
	"path/filepath"
	"testing"

	"github.com/pingcap/tidb/pkg/dxf/framework/proto"
	"github.com/pingcap/tidb/pkg/dxf/framework/testutil"
	"github.com/pingcap/tidb/pkg/dxf/importinto"
	"github.com/pingcap/tidb/pkg/executor/importer"
	"github.com/pingcap/tidb/pkg/lightning/backend/external"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/stretchr/testify/require"
)

func TestBuildBaseManifestForFull(t *testing.T) {
	store, taskMgr, ctx := testutil.InitTableTest(t)
	t.Cleanup(func() {
		require.NoError(t, store.Close())
	})
	require.NoError(t, taskMgr.InitMeta(ctx, ":4000", ""))

	baseDir := t.TempDir()
	baseURI := "local://" + filepath.ToSlash(baseDir)
	taskMeta := &importinto.TaskMeta{
		JobID: 7,
		Plan: importer.Plan{
			CloudStorageURI: baseURI,
			BaseURI:         baseURI,
			TableInfo:       &model.TableInfo{ID: 100},
		},
		Summary: importer.Summary{ImportedRows: 3},
	}
	metaBytes, err := json.Marshal(taskMeta)
	require.NoError(t, err)
	taskID, err := taskMgr.CreateTask(ctx, importinto.TaskKey(taskMeta.JobID), proto.ImportInto, "", 1, "", 0, proto.ExtraParams{}, metaBytes)
	require.NoError(t, err)

	writeMeta := func(start, end byte, dataFile, statFile string, kvGroup string) {
		meta := &importinto.WriteIngestStepMeta{
			KVGroup: kvGroup,
			SortedKVMeta: external.SortedKVMeta{
				StartKey: []byte{start},
				EndKey:   []byte{end},
			},
			DataFiles: []string{dataFile},
			StatFiles: []string{statFile},
		}
		b, err := json.Marshal(meta)
		require.NoError(t, err)
		testutil.InsertSubtask(t, taskMgr, taskID, proto.ImportStepWriteAndIngest, "exec", b, proto.SubtaskStateSucceed, proto.ImportInto, 1)
	}

	// Out of order to test sorting; index kv group should be ignored.
	writeMeta(0x10, 0x20, "data-2", "stat-2", external.DataKVGroup)
	writeMeta(0x00, 0x10, "data-1", "stat-1", external.DataKVGroup)
	writeMeta(0x00, 0x10, "index-1", "index-1.stat", "1")

	manifest, err := importinto.BuildBaseManifestForFullForTest(ctx, taskID, taskMeta)
	require.NoError(t, err)
	require.Equal(t, "base-7", manifest.BaseID)
	require.Equal(t, baseURI, manifest.BaseURI)
	require.Equal(t, int64(100), manifest.TableID)
	require.Equal(t, uint64(3), manifest.RowCount)
	require.Len(t, manifest.Regions, 2)
	require.Equal(t, "00", manifest.Regions[0].StartKey)
	require.Equal(t, "10", manifest.Regions[0].EndKey)
	require.Equal(t, []string{"data-1"}, manifest.Regions[0].DataFiles)
	require.Equal(t, []string{"stat-1"}, manifest.Regions[0].StatFiles)
	require.Equal(t, "10", manifest.Regions[1].StartKey)
	require.Equal(t, "20", manifest.Regions[1].EndKey)
}
