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

package executor_test

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/pingcap/tidb/pkg/dxf/framework/proto"
	fstorage "github.com/pingcap/tidb/pkg/dxf/framework/storage"
	"github.com/pingcap/tidb/pkg/dxf/importinto"
	"github.com/pingcap/tidb/pkg/executor/importer"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/auth"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

func TestShowImportBaseAndRegions(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (id int primary key)")
	tk.Session().GetSessionVars().User = &auth.UserIdentity{Username: "root", Hostname: "%"}

	tbl, err := dom.InfoSchema().TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("t"))
	require.NoError(t, err)

	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalImportInto)
	exec := tk.Session().(sessionctx.Context).GetSQLExecutor()
	params := &importer.ImportParameters{
		FileLocation: "local://",
		Format:       "csv",
	}
	jobID, err := importer.CreateJob(ctx, exec, "test", "t", tbl.Meta().ID, "root", "", params, 0)
	require.NoError(t, err)
	require.NoError(t, importer.StartJob(ctx, exec, jobID, importer.JobStepImporting))

	baseDir := t.TempDir()
	baseURI := "local://" + filepath.ToSlash(baseDir)
	baseStore, err := importer.GetSortStore(ctx, baseURI)
	require.NoError(t, err)
	defer baseStore.Close()

	baseID := fmt.Sprintf("base-%d", jobID)
	baseManifestPath := importinto.BaseManifestPath(baseID)
	manifest := &importinto.BaseManifest{
		TableID:    tbl.Meta().ID,
		BaseID:     baseID,
		BaseURI:    baseURI,
		RowCount:   10,
		DataBytes:  123,
		IndexBytes: 456,
		Regions: []importinto.BaseRegionMeta{
			{
				StartKey:       "00",
				EndKey:         "10",
				DataFiles:      []string{"base/data/1"},
				StatFiles:      []string{"base/data/1.stat"},
				IndexFiles:     []string{"base/index/1/1"},
				IndexStatFiles: []string{"base/index/1/1.stat"},
				KVBytes:        123,
			},
		},
	}
	require.NoError(t, importinto.WriteBaseManifest(ctx, baseStore, baseManifestPath, manifest))

	changedPath := importinto.ChangedRegionsPath(jobID)
	changed := &importinto.ChangedRegionsManifest{
		BaseID: baseID,
		JobID:  jobID,
		Regions: []importinto.ChangedRegionMeta{
			{StartKey: "00", EndKey: "10", ChangedRows: 2, KVBytes: 123},
		},
	}
	require.NoError(t, importinto.WriteChangedRegionsManifest(ctx, baseStore, changedPath, changed))

	summary := &importer.Summary{
		BaseID:             baseID,
		BaseURI:            baseURI,
		BaseManifestPath:   baseManifestPath,
		ChangedRegionsPath: changedPath,
	}
	require.NoError(t, importer.FinishJob(ctx, exec, jobID, summary))

	taskMgr, err := fstorage.GetTaskManager()
	require.NoError(t, err)
	taskMeta := importinto.TaskMeta{
		JobID: jobID,
		Plan: importer.Plan{
			CloudStorageURI: baseURI,
			BaseURI:         baseURI,
		},
	}
	metaBytes, err := json.Marshal(taskMeta)
	require.NoError(t, err)
	_, err = taskMgr.CreateTask(ctx, importinto.TaskKey(jobID), proto.ImportInto, "", 1, "", 0, proto.ExtraParams{}, metaBytes)
	require.NoError(t, err)

	rows := tk.MustQuery("show import bases").Rows()
	found := false
	for _, row := range rows {
		if row[0] == baseID {
			found = true
			require.Equal(t, fmt.Sprintf("%d", jobID), row[1])
			require.Equal(t, "test", row[2])
			require.Equal(t, "t", row[3])
			require.Equal(t, baseURI, row[5])
			require.Equal(t, baseManifestPath, row[6])
			require.Equal(t, "10", row[9])
			require.Equal(t, "123", row[10])
			require.Equal(t, "456", row[11])
			break
		}
	}
	require.True(t, found)

	tk.MustQuery(fmt.Sprintf("show import regions base '%s'", baseID)).
		Check(testkit.Rows("00 10 [\"base/data/1\"] [\"base/data/1.stat\"] [\"base/index/1/1\"]  123"))

	tk.MustQuery(fmt.Sprintf("show import changed regions job %d", jobID)).
		Check(testkit.Rows(fmt.Sprintf("%d %s 00 10 2 123", jobID, baseID)))
}
