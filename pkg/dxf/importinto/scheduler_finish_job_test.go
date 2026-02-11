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
	"context"
	"testing"
	"time"

	"github.com/ngaut/pools"
	"github.com/pingcap/tidb/pkg/dxf/framework/proto"
	"github.com/pingcap/tidb/pkg/dxf/framework/storage"
	"github.com/pingcap/tidb/pkg/dxf/importinto"
	"github.com/pingcap/tidb/pkg/executor/importer"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestFinishJobUpdatesSummary(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (id int primary key)")
	tbl, err := dom.InfoSchema().TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("t"))
	require.NoError(t, err)

	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalDistTask)
	exec := tk.Session().GetSQLExecutor()
	jobID, err := importer.CreateJob(ctx, exec, "test", "t", tbl.Meta().ID, "root@%", "", &importer.ImportParameters{}, 0)
	require.NoError(t, err)
	require.NoError(t, importer.StartJob(ctx, exec, jobID, importer.JobStepImporting))

	pool := pools.NewResourcePool(func() (pools.Resource, error) {
		return testkit.NewTestKit(t, store).Session(), nil
	}, 1, 1, time.Second)
	t.Cleanup(func() {
		pool.Close()
	})
	taskMgr := storage.NewTaskManager(pool)

	task := &proto.Task{TaskBase: proto.TaskBase{ID: 1}}
	taskMeta := &importinto.TaskMeta{
		JobID:   jobID,
		Plan:    importer.Plan{TableInfo: tbl.Meta()},
		Summary: importer.Summary{ImportedRows: 1},
	}
	require.NoError(t, importinto.FinishJobForTest(ctx, taskMgr, task, taskMeta, zap.NewNop()))

	gotJob, err := importer.GetJob(ctx, exec, jobID, "root@%", true)
	require.NoError(t, err)
	require.Equal(t, importer.JobStatusFinished, gotJob.Status)
	require.Equal(t, int64(1), gotJob.Summary.ImportedRows)
}
