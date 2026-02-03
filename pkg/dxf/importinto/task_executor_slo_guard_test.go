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
	"encoding/json"
	"math"
	"testing"

	"github.com/pingcap/tidb/pkg/ddl"
	"github.com/pingcap/tidb/pkg/dxf/framework/proto"
	"github.com/pingcap/tidb/pkg/dxf/framework/taskexecutor/execute"
	"github.com/pingcap/tidb/pkg/executor/importer"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/lightning/backend/external"
	"github.com/pingcap/tidb/pkg/lightning/config"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/store/mockstore"
	"github.com/pingcap/tidb/pkg/table/tables"
	"github.com/pingcap/tidb/pkg/util/logutil"
	utilmock "github.com/pingcap/tidb/pkg/util/mock"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func newTestTableImporter(t *testing.T) *importer.TableImporter {
	store, err := mockstore.NewMockStore()
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, store.Close())
	})

	p := parser.New()
	stmt, err := p.ParseOneStmt("create table t (id int primary key)", "", "")
	require.NoError(t, err)
	sctx := utilmock.NewContext()
	tblInfo, err := ddl.MockTableInfo(sctx, stmt.(*ast.CreateTableStmt), 1)
	require.NoError(t, err)
	tblInfo.State = model.StatePublic
	for _, col := range tblInfo.Columns {
		if col.State == model.StateNone {
			col.State = model.StatePublic
		}
	}
	tbl := tables.MockTableFromMeta(tblInfo)

	plan := importer.Plan{
		DBName:         "test",
		TableInfo:      tblInfo,
		DataSourceType: importer.DataSourceTypeFile,
		Format:         importer.DataFormatCSV,
		Path:           "local://dummy.csv",
		InImportInto:   true,
	}
	astArgs, err := importer.ASTArgsFromStmt("IMPORT INTO t FROM 'local://dummy.csv'")
	require.NoError(t, err)

	controller, err := importer.NewLoadDataController(&plan, tbl, astArgs, importer.WithLogger(logutil.BgLogger()))
	require.NoError(t, err)
	tableImporter, err := importer.NewTableImporterForTest(context.Background(), controller, "task-1", store)
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = tableImporter.Close()
	})
	return tableImporter
}

func buildWriteAndIngestTaskMeta(t *testing.T) (*TaskMeta, kv.Storage) {
	store, err := mockstore.NewMockStore()
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, store.Close())
	})

	p := parser.New()
	stmt, err := p.ParseOneStmt("create table t (id int primary key)", "", "")
	require.NoError(t, err)
	sctx := utilmock.NewContext()
	tblInfo, err := ddl.MockTableInfo(sctx, stmt.(*ast.CreateTableStmt), 1)
	require.NoError(t, err)
	tblInfo.State = model.StatePublic
	for _, col := range tblInfo.Columns {
		if col.State == model.StateNone {
			col.State = model.StatePublic
		}
	}

	plan := importer.Plan{
		DBName:         "test",
		TableInfo:      tblInfo,
		DataSourceType: importer.DataSourceTypeFile,
		Format:         importer.DataFormatCSV,
		Path:           "local://dummy.csv",
		InImportInto:   true,
	}
	taskMeta := &TaskMeta{
		JobID:  1,
		Plan:   plan,
		Stmt:   "IMPORT INTO t FROM 'local://dummy.csv'",
	}
	return taskMeta, store
}

func TestApplySLOGuardRateLimit(t *testing.T) {
	tableImporter := newTestTableImporter(t)
	exec := &writeAndIngestStepExecutor{
		tableImporter: tableImporter,
		taskMeta:      &TaskMeta{JobID: 1},
	}

	setSLOGuardRuntime(1, sloGuardStateSlow, config.ByteSize(1024))
	exec.applySLOGuardRateLimit()
	require.Equal(t, 1024, tableImporter.Backend().GetWriteSpeedLimit())

	setSLOGuardRuntime(1, sloGuardStateSlow, 0)
	exec.applySLOGuardRateLimit()
	require.Equal(t, 0, tableImporter.Backend().GetWriteSpeedLimit())

	limit := config.ByteSize(math.MaxInt64)
	setSLOGuardRuntime(1, sloGuardStateSlow, limit)
	exec.applySLOGuardRateLimit()
	maxInt := int(^uint(0) >> 1)
	require.Equal(t, maxInt, tableImporter.Backend().GetWriteSpeedLimit())
}

func TestApplySLOGuardRateLimitNilImporter(t *testing.T) {
	exec := &writeAndIngestStepExecutor{}
	require.NotPanics(t, func() {
		exec.applySLOGuardRateLimit()
	})
}

func TestWriteAndIngestExecutorInitAndRunApplySLOGuard(t *testing.T) {
	taskMeta, store := buildWriteAndIngestTaskMeta(t)
	exec := &writeAndIngestStepExecutor{
		taskID:   1,
		taskMeta: taskMeta,
		store:    store,
		logger:   zap.NewNop(),
	}
	resource := &proto.StepResource{
		CPU: proto.NewAllocatable(1),
		Mem: proto.NewAllocatable(1),
	}
	execute.SetFrameworkInfo(exec, &proto.Task{TaskBase: proto.TaskBase{Step: proto.ImportStepWriteAndIngest}}, resource, nil, nil)

	setSLOGuardRuntime(taskMeta.JobID, sloGuardStateSlow, config.ByteSize(1024))
	t.Cleanup(func() {
		setSLOGuardRuntime(taskMeta.JobID, sloGuardStateNormal, 0)
	})

	require.NoError(t, exec.Init(context.Background()))
	t.Cleanup(func() {
		if exec.tableImporter != nil {
			_ = exec.tableImporter.Close()
		}
	})
	require.Equal(t, 1024, exec.tableImporter.Backend().GetWriteSpeedLimit())

	setSLOGuardRuntime(taskMeta.JobID, sloGuardStateSlow, config.ByteSize(2048))
	meta := &WriteIngestStepMeta{
		KVGroup:  external.DataKVGroup,
		StoreURI: "invalid://",
	}
	bs, err := json.Marshal(meta)
	require.NoError(t, err)
	err = exec.RunSubtask(context.Background(), &proto.Subtask{Meta: bs})
	require.Error(t, err)
	require.Equal(t, 2048, exec.tableImporter.Backend().GetWriteSpeedLimit())
}
