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

package executor

import (
	"context"
	"errors"
	"testing"

	"github.com/ngaut/pools"
	perrors "github.com/pingcap/errors"
	fstorage "github.com/pingcap/tidb/pkg/dxf/framework/storage"
	"github.com/pingcap/tidb/pkg/executor/importer"
	"github.com/pingcap/tidb/pkg/executor/internal/exec"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/privilege"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/stretchr/testify/require"
)

func TestFetchShowImportIntoSLOGuardTaskManagerNotInitialized(t *testing.T) {
	sctx := mock.NewContext()
	e := &ShowExec{
		BaseExecutor: exec.NewBaseExecutor(sctx, nil, 0),
	}
	oldMgr, _ := fstorage.GetTaskManager()
	fstorage.SetTaskManager(nil)
	t.Cleanup(func() {
		fstorage.SetTaskManager(oldMgr)
	})

	err := e.fetchShowImportIntoSLOGuard(context.Background())
	require.Error(t, err)
	require.Contains(t, err.Error(), "task manager")
}

type errSessionPool struct{}

func (errSessionPool) Get() (pools.Resource, error) {
	return nil, errors.New("pool error")
}

func (errSessionPool) Put(pools.Resource) {}

func (errSessionPool) Close() {}

func TestFetchShowImportIntoSLOGuardSessionPoolError(t *testing.T) {
	sctx := mock.NewContext()
	e := &ShowExec{
		BaseExecutor: exec.NewBaseExecutor(sctx, nil, 0),
	}
	oldMgr, _ := fstorage.GetTaskManager()
	fstorage.SetTaskManager(fstorage.NewTaskManager(errSessionPool{}))
	t.Cleanup(func() {
		fstorage.SetTaskManager(oldMgr)
	})

	err := e.fetchShowImportIntoSLOGuard(context.Background())
	require.Error(t, err)
	require.Contains(t, err.Error(), "pool error")
}

func TestFetchShowImportIntoSLOGuardGlobalRequiresSuper(t *testing.T) {
	sctx := mock.NewContext()
	privilege.BindPrivilegeManager(sctx, nil)
	oldMgr, _ := fstorage.GetTaskManager()
	fstorage.SetTaskManager(fstorage.NewTaskManager(errSessionPool{}))
	t.Cleanup(func() {
		fstorage.SetTaskManager(oldMgr)
	})
	jobID := importer.SLOGuardGlobalJobID
	e := &ShowExec{
		BaseExecutor: exec.NewBaseExecutor(sctx, nil, 0),
		ImportJobID:  &jobID,
	}

	err := e.fetchShowImportIntoSLOGuard(context.Background())
	require.Error(t, err)
	originErr := perrors.Cause(err)
	tErr, ok := originErr.(*terror.Error)
	require.True(t, ok)
	sqlErr := terror.ToSQLError(tErr)
	require.Equal(t, mysql.ErrSpecificAccessDenied, int(sqlErr.Code))
}
