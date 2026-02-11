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
	"testing"

	dxfmock "github.com/pingcap/tidb/pkg/dxf/framework/mock"
	frameworkscheduler "github.com/pingcap/tidb/pkg/dxf/framework/scheduler"
	"github.com/pingcap/tidb/pkg/dxf/framework/proto"
	"github.com/pingcap/tidb/pkg/executor/importer"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/sessionctx"
	utilmock "github.com/pingcap/tidb/pkg/util/mock"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/mock/gomock"
)

func TestFinishJobError(t *testing.T) {
	oldRetry := frameworkscheduler.RetrySQLTimes
	frameworkscheduler.RetrySQLTimes = 1
	t.Cleanup(func() {
		frameworkscheduler.RetrySQLTimes = oldRetry
	})

	ctrl := gomock.NewController(t)
	taskMgr := dxfmock.NewMockTaskManager(ctrl)
	taskMgr.EXPECT().WithNewTxn(gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, fn func(sessionctx.Context) error) error {
			sctx := utilmock.NewContext()
			return fn(sctx)
		},
	)

	sch := &importScheduler{taskKSTaskMgr: taskMgr}

	taskMeta := &TaskMeta{
		JobID: 1,
		Plan: importer.Plan{
			TableInfo: &model.TableInfo{ID: 1},
		},
		Summary: importer.Summary{ImportedRows: 1},
	}
	task := &proto.Task{TaskBase: proto.TaskBase{ID: 1}}

	err := sch.finishJob(context.Background(), zap.NewNop(), task, taskMeta)
	require.Error(t, err)
}
