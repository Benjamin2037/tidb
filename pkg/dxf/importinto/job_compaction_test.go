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

	"github.com/pingcap/tidb/pkg/dxf/framework/proto"
	"github.com/pingcap/tidb/pkg/domain/serverinfo"
	"github.com/pingcap/tidb/pkg/executor/importer"
	"github.com/pingcap/tidb/pkg/lightning/common"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/stretchr/testify/require"
)

func TestSubmitCompactionTaskBuildPlan(t *testing.T) {
	backup := submitTaskFn
	t.Cleanup(func() {
		submitTaskFn = backup
	})

	var gotPlan *importer.Plan
	var gotStmt string
	var gotChunkMap map[int32][]importer.Chunk

	submitTaskFn = func(ctx context.Context, plan *importer.Plan, stmt string, instance *serverinfo.ServerInfo, chunkMap map[int32][]importer.Chunk) (int64, *proto.TaskBase, error) {
		gotPlan = plan
		gotStmt = stmt
		gotChunkMap = chunkMap
		return 123, &proto.TaskBase{ID: 456, Key: "compaction-task"}, nil
	}

	ctx := context.Background()
	_, _, err := SubmitCompactionTask(ctx, "test", 1, nil, "base-1", "local://", "", "user", "group")
	require.Error(t, err)

	tbl := &model.TableInfo{ID: 100, Name: ast.NewCIStr("t")}
	jobID, task, err := SubmitCompactionTask(ctx, "test", 1, tbl, "base-1", "", "", "user", "group")
	require.NoError(t, err)
	require.Equal(t, int64(123), jobID)
	require.NotNil(t, task)
	require.NotNil(t, gotPlan)
	require.Equal(t, importer.UpsertModeDelta, gotPlan.UpsertMode)
	require.Equal(t, importer.MergeStrategyLastWriteWins, gotPlan.MergeStrategy)
	require.Equal(t, "base-1", gotPlan.BaseVersion)
	require.Equal(t, gotPlan.CloudStorageURI, gotPlan.BaseURI)
	require.Equal(t, "test", gotPlan.DBName)
	require.Equal(t, int64(1), gotPlan.DBID)
	require.Equal(t, tbl, gotPlan.TableInfo)
	require.Equal(t, "user", gotPlan.User)
	require.Equal(t, "group", gotPlan.GroupKey)
	require.Equal(t, true, gotPlan.Parameters.Options["compaction"])
	require.Contains(t, gotStmt, "/* compaction */")
	require.NotNil(t, gotChunkMap)
	require.Contains(t, gotChunkMap, int32(common.IndexEngineID))
	require.Len(t, gotChunkMap[int32(common.IndexEngineID)], 0)
}
