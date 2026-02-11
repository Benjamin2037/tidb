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

	"github.com/pingcap/tidb/pkg/domain/infosync"
	"github.com/pingcap/tidb/pkg/domain/serverinfo"
	"github.com/pingcap/tidb/pkg/dxf/framework/proto"
	"github.com/pingcap/tidb/pkg/executor/importer"
	"github.com/pingcap/tidb/pkg/keyspace"
	"github.com/stretchr/testify/require"
)

func TestSubmitTaskDelegation(t *testing.T) {
	oldFn := submitTaskFn
	t.Cleanup(func() {
		submitTaskFn = oldFn
	})

	var gotInstance *serverinfo.ServerInfo
	var gotChunkMap map[int32][]importer.Chunk
	submitTaskFn = func(
		ctx context.Context,
		plan *importer.Plan,
		stmt string,
		instance *serverinfo.ServerInfo,
		chunkMap map[int32][]importer.Chunk,
	) (int64, *proto.TaskBase, error) {
		gotInstance = instance
		gotChunkMap = chunkMap
		return 10, &proto.TaskBase{ID: 11}, nil
	}

	_, err := infosync.GlobalInfoSyncerInit(context.Background(), "test", func() uint64 { return 1 }, nil, nil, nil, nil, keyspace.CodecV1, true, nil)
	require.NoError(t, err)
	jobID, task, err := SubmitStandaloneTask(context.Background(), &importer.Plan{}, "stmt", map[int32][]importer.Chunk{1: {}})
	require.NoError(t, err)
	require.Equal(t, int64(10), jobID)
	require.Equal(t, int64(11), task.ID)
	require.NotNil(t, gotInstance)
	require.NotNil(t, gotChunkMap)

	gotInstance = nil
	gotChunkMap = nil
	jobID, task, err = SubmitTask(context.Background(), &importer.Plan{}, "stmt")
	require.NoError(t, err)
	require.Equal(t, int64(10), jobID)
	require.Equal(t, int64(11), task.ID)
	require.Nil(t, gotInstance)
	require.Nil(t, gotChunkMap)
}
