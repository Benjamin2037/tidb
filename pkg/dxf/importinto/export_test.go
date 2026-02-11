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

	"github.com/pingcap/tidb/pkg/dxf/framework/proto"
	"github.com/pingcap/tidb/pkg/dxf/framework/storage"
	"go.uber.org/zap"
)

// BuildBaseManifestForFullForTest exposes buildBaseManifestForFull for external tests.
func BuildBaseManifestForFullForTest(ctx context.Context, taskID int64, taskMeta *TaskMeta) (*BaseManifest, error) {
	return buildBaseManifestForFull(ctx, taskID, taskMeta)
}

// FinishJobForTest exposes finishJob for external tests.
func FinishJobForTest(ctx context.Context, taskMgr *storage.TaskManager, task *proto.Task, taskMeta *TaskMeta, logger *zap.Logger) error {
	sch := importScheduler{taskKSTaskMgr: taskMgr}
	return sch.finishJob(ctx, logger, task, taskMeta)
}
