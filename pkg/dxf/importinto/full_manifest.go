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
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"sort"
	"strconv"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/dxf/framework/proto"
	dxfstorage "github.com/pingcap/tidb/pkg/dxf/framework/storage"
	"github.com/pingcap/tidb/pkg/executor/importer"
	"github.com/pingcap/tidb/pkg/lightning/backend/external"
	"go.uber.org/zap"
)

func defaultBaseID(jobID int64) string {
	return "base-" + strconv.FormatInt(jobID, 10)
}

func (p *postProcessStepExecutor) maybeWriteFullBaseManifest(ctx context.Context, logger *zap.Logger) error {
	if p.taskMeta == nil || !p.taskMeta.Plan.IsUpsertFull() {
		return nil
	}
	manifest, err := buildBaseManifestForFull(ctx, p.taskID, p.taskMeta)
	if err != nil {
		return err
	}
	baseURI := p.taskMeta.Plan.BaseURI
	if baseURI == "" {
		baseURI = p.taskMeta.Plan.CloudStorageURI
	}
	store, err := importer.GetSortStore(ctx, baseURI)
	if err != nil {
		return err
	}
	defer store.Close()
	manifestPath := BaseManifestPath(manifest.BaseID)
	if err := WriteBaseManifest(ctx, store, manifestPath, manifest); err != nil {
		return err
	}
	p.taskMeta.Summary.BaseID = manifest.BaseID
	p.taskMeta.Summary.BaseURI = baseURI
	p.taskMeta.Summary.BaseManifestPath = manifestPath
	logger.Info("base manifest written", zap.String("base-id", manifest.BaseID), zap.String("path", manifestPath))
	return nil
}

func buildBaseManifestForFull(
	ctx context.Context,
	taskID int64,
	taskMeta *TaskMeta,
) (*BaseManifest, error) {
	if taskMeta == nil {
		return nil, errors.New("task meta is nil")
	}
	plan := taskMeta.Plan
	taskMgr, err := dxfstorage.GetDXFSvcTaskMgr()
	if err != nil {
		return nil, errors.Trace(err)
	}

	subtasks, err := taskMgr.GetAllSubtasksByStepAndState(ctx, taskID, proto.ImportStepWriteAndIngest, proto.SubtaskStateSucceed)
	if err != nil {
		return nil, errors.Trace(err)
	}

	sortStore, err := importer.GetSortStore(ctx, plan.CloudStorageURI)
	if err != nil {
		return nil, errors.Trace(err)
	}
	defer sortStore.Close()

	dataMetas := make([]WriteIngestStepMeta, 0, len(subtasks))
	for _, subtask := range subtasks {
		var meta WriteIngestStepMeta
		if err := json.Unmarshal(subtask.Meta, &meta); err != nil {
			return nil, errors.Trace(err)
		}
		if meta.ExternalPath != "" {
			if err := meta.ReadJSONFromExternalStorage(ctx, sortStore, &meta); err != nil {
				return nil, errors.Trace(err)
			}
		}
		if meta.KVGroup != external.DataKVGroup {
			continue
		}
		dataMetas = append(dataMetas, meta)
	}

	sort.Slice(dataMetas, func(i, j int) bool {
		return bytes.Compare(dataMetas[i].StartKey, dataMetas[j].StartKey) < 0
	})

	regions := make([]BaseRegionMeta, 0, len(dataMetas))
	for _, meta := range dataMetas {
		regions = append(regions, BaseRegionMeta{
			StartKey:  hex.EncodeToString(meta.StartKey),
			EndKey:    hex.EncodeToString(meta.EndKey),
			DataFiles: meta.DataFiles,
			StatFiles: meta.StatFiles,
		})
	}

	baseID := defaultBaseID(taskMeta.JobID)
	baseURI := plan.BaseURI
	manifest := &BaseManifest{
		TableID:    plan.TableInfo.ID,
		BaseID:     baseID,
		BaseURI:    baseURI,
		CreateTime: time.Now().UTC(),
		RowCount:   uint64(taskMeta.Summary.ImportedRows),
		Regions:    regions,
	}
	return manifest, nil
}
