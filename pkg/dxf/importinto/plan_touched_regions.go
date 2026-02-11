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

	"github.com/pingcap/errors"
	dxfhandle "github.com/pingcap/tidb/pkg/dxf/framework/handle"
	"github.com/pingcap/tidb/pkg/dxf/framework/proto"
	"github.com/pingcap/tidb/pkg/dxf/framework/taskexecutor"
	"github.com/pingcap/tidb/pkg/dxf/framework/taskexecutor/execute"
	"github.com/pingcap/tidb/pkg/lightning/log"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

type planTouchedRegionsStepExecutor struct {
	taskexecutor.BaseStepExecutor
	task     *proto.TaskBase
	taskMeta *TaskMeta
	logger   *zap.Logger

	summary execute.SubtaskSummary
}

var _ execute.StepExecutor = &planTouchedRegionsStepExecutor{}

// NewPlanTouchedRegionsStepExecutor creates a new executor for planning touched regions.
func NewPlanTouchedRegionsStepExecutor(
	task *proto.TaskBase,
	taskMeta *TaskMeta,
	logger *zap.Logger,
) execute.StepExecutor {
	return &planTouchedRegionsStepExecutor{
		task:     task,
		taskMeta: taskMeta,
		logger:   logger,
	}
}

func (*planTouchedRegionsStepExecutor) Init(context.Context) error {
	return nil
}

func (e *planTouchedRegionsStepExecutor) RunSubtask(ctx context.Context, subtask *proto.Subtask) (err error) {
	logger := e.logger.With(zap.Int64("subtask-id", subtask.ID))
	task := log.BeginTask(logger, "run subtask")
	defer func() {
		task.End(zapcore.ErrorLevel, err)
	}()

	stMeta := &PlanTouchedRegionsStepMeta{}
	if err = json.Unmarshal(subtask.Meta, stMeta); err != nil {
		return errors.Trace(err)
	}

	deltaURI := stMeta.DeltaStoreURI
	if deltaURI == "" {
		deltaURI = e.taskMeta.Plan.CloudStorageURI
	}
	accessRecDelta, deltaStore, err := dxfhandle.NewObjStoreWithRecording(ctx, deltaURI)
	if err != nil {
		return err
	}
	defer func() {
		deltaStore.Close()
		e.summary.MergeObjStoreRequests(&accessRecDelta.Requests)
		e.GetMeterRecorder().MergeObjStoreAccess(accessRecDelta)
	}()

	if stMeta.ExternalPath != "" {
		if err := stMeta.ReadJSONFromExternalStorage(ctx, deltaStore, stMeta); err != nil {
			return errors.Trace(err)
		}
	}

	baseURI := stMeta.BaseURI
	if baseURI == "" {
		baseURI = e.taskMeta.Plan.CloudStorageURI
	}
	accessRecBase, baseStore, err := dxfhandle.NewObjStoreWithRecording(ctx, baseURI)
	if err != nil {
		return err
	}
	defer func() {
		baseStore.Close()
		e.summary.MergeObjStoreRequests(&accessRecBase.Requests)
		e.GetMeterRecorder().MergeObjStoreAccess(accessRecBase)
	}()

	deltaStart, err := decodeHexKey(stMeta.DeltaStartKey)
	if err != nil {
		return errors.Annotate(err, "decode delta start key")
	}
	deltaEnd, err := decodeHexKey(stMeta.DeltaEndKey)
	if err != nil {
		return errors.Annotate(err, "decode delta end key")
	}
	// deltaRangeKnown means the delta job exposes a key range; otherwise we may only
	// know that delta data files exist without precise range boundaries.
	deltaRangeKnown := len(deltaStart) > 0 || len(deltaEnd) > 0

	if stMeta.BaseManifestPath == "" {
		logger.Warn("base manifest missing, fallback to remote coprocessor scan on S3 SSTs")
		changedRegions := make([]ChangedRegionMeta, 0, 1)
		// Fallback to full range so rebuild does not miss any base data.
		changedRegions = append(changedRegions, ChangedRegionMeta{
			StartKey: "",
			EndKey:   "",
		})
		out := &ChangedRegionsManifest{
			BaseID:  stMeta.BaseID,
			JobID:   e.taskMeta.JobID,
			Regions: changedRegions,
		}
		if err := WriteChangedRegionsManifest(ctx, deltaStore, stMeta.ChangedRegionsPath, out); err != nil {
			return errors.Trace(err)
		}
		return nil
	}
	baseManifest, err := ReadBaseManifest(ctx, baseStore, stMeta.BaseManifestPath)
	if err != nil {
		return errors.Trace(err)
	}
	changedRegions := make([]ChangedRegionMeta, 0)
	for _, region := range baseManifest.Regions {
		regionStart, err := decodeHexKey(region.StartKey)
		if err != nil {
			return errors.Annotate(err, "decode base region start key")
		}
		regionEnd, err := decodeHexKey(region.EndKey)
		if err != nil {
			return errors.Annotate(err, "decode base region end key")
		}
		overlap := false
		switch {
		case deltaRangeKnown:
			// Fast path: use range overlap when delta exposes a range.
			overlap = rangesOverlap(regionStart, regionEnd, deltaStart, deltaEnd)
		case len(stMeta.DeltaDataFiles) > 0:
			// Conservative path: if delta files exist but range is unknown, mark all regions.
			overlap = true
		}
		if overlap {
			changedRegions = append(changedRegions, ChangedRegionMeta{
				StartKey: region.StartKey,
				EndKey:   region.EndKey,
			})
		}
	}
	if len(changedRegions) == 0 && deltaRangeKnown {
		changedRegions = append(changedRegions, ChangedRegionMeta{
			StartKey: stMeta.DeltaStartKey,
			EndKey:   stMeta.DeltaEndKey,
		})
	}

	out := &ChangedRegionsManifest{
		BaseID:  stMeta.BaseID,
		JobID:   e.taskMeta.JobID,
		Regions: changedRegions,
	}
	if err := WriteChangedRegionsManifest(ctx, deltaStore, stMeta.ChangedRegionsPath, out); err != nil {
		return errors.Trace(err)
	}

	return nil
}

func (e *planTouchedRegionsStepExecutor) Cleanup(ctx context.Context) error {
	return e.BaseStepExecutor.Cleanup(ctx)
}

func (e *planTouchedRegionsStepExecutor) RealtimeSummary() *execute.SubtaskSummary {
	e.summary.Update()
	return &e.summary
}

func (e *planTouchedRegionsStepExecutor) ResetSummary() {
	e.summary.Reset()
}
