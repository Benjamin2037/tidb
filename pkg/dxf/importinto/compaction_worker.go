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
	"strings"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/dxf/framework/storage"
	"github.com/pingcap/tidb/pkg/executor/importer"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/keyspace"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
)

var (
	compactionCheckInterval = time.Hour
	compactionTimeNow       = time.Now
)

func init() {
	domain.StartImportIntoCompactionWorker = startImportIntoCompactionWorker
}

func startImportIntoCompactionWorker(do *domain.Domain, run func(exec func(), label string), exit <-chan struct{}) {
	run(func() {
		importIntoCompactionWorker(do, exit)
	}, "importIntoCompactionWorker")
}

func importIntoCompactionWorker(do *domain.Domain, exit <-chan struct{}) {
	defer util.Recover(metrics.LabelDomain, "importIntoCompactionWorker", nil, false)

	logger := logutil.BgLogger().With(zap.String("keyspace", keyspace.GetKeyspaceNameBySettings()))
	ticker := time.NewTicker(compactionCheckInterval)
	defer func() {
		ticker.Stop()
		logger.Info("import into compaction worker exited")
	}()

	for {
		select {
		case <-exit:
			return
		case <-ticker.C:
			if !do.DDL().OwnerManager().IsOwner() {
				continue
			}
			if err := runImportIntoCompactionOnce(do, logger); err != nil {
				logger.Warn("import into compaction tick failed", zap.Error(err))
			}
		}
	}
}

type baseVersion struct {
	job          *importer.JobInfo
	baseID       string
	baseURI      string
	manifestPath string
	createTime   time.Time
	isCompaction bool
}

type tableCompactionState struct {
	tableID              int64
	schemaName           string
	tableName            string
	bases                []baseVersion
	hasActiveJob         bool
	hasRunningCompaction bool
}

func runImportIntoCompactionOnce(do *domain.Domain, logger *zap.Logger) error {
	retentionDays := int(vardef.ImportIntoBaseRetentionDays.Load())
	if retentionDays <= 0 {
		return nil
	}
	taskMgr, err := storage.GetTaskManager()
	if err != nil {
		return err
	}
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalDistTask)
	var jobs []*importer.JobInfo
	if err := taskMgr.WithNewSession(func(se sessionctx.Context) error {
		exec := se.GetSQLExecutor()
		var err2 error
		jobs, err2 = importer.GetAllViewableJobs(ctx, exec, "", true)
		return err2
	}); err != nil {
		return err
	}

	states := buildTableCompactionStates(jobs)
	if len(states) == 0 {
		return nil
	}
	is := do.InfoSchema()
	now := compactionTimeNow().UTC()

	for _, state := range states {
		if state.hasActiveJob {
			continue
		}
		tbl, dbInfo, ok := resolveTableInfo(is, state)
		if !ok {
			continue
		}
		bases, err := loadBaseVersions(ctx, state.bases, logger)
		if err != nil {
			logger.Warn("load base manifest failed", zap.Int64("table-id", state.tableID), zap.Error(err))
			continue
		}
		compactBase, gcBases := planCompaction(now, retentionDays, bases, state.hasActiveJob, state.hasRunningCompaction)
		if compactBase != nil {
			if _, _, err := SubmitCompactionTask(
				ctx,
				dbInfo.Name.O,
				dbInfo.ID,
				tbl.Meta(),
				compactBase.baseID,
				compactBase.baseURI,
				"",
				"system@internal",
				"",
			); err != nil {
				logger.Warn("submit compaction task failed",
					zap.Int64("table-id", state.tableID),
					zap.String("base-id", compactBase.baseID),
					zap.Error(err),
				)
				continue
			}
			logger.Info("submitted base compaction task",
				zap.Int64("table-id", state.tableID),
				zap.String("base-id", compactBase.baseID),
				zap.String("base-uri", compactBase.baseURI),
			)
		}
		if len(gcBases) > 0 {
			if err := gcBaseVersions(ctx, gcBases, logger, state.tableID); err != nil {
				logger.Warn("gc base versions failed", zap.Int64("table-id", state.tableID), zap.Error(err))
			}
		}
	}
	return nil
}

func buildTableCompactionStates(jobs []*importer.JobInfo) map[int64]*tableCompactionState {
	states := make(map[int64]*tableCompactionState, len(jobs))
	for _, job := range jobs {
		if job == nil || job.TableID == 0 {
			continue
		}
		state, ok := states[job.TableID]
		if !ok {
			state = &tableCompactionState{
				tableID:    job.TableID,
				schemaName: job.TableSchema,
				tableName:  job.TableName,
			}
			states[job.TableID] = state
		}
		if job.CanCancel() {
			state.hasActiveJob = true
			if isCompactionJob(job) {
				state.hasRunningCompaction = true
			}
		}
		if job.Status == importer.JobStatusFinished && job.Summary != nil && job.Summary.BaseID != "" {
			state.bases = append(state.bases, baseVersion{
				job:          job,
				baseID:       job.Summary.BaseID,
				baseURI:      job.Summary.BaseURI,
				manifestPath: job.Summary.BaseManifestPath,
				isCompaction: isCompactionJob(job),
			})
		}
	}
	return states
}

func resolveTableInfo(is infoschema.InfoSchema, state *tableCompactionState) (table.Table, *model.DBInfo, bool) {
	tbl, ok := is.TableByID(context.Background(), state.tableID)
	if !ok {
		return nil, nil, false
	}
	dbInfo, ok := is.SchemaByName(model.NewCIStr(state.schemaName))
	if !ok {
		return nil, nil, false
	}
	return tbl, dbInfo, true
}

func loadBaseVersions(ctx context.Context, bases []baseVersion, logger *zap.Logger) ([]baseVersion, error) {
	out := make([]baseVersion, 0, len(bases))
	for _, base := range bases {
		if err := fillBaseVersion(ctx, &base); err != nil {
			logger.Debug("skip base version", zap.String("base-id", base.baseID), zap.Error(err))
			continue
		}
		if base.createTime.IsZero() {
			continue
		}
		out = append(out, base)
	}
	if len(out) == 0 {
		return nil, nil
	}
	return out, nil
}

func fillBaseVersion(ctx context.Context, base *baseVersion) error {
	if base == nil {
		return errors.New("base version is nil")
	}
	baseURI := strings.TrimSpace(base.baseURI)
	if baseURI == "" && base.job != nil {
		baseURI = strings.TrimSpace(getOptionString(base.job.Parameters.Options, "base_uri"))
	}
	if baseURI == "" && base.job != nil {
		baseURI = strings.TrimSpace(base.job.Parameters.FileLocation)
	}
	if baseURI == "" {
		return errors.New("base uri is empty")
	}
	manifestPath := base.manifestPath
	if manifestPath == "" {
		manifestPath = BaseManifestPath(base.baseID)
	}
	store, err := importer.GetSortStore(ctx, baseURI)
	if err != nil {
		return err
	}
	defer store.Close()
	manifest, err := ReadBaseManifest(ctx, store, manifestPath)
	if err != nil {
		return err
	}
	base.baseURI = baseURI
	base.manifestPath = manifestPath
	base.createTime = manifest.CreateTime
	if base.createTime.IsZero() && base.job != nil && !base.job.CreateTime.IsZero() {
		base.createTime = base.job.CreateTime.GoTime(time.UTC)
	}
	return nil
}

func getOptionString(opts map[string]any, key string) string {
	if opts == nil {
		return ""
	}
	val, ok := opts[key]
	if !ok {
		return ""
	}
	switch v := val.(type) {
	case string:
		return v
	case []byte:
		return string(v)
	default:
		return ""
	}
}

func isCompactionJob(job *importer.JobInfo) bool {
	if job == nil {
		return false
	}
	if job.Step == importer.JobStepCompacting {
		return true
	}
	if job.Parameters.Options == nil {
		return false
	}
	val, ok := job.Parameters.Options["compaction"]
	if !ok {
		return false
	}
	if b, ok := val.(bool); ok {
		return b
	}
	if s, ok := val.(string); ok {
		return strings.EqualFold(s, "true")
	}
	return false
}

func planCompaction(now time.Time, retentionDays int, bases []baseVersion, hasActiveJob, hasRunningCompaction bool) (*baseVersion, []baseVersion) {
	if retentionDays <= 0 || len(bases) == 0 {
		return nil, nil
	}
	cutoff := now.Add(-time.Duration(retentionDays) * 24 * time.Hour)
	latestIdx := 0
	oldExists := false
	for i := range bases {
		ct := bases[i].createTime
		if ct.Before(cutoff) {
			oldExists = true
		}
		if ct.After(bases[latestIdx].createTime) {
			latestIdx = i
		}
	}
	if !oldExists || hasActiveJob {
		return nil, nil
	}
	latest := &bases[latestIdx]
	var compactBase *baseVersion
	if !hasRunningCompaction && !latest.isCompaction {
		compactBase = latest
	}
	var gcBases []baseVersion
	if !hasRunningCompaction && latest.isCompaction {
		for i := range bases {
			if bases[i].baseID == latest.baseID {
				continue
			}
			if bases[i].createTime.Before(cutoff) {
				gcBases = append(gcBases, bases[i])
			}
		}
	}
	return compactBase, gcBases
}

func gcBaseVersions(ctx context.Context, bases []baseVersion, logger *zap.Logger, tableID int64) error {
	for _, base := range bases {
		if base.baseURI == "" {
			continue
		}
		store, err := importer.GetSortStore(ctx, base.baseURI)
		if err != nil {
			return err
		}
		err = RemoveBaseVersion(ctx, store, base.baseID)
		store.Close()
		if err != nil {
			return err
		}
		logger.Info("removed base version",
			zap.Int64("table-id", tableID),
			zap.String("base-id", base.baseID),
			zap.String("base-uri", base.baseURI),
		)
	}
	return nil
}
