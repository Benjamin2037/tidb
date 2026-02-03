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
	"errors"
	"strconv"
	"strings"
	"sync"
	"time"

	perrors "github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/dxf/framework/storage"
	"github.com/pingcap/tidb/pkg/executor/importer"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/keyspace"
	"github.com/pingcap/tidb/pkg/kv"
	lightningcfg "github.com/pingcap/tidb/pkg/lightning/config"
	"github.com/pingcap/tidb/pkg/meta/metadef"
	"github.com/pingcap/tidb/pkg/metrics"
	plannerutil "github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/timeutil"
	"go.uber.org/zap"
)

var (
	sloGuardTimeNow = time.Now
)

var errNoSLOGuardMetricData = errors.New("no slo guard metric data")

const (
	sloGuardMetricName       = "tidb_query_duration"
	sloGuardMetricQuantile   = 0.99
	sloGuardMetricLabelKey   = "sql_type"
	sloGuardMetricLabelValue = "Select"
	sloGuardWindow           = 5 * time.Minute
	sloGuardCheckInterval    = 30 * time.Second
	sloGuardSlowRatio        = 0.8
	sloGuardResumeRatio      = 0.6
)

type sloGuardThresholds struct {
	resume time.Duration
	slow   time.Duration
	pause  time.Duration
}

type sloGuardPausedSet struct {
	mu   sync.Mutex
	keys map[string]struct{}
}

var sloGuardPausedTasks = sloGuardPausedSet{keys: make(map[string]struct{})}

func init() {
	domain.StartImportIntoSLOGuardWorker = startImportIntoSLOGuardWorker
}

func startImportIntoSLOGuardWorker(do *domain.Domain, run func(exec func(), label string), exit <-chan struct{}) {
	run(func() {
		importIntoSLOGuardWorker(do, exit)
	}, "importIntoSLOGuardWorker")
}

func importIntoSLOGuardWorker(do *domain.Domain, exit <-chan struct{}) {
	defer util.Recover(metrics.LabelDomain, "importIntoSLOGuardWorker", nil, false)

	logger := logutil.BgLogger().With(zap.String("keyspace", keyspace.GetKeyspaceNameBySettings()))
	for {
		interval := sloGuardCheckInterval
		timer := time.NewTimer(interval)
		select {
		case <-exit:
			timer.Stop()
			logger.Info("import slo guard worker exited")
			return
		case <-timer.C:
			timer.Stop()
			if !do.DDL().OwnerManager().IsOwner() {
				continue
			}
			if err := runImportIntoSLOGuardOnce(do, logger); err != nil {
				if errors.Is(err, errNoSLOGuardMetricData) {
					logger.Debug("import slo guard skip, no metric data")
					continue
				}
				logger.Warn("import slo guard tick failed", zap.Error(err))
			}
		}
	}
}

type activeImportTask struct {
	jobID int64
	key   string
}

func runImportIntoSLOGuardOnce(do *domain.Domain, logger *zap.Logger) error {
	taskMgr, err := storage.GetTaskManager()
	if err != nil {
		return err
	}
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalDistTask)
	activeTasks, err := collectActiveImportTasks(ctx, taskMgr)
	if err != nil {
		return err
	}

	activeKeys := make(map[string]struct{}, len(activeTasks))
	jobKeys := make(map[int64][]string)
	activeJobs := make(map[int64]struct{})
	for _, task := range activeTasks {
		activeKeys[task.key] = struct{}{}
		activeJobs[task.jobID] = struct{}{}
		jobKeys[task.jobID] = append(jobKeys[task.jobID], task.key)
	}

	queryJobIDs := make([]int64, 0, len(activeJobs)+1)
	queryJobIDs = append(queryJobIDs, importer.SLOGuardGlobalJobID)
	for jobID := range activeJobs {
		queryJobIDs = append(queryJobIDs, jobID)
	}

	configs, err := loadSLOGuardConfigs(ctx, taskMgr, queryJobIDs)
	if err != nil {
		return err
	}
	globalCfg := configs[importer.SLOGuardGlobalJobID]
	if globalCfg == nil {
		globalCfg = defaultSLOGuardConfig(importer.SLOGuardGlobalJobID)
	}

	effectiveConfigs := make(map[int64]*importer.SLOGuardConfig, len(activeJobs))
	anyEnabled := globalCfg.Enable
	for jobID := range activeJobs {
		cfg := configs[jobID]
		if cfg == nil {
			cfg = globalCfg
		}
		effectiveConfigs[jobID] = cfg
		if cfg.Enable {
			anyEnabled = true
		}
	}

	if !anyEnabled {
		if hasSLOGuardPausedTasks() {
			if err := resumeSLOGuardPausedTasks(ctx, taskMgr, logger, activeKeys, func(string, int64) bool { return true }); err != nil {
				return err
			}
		}
		setSLOGuardRuntime(importer.SLOGuardGlobalJobID, sloGuardStateNormal, 0)
		for jobID := range activeJobs {
			setSLOGuardRuntime(jobID, sloGuardStateNormal, 0)
		}
		clearSLOGuardRuntime(activeJobs)
		return nil
	}

	metricValue, err := querySLOGuardMetric(ctx, taskMgr, sloGuardTimeNow())
	if err != nil {
		return err
	}
	metricDuration := time.Duration(metricValue * float64(time.Second))

	if globalCfg.Enable {
		thresholds := sloGuardThresholdsFromConfig(globalCfg, logger)
		prevState := getSLOGuardState(importer.SLOGuardGlobalJobID)
		nextState := decideSLOGuardState(prevState, metricDuration, thresholds)
		slowRate := sloGuardSlowApplyRateLimit(globalCfg)
		if nextState != prevState {
			logger.Info("import slo guard state changed",
				zap.Int64("job-id", importer.SLOGuardGlobalJobID),
				zap.String("from", prevState.String()),
				zap.String("to", nextState.String()),
				zap.Duration("metric", metricDuration),
				zap.Duration("slow-threshold", thresholds.slow),
				zap.Duration("pause-threshold", thresholds.pause),
				zap.Duration("resume-threshold", thresholds.resume),
			)
		}
		setSLOGuardRuntime(importer.SLOGuardGlobalJobID, nextState, slowRate)
	} else {
		setSLOGuardRuntime(importer.SLOGuardGlobalJobID, sloGuardStateNormal, 0)
	}

	for jobID, cfg := range effectiveConfigs {
		if !cfg.Enable {
			setSLOGuardRuntime(jobID, sloGuardStateNormal, 0)
			if err := resumeSLOGuardPausedTasks(ctx, taskMgr, logger, activeKeys, func(_ string, id int64) bool { return id == jobID }); err != nil {
				return err
			}
			continue
		}
		thresholds := sloGuardThresholdsFromConfig(cfg, logger)
		prevState := getSLOGuardState(jobID)
		nextState := decideSLOGuardState(prevState, metricDuration, thresholds)
		slowRate := sloGuardSlowApplyRateLimit(cfg)
		if nextState == sloGuardStatePause {
			if err := pauseSLOGuardTasks(ctx, taskMgr, logger, jobKeys[jobID]); err != nil {
				return err
			}
		} else {
			if err := resumeSLOGuardPausedTasks(ctx, taskMgr, logger, activeKeys, func(_ string, id int64) bool { return id == jobID }); err != nil {
				return err
			}
		}
		if nextState != prevState {
			logger.Info("import slo guard state changed",
				zap.Int64("job-id", jobID),
				zap.String("from", prevState.String()),
				zap.String("to", nextState.String()),
				zap.Duration("metric", metricDuration),
				zap.Duration("slow-threshold", thresholds.slow),
				zap.Duration("pause-threshold", thresholds.pause),
				zap.Duration("resume-threshold", thresholds.resume),
			)
		}
		setSLOGuardRuntime(jobID, nextState, slowRate)
	}

	clearSLOGuardRuntime(activeJobs)
	return nil
}

func collectActiveImportTasks(ctx context.Context, taskMgr *storage.TaskManager) ([]activeImportTask, error) {
	var jobs []*importer.JobInfo
	if err := taskMgr.WithNewSession(func(se sessionctx.Context) error {
		exec := se.GetSQLExecutor()
		var err error
		jobs, err = importer.GetAllViewableJobs(ctx, exec, "", true)
		return err
	}); err != nil {
		return nil, err
	}
	tasks := make([]activeImportTask, 0, len(jobs))
	for _, job := range jobs {
		if job == nil || !job.CanCancel() {
			continue
		}
		tasks = append(tasks, activeImportTask{
			jobID: job.ID,
			key:   TaskKey(job.ID),
		})
	}
	return tasks, nil
}

func loadSLOGuardConfigs(ctx context.Context, taskMgr *storage.TaskManager, jobIDs []int64) (map[int64]*importer.SLOGuardConfig, error) {
	var configs map[int64]*importer.SLOGuardConfig
	if err := taskMgr.WithNewSession(func(se sessionctx.Context) error {
		exec := se.GetSQLExecutor()
		var err error
		configs, err = importer.GetSLOGuardConfigs(ctx, exec, jobIDs)
		return err
	}); err != nil {
		return nil, err
	}
	return configs, nil
}

func defaultSLOGuardConfig(jobID int64) *importer.SLOGuardConfig {
	return &importer.SLOGuardConfig{
		JobID:                      jobID,
		Enable:                     false,
		PauseThreshold:             importer.DefaultSLOGuardPauseThreshold,
		SlowApplyRateLimitMBPerSec: importer.DefaultSLOGuardSlowApplyRateLimitMB,
	}
}

func sloGuardThresholdsFromConfig(cfg *importer.SLOGuardConfig, logger *zap.Logger) sloGuardThresholds {
	pauseThreshold, err := parseSLOGuardPauseThreshold(cfg.PauseThreshold)
	if err != nil || pauseThreshold <= 0 {
		pauseThreshold, _ = parseSLOGuardPauseThreshold(importer.DefaultSLOGuardPauseThreshold)
		logger.Warn("invalid slo guard pause threshold, fallback to default",
			zap.String("value", cfg.PauseThreshold),
			zap.Error(err),
		)
	}
	slowThreshold := time.Duration(float64(pauseThreshold) * sloGuardSlowRatio)
	resumeThreshold := time.Duration(float64(pauseThreshold) * sloGuardResumeRatio)
	return sloGuardThresholds{
		resume: resumeThreshold,
		slow:   slowThreshold,
		pause:  pauseThreshold,
	}
}

func parseSLOGuardPauseThreshold(value string) (time.Duration, error) {
	return time.ParseDuration(strings.TrimSpace(value))
}

func sloGuardSlowApplyRateLimit(cfg *importer.SLOGuardConfig) lightningcfg.ByteSize {
	if cfg == nil || cfg.SlowApplyRateLimitMBPerSec <= 0 {
		return 0
	}
	maxInt64 := int64(^uint64(0) >> 1)
	if cfg.SlowApplyRateLimitMBPerSec > (maxInt64 >> 20) {
		return lightningcfg.ByteSize(maxInt64)
	}
	return lightningcfg.ByteSize(cfg.SlowApplyRateLimitMBPerSec << 20)
}

func querySLOGuardMetric(ctx context.Context, taskMgr *storage.TaskManager, now time.Time) (float64, error) {
	metric := sloGuardMetricName
	def, ok := infoschema.MetricTableMap[metric]
	if !ok {
		return 0, perrors.Errorf("invalid slo guard metric: %s", metric)
	}
	window := sloGuardWindow
	if window <= 0 {
		return 0, errors.New("invalid slo guard window")
	}
	start := now.Add(-window).In(timeutil.SystemLocation())
	end := now.In(timeutil.SystemLocation())
	hasLabel := false
	for _, label := range def.Labels {
		if strings.EqualFold(label, sloGuardMetricLabelKey) {
			hasLabel = true
			break
		}
	}
	if !hasLabel {
		return 0, perrors.Errorf("missing slo guard label key: %s", sloGuardMetricLabelKey)
	}
	var (
		value float64
		args  []any
	)
	var sb strings.Builder
	sb.WriteString("select max(value) from `")
	sb.WriteString(metadef.MetricSchemaName.L)
	sb.WriteString("`.`")
	sb.WriteString(metric)
	sb.WriteString("` where time >= %? and time <= %?")
	args = append(args, start.Format(plannerutil.MetricTableTimeFormat), end.Format(plannerutil.MetricTableTimeFormat))
	if def.Quantile > 0 {
		sb.WriteString(" and quantile = %?")
		args = append(args, sloGuardMetricQuantile)
	}
	sb.WriteString(" and `")
	sb.WriteString(sloGuardMetricLabelKey)
	sb.WriteString("` = %?")
	args = append(args, sloGuardMetricLabelValue)
	sql := sb.String()
	innerCtx := kv.WithInternalSourceType(ctx, kv.InternalTxnOthers)
	err := taskMgr.WithNewSession(func(se sessionctx.Context) error {
		exec := se.GetRestrictedSQLExecutor()
		rows, _, err := exec.ExecRestrictedSQL(innerCtx, nil, sql, args...)
		if err != nil {
			return err
		}
		if len(rows) == 0 || rows[0].IsNull(0) {
			return errNoSLOGuardMetricData
		}
		value = rows[0].GetFloat64(0)
		return nil
	})
	if err != nil {
		return 0, err
	}
	return value, nil
}

func decideSLOGuardState(prev sloGuardState, metric time.Duration, thresholds sloGuardThresholds) sloGuardState {
	if thresholds.slow <= 0 {
		thresholds.slow = thresholds.pause
	}
	if thresholds.pause < thresholds.slow {
		thresholds.pause = thresholds.slow
	}
	if thresholds.resume <= 0 || thresholds.resume > thresholds.slow {
		thresholds.resume = thresholds.slow
	}

	switch prev {
	case sloGuardStatePause:
		if metric <= thresholds.resume {
			return sloGuardStateNormal
		}
		if metric >= thresholds.pause {
			return sloGuardStatePause
		}
		return sloGuardStateSlow
	case sloGuardStateSlow:
		if metric <= thresholds.resume {
			return sloGuardStateNormal
		}
		if metric >= thresholds.pause {
			return sloGuardStatePause
		}
		return sloGuardStateSlow
	default:
		if metric >= thresholds.pause {
			return sloGuardStatePause
		}
		if metric >= thresholds.slow {
			return sloGuardStateSlow
		}
		return sloGuardStateNormal
	}
}

func pauseSLOGuardTasks(ctx context.Context, taskMgr *storage.TaskManager, logger *zap.Logger, taskKeys []string) error {
	for _, key := range taskKeys {
		if isSLOGuardPausedTask(key) {
			continue
		}
		found, err := taskMgr.PauseTask(ctx, key)
		if err != nil {
			logger.Warn("pause import task failed", zap.String("task-key", key), zap.Error(err))
			continue
		}
		if found {
			markSLOGuardPausedTask(key)
			logger.Info("paused import task by slo guard", zap.String("task-key", key))
		}
	}
	return nil
}

func resumeSLOGuardPausedTasks(
	ctx context.Context,
	taskMgr *storage.TaskManager,
	logger *zap.Logger,
	activeKeys map[string]struct{},
	shouldResume func(key string, jobID int64) bool,
) error {
	paused := snapshotSLOGuardPausedTasks()
	for _, key := range paused {
		jobID, ok := parseSLOGuardTaskKeyJobID(key)
		if !ok {
			if _, ok := activeKeys[key]; !ok {
				unmarkSLOGuardPausedTask(key)
			}
			continue
		}
		if !shouldResume(key, jobID) {
			if _, ok := activeKeys[key]; !ok {
				unmarkSLOGuardPausedTask(key)
			}
			continue
		}
		found, err := taskMgr.ResumeTask(ctx, key)
		if err != nil {
			logger.Warn("resume import task failed", zap.String("task-key", key), zap.Error(err))
			continue
		}
		if found {
			unmarkSLOGuardPausedTask(key)
			logger.Info("resumed import task by slo guard", zap.String("task-key", key))
			continue
		}
		if _, ok := activeKeys[key]; !ok {
			unmarkSLOGuardPausedTask(key)
		}
	}
	return nil
}

func parseSLOGuardTaskKeyJobID(key string) (int64, bool) {
	pos := strings.LastIndex(key, "/")
	if pos == -1 || pos+1 >= len(key) {
		return 0, false
	}
	jobID, err := strconv.ParseInt(key[pos+1:], 10, 64)
	if err != nil {
		return 0, false
	}
	return jobID, true
}

func (s sloGuardState) String() string {
	switch s {
	case sloGuardStateSlow:
		return "slow"
	case sloGuardStatePause:
		return "pause"
	default:
		return "normal"
	}
}

func hasSLOGuardPausedTasks() bool {
	sloGuardPausedTasks.mu.Lock()
	defer sloGuardPausedTasks.mu.Unlock()
	return len(sloGuardPausedTasks.keys) > 0
}

func snapshotSLOGuardPausedTasks() []string {
	sloGuardPausedTasks.mu.Lock()
	defer sloGuardPausedTasks.mu.Unlock()
	keys := make([]string, 0, len(sloGuardPausedTasks.keys))
	for key := range sloGuardPausedTasks.keys {
		keys = append(keys, key)
	}
	return keys
}

func isSLOGuardPausedTask(key string) bool {
	sloGuardPausedTasks.mu.Lock()
	defer sloGuardPausedTasks.mu.Unlock()
	_, ok := sloGuardPausedTasks.keys[key]
	return ok
}

func markSLOGuardPausedTask(key string) {
	sloGuardPausedTasks.mu.Lock()
	defer sloGuardPausedTasks.mu.Unlock()
	sloGuardPausedTasks.keys[key] = struct{}{}
}

func unmarkSLOGuardPausedTask(key string) {
	sloGuardPausedTasks.mu.Lock()
	defer sloGuardPausedTasks.mu.Unlock()
	delete(sloGuardPausedTasks.keys, key)
}
