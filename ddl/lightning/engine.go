// Copyright 2022 PingCAP, Inc.
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

package lightning

import (
	"context"
	"strconv"

	"github.com/google/uuid"
	"github.com/pingcap/tidb/br/pkg/lightning/backend"
	"github.com/pingcap/tidb/br/pkg/lightning/backend/kv"
	"github.com/pingcap/tidb/br/pkg/lightning/common"
	"github.com/pingcap/tidb/br/pkg/lightning/config"
	tikv "github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

// One engine for one index reorg task, each task will create several new writers under the
// Opened Engine. Note engineInfo is not thread safe.
type engineInfo struct {
	id  int64
	key string

	backCtx      *BackendContext
	openedEngine *backend.OpenedEngine
	uuid         uuid.UUID
	cfg          *backend.EngineConfig
	tableName    string
	WriterCount  int
	writerCache  map[string]*backend.LocalEngineWriter
}

// NewEngineInfo create a new EngineInfo struct.
func NewEngineInfo(
	id int64, key string, cfg *backend.EngineConfig, bCtx *BackendContext,
	en *backend.OpenedEngine, tblName string, uuid uuid.UUID, wCnt int) *engineInfo {
	ei := engineInfo{
		id:           id,
		key:          key,
		cfg:          cfg,
		backCtx:      bCtx,
		openedEngine: en,
		uuid:         uuid,
		tableName:    tblName,
		WriterCount:  wCnt,
		writerCache:  make(map[string]*backend.LocalEngineWriter, wCnt),
	}
	return &ei
}

// GenEngineInfoKey generate one engine key with jobID and indexID.
func GenEngineInfoKey(jobID int64, indexID int64) string {
	return strconv.FormatInt(jobID, 10) + strconv.FormatInt(indexID, 10)
}

// CreateEngine will create a engine to do backfill task for one add index reorg task.
func CreateEngine(
	ctx context.Context,
	job *model.Job,
	backendKey string,
	engineKey string,
	indexID int64,
	wCnt int) (err error) {
	// Get a created backend to create engine under it.
	bc := GlobalEnv.LitMemRoot.backendCache[backendKey]
	// Open one engine under an existing backend.
	cfg := generateLocalEngineConfig(job.ID, job.SchemaName, job.TableName)
	en, err := bc.Backend.OpenEngine(ctx, cfg, job.TableName, int32(indexID))
	if err != nil {
		errMsg := LitErrCreateEngineFail + err.Error()
		logutil.BgLogger().Error(errMsg)
		return errors.New(errMsg)
	}
	id := en.GetEngineUUID()
	ei := NewEngineInfo(indexID, engineKey, cfg, bc, en, job.TableName, id, wCnt)
	GlobalEnv.LitMemRoot.EngineMgr.StoreEngineInfo(engineKey, ei)
	bc.EngineCache[engineKey] = ei
	return nil
}

// FinishIndexOp will finish local index preparation job and ingest index sst file into TiKV.
func FinishIndexOp(ctx context.Context, engineInfoKey string, tbl table.Table, unique bool) (err error) {
	var errMsg string
	var keyMsg string
	ei, exist := GlobalEnv.LitMemRoot.EngineMgr.LoadEngineInfo(engineInfoKey)
	if !exist {
		return errors.New(LitErrGetEngineFail)
	}
	defer func() {
		GlobalEnv.LitMemRoot.EngineMgr.ReleaseEngine(engineInfoKey)
	}()

	keyMsg = "backend key:" + ei.backCtx.Key + "Engine key:" + ei.key
	// Close engine and finish local tasks of lightning.
	logutil.BgLogger().Info(LitInfoCloseEngine, zap.String("backend key", ei.backCtx.Key), zap.String("Engine key", ei.key))
	indexEngine := ei.openedEngine
	closeEngine, err1 := indexEngine.Close(ei.backCtx.Ctx, ei.cfg)
	if err1 != nil {
		errMsg = LitErrCloseEngineErr + keyMsg
		logutil.BgLogger().Error(errMsg)
		return errors.New(errMsg)
	}

	// Reset disk quota before ingest, if user changed it.
	GlobalEnv.checkAndResetQuota()

	// Ingest data to TiKV
	logutil.BgLogger().Info(LitInfoStartImport, zap.String("backend key", ei.backCtx.Key),
		zap.String("Engine key", ei.key),
		zap.String("Split Region Size", strconv.FormatInt(int64(config.SplitRegionSize), 10)))
	err = closeEngine.Import(ctx, int64(config.SplitRegionSize), int64(config.SplitRegionKeys))
	if err != nil {
		errMsg = LitErrIngestDataErr + keyMsg
		logutil.BgLogger().Error(errMsg)
		return errors.New(errMsg)
	}

	// Clean up the engine local workspace.
	err = closeEngine.Cleanup(ctx)
	if err != nil {
		errMsg = LitErrCloseEngineErr + keyMsg
		logutil.BgLogger().Error(errMsg)
		return errors.New(errMsg)
	}

	// Check Remote duplicate value for index
	if unique {
		hasDupe, err := ei.backCtx.Backend.CollectRemoteDuplicateIndex(ctx, tbl, ei.tableName, &kv.SessionOptions{
			SQLMode: mysql.ModeStrictAllTables,
			SysVars: ei.backCtx.sysVars,
		}, ei.id)
		if err != nil {
			errMsg = LitErrRemoteDupCheckrr + keyMsg
			logutil.BgLogger().Error(errMsg)
			return errors.New(errMsg)
		} else if hasDupe {
			errMsg = LitErrRemoteDupExistErr + keyMsg
			logutil.BgLogger().Error(errMsg)
			return tikv.ErrKeyExists
		}
	}
	return nil
}

// FlushEngine flush the lightning engine memory data into local disk.
func FlushEngine(engineKey string, ei *engineInfo) error {
	err := ei.openedEngine.Flush(ei.backCtx.Ctx)
	if err != nil {
		logutil.BgLogger().Error(LitErrFlushEngineErr, zap.String("Engine key:", engineKey))
		return err
	}
	return nil
}

// UnsafeImportEngineData check if disk consumption is over disk quota, if yes then ingest temp file into TiKV
func UnsafeImportEngineData(jobID int64, indexID int64) error {
	engineKey := GenEngineInfoKey(jobID, indexID)
	ei, exist := GlobalEnv.LitMemRoot.EngineMgr.LoadEngineInfo(engineKey)
	if !exist {
		logutil.BgLogger().Error(LitErrGetEngineFail, zap.String("Engine key:", engineKey))
		return errors.New(LitErrGetEngineFail)
	}

	totalStorageUsed, totalStorageAvail := GlobalEnv.LitMemRoot.DiskStat()
	GlobalEnv.checkAndResetQuota()
	if GlobalEnv.NeedImportEngineData(totalStorageUsed, totalStorageAvail) {
		// ToDo it should be changed according checkpoint solution.
		// Flush writer cached data into local disk for engine first.
		err := FlushEngine(engineKey, ei)
		if err != nil {
			return err
		}
		logutil.BgLogger().Info(LitInfoUnsafeImport, zap.String("Engine key:", engineKey), zap.String("Current total available disk:", strconv.FormatUint(totalStorageAvail, 10)))
		err = ei.backCtx.Backend.UnsafeImportAndReset(ei.backCtx.Ctx, ei.uuid, int64(config.SplitRegionSize)*int64(config.MaxSplitRegionSizeRatio), int64(config.SplitRegionKeys))
		if err != nil {
			logutil.BgLogger().Error(LitErrIngestDataErr, zap.String("Engine key:", engineKey),
				zap.String("import partial file failed, current disk storage remains", strconv.FormatUint(totalStorageAvail, 10)))
			return err
		}
	}
	return nil
}

// WorkerContext used keep one lightning local writer for one backfill worker.
type WorkerContext struct {
	eInfo  *engineInfo
	lWrite *backend.LocalEngineWriter
}

// InitWorkerContext will get worker local writer from engine info writer cache first, if exists.
// If local writer not exist, then create new one and store it into engine info writer cache.
// note: operate ei.writeCache map is not thread safe please make sure there is sync mechanism to
// make sure the safe.
func (wCtx *WorkerContext) InitWorkerContext(engineKey string, workerID int) (err error) {
	wCtxKey := engineKey + strconv.Itoa(workerID)
	ei, exist := GlobalEnv.LitMemRoot.EngineMgr.enginePool[engineKey]
	if !exist {
		return errors.New(LitErrGetEngineFail)
	}
	wCtx.eInfo = ei

	// First get local writer from engine cache.
	wCtx.lWrite, exist = ei.writerCache[wCtxKey]
	// If not exist then build one
	if !exist {
		wCtx.lWrite, err = ei.openedEngine.LocalWriter(ei.backCtx.Ctx, &backend.LocalWriterConfig{})
		if err != nil {
			return err
		}
		// Cache the lwriter, here we do not lock, because this is done under mem root alloc
		// process it own the lock already while alloc object.
		ei.writerCache[wCtxKey] = wCtx.lWrite
	}
	return nil
}

// WriteRow Write one row into local writer buffer.
func (wCtx *WorkerContext) WriteRow(key, idxVal []byte) error {
	var kvs []common.KvPair = make([]common.KvPair, 1)
	kvs[0].Key = key
	kvs[0].Val = idxVal
	row := kv.MakeRowsFromKvPairs(kvs)
	return wCtx.lWrite.WriteRows(wCtx.eInfo.backCtx.Ctx, nil, row)
}

// CanRestoreReorgTask only when backend and Engine still be cached, then the task could be restored,
// otherwise return false to let reorg task restart.
func CanRestoreReorgTask(jobID int64, indexID int64) bool {
	engineInfoKey := GenEngineInfoKey(jobID, indexID)
	bcKey := GenBackendContextKey(jobID)
	_, enExist := GlobalEnv.LitMemRoot.EngineMgr.LoadEngineInfo(engineInfoKey)
	_, bcExist := GlobalEnv.LitMemRoot.getBackendContext(bcKey, true)
	if enExist && bcExist {
		return true
	}
	return false
}
