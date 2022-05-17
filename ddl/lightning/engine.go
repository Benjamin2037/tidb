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

	"github.com/pingcap/tidb/br/pkg/lightning/backend"
	"github.com/pingcap/tidb/br/pkg/lightning/backend/kv"
	"github.com/pingcap/tidb/br/pkg/lightning/common"
	"github.com/pingcap/tidb/br/pkg/lightning/config"
	"github.com/pingcap/tidb/br/pkg/lightning/log"
	tidbkv "github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/table"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

// Currently, one engine for one index reorg task, each task will create new writer under the
// OpenEngine. Note engineInfo is not thread safe.
type engineInfo struct {
	id           int32
	key          string
	backCtx      *BackendContext
	openedEngine *backend.OpenedEngine
	cfg          *backend.EngineConfig
	tableName    string
	WriterCount  int
	writeCache   map[string]*backend.LocalEngineWriter
}

func (ei *engineInfo) Init(id int32, key string, cfg *backend.EngineConfig, bCtx *BackendContext, en *backend.OpenedEngine, tblName string) {
	ei.id = id
	ei.key = key
	ei.cfg = cfg
	ei.backCtx = bCtx
	ei.openedEngine = en
	ei.tableName = tblName
	ei.writeCache = make(map[string]*backend.LocalEngineWriter)
}

func GenEngineInfoKey(jobid int64, indexId int64) string {
    return strconv.FormatInt(jobid, 10) + strconv.FormatInt(indexId, 10)
}

func CreateEngine(ctx context.Context, job *model.Job, backendKey string, engineKey string, indexId int32) (err error) {
	ei := new(engineInfo)

	var cfg backend.EngineConfig
	cfg.Local = &backend.LocalEngineConfig{
		Compact:            true,
		CompactThreshold:   1024 * _mb,
		CompactConcurrency: 4,
	}

	// Open lightning engine
	bc := GlobalLightningEnv.LitMemRoot.backendCache[backendKey]
	be := bc.Backend

	en, err := be.OpenEngine(ctx, &cfg, job.TableName, indexId)
	if err != nil {
		errMsg := LERR_CREATE_ENGINE_FAILED + err.Error()
		log.L().Error(errMsg)
		return errors.New(errMsg)
	}
	ei.Init(indexId, engineKey, &cfg, bc, en, job.TableName)
	GlobalLightningEnv.LitMemRoot.EngineMgr.StoreEngineInfo(engineKey, ei)
	bc.EngineCache[engineKey] = ei
    log.L().Info(LINFO_OPEN_ENGINE, 
		zap.String("backend key", ei.backCtx.Key),
		zap.String("Engine key", ei.key))
	return nil
}

func FinishIndexOp(ctx context.Context, keyEngineInfo string, tbl table.Table, unique bool) (err error) {
	var errMsg string
	var keyMsg string
	ei, err := GlobalLightningEnv.LitMemRoot.EngineMgr.LoadEngineInfo(keyEngineInfo)
	if err != nil {
		return err
	}
	defer func() {
		GlobalLightningEnv.LitMemRoot.EngineMgr.ReleaseEngine(keyEngineInfo)
	}()
    
	keyMsg = "backend key:" + ei.backCtx.Key + "Engine key:" + ei.key
	// Close engine
	log.L().Info(LINFO_CLOSE_ENGINE, zap.String("backend key", ei.backCtx.Key), zap.String("Engine key", ei.key))
	indexEngine := ei.openedEngine
	closeEngine, err1 := indexEngine.Close(ei.backCtx.Ctx, ei.cfg)
	if err1 != nil {
		errMsg = LERR_CLOSE_ENGINE_ERR + keyMsg
		log.L().Error(errMsg)
		return errors.New(errMsg)
	}

	// Local dupl check
	if unique {
        hasDupe, err := ei.backCtx.Backend.CollectLocalDuplicateRows(ctx, tbl, ei.tableName, &kv.SessionOptions{
			SQLMode: mysql.ModeStrictAllTables,
			SysVars: ei.backCtx.sysVars,
		})
		if hasDupe {
			errMsg = LERR_LOCAL_DUP_EXIST_ERR + keyMsg
			log.L().Error(errMsg)
			return errors.New(errMsg)
		} else if err != nil {
			errMsg = LERR_LOCAL_DUP_CHECK_ERR + keyMsg
			log.L().Error(errMsg)
			return errors.New(errMsg)
		}
	}

	// Ingest data to TiKV
	log.L().Info(LINFO_START_TO_IMPORT, zap.String("backend key", ei.backCtx.Key), zap.String("Engine key", ei.key))
	err = closeEngine.Import(ctx, int64(config.SplitRegionSize))
	if err != nil {
		errMsg = LERR_INGEST_DATA_ERR + keyMsg
		log.L().Error(errMsg)
		return errors.New(errMsg)
	}

	// Clean up the engine
	err = closeEngine.Cleanup(ctx)
	if err != nil {
		errMsg = LERR_CLOSE_ENGINE_ERR + keyMsg
		log.L().Error(errMsg)
		return errors.New(errMsg)
	}

	// Check Remote duplicate value for index
	if unique {
		hasDupe, err := ei.backCtx.Backend.CollectRemoteDuplicateRows(ctx, tbl, ei.tableName, &kv.SessionOptions{
			SQLMode: mysql.ModeStrictAllTables,
			SysVars: ei.backCtx.sysVars,
		})
		if hasDupe {
			errMsg = LERR_REMOTE_DUP_EXIST_ERR + keyMsg
			log.L().Error(errMsg)
			return errors.New(errMsg)
		} else if err != nil {
			errMsg = LERR_REMOTE_DUP_CHECK_ERR + keyMsg
			log.L().Error(errMsg)
			return errors.New(errMsg)
		}
	}
	return nil
}

type WorkerContext struct {
	eInfo     *engineInfo
	lWrite    *backend.LocalEngineWriter
}

// Init Worker Context will get worker local writer from engine info writer cache first, if exist.
// If local wirter not exist, then create new one and store it into engine info writer cache.
// note operate ei.writeCache map is not thread safe please make sure there is sync mechaism to
// make sure the safe.
func (wCtx *WorkerContext)InitWorkerContext (engineKey string, workerid int) (err error) {
	wCtxKey := engineKey + strconv.Itoa(workerid)
    
	ei, exist := GlobalLightningEnv.LitMemRoot.EngineMgr.engineCache[engineKey]
    
	if !exist {
		
		return errors.New(LERR_GET_ENGINE_FAILED)
	} 
	wCtx.eInfo= ei;

	// Fisrt get local writer from engine cache.
	wCtx.lWrite, exist = ei.writeCache[wCtxKey]
	// If not exist then build one 
	if !exist {
        wCtx.lWrite, err = ei.openedEngine.LocalWriter(ei.backCtx.Ctx, &backend.LocalWriterConfig{})
	    if err != nil {
		   return err
	    }
        // Cache the lwriter, here we do not lock, because this is called in mem root alloc
		// process it will lock while alloc object.
	    ei.writeCache[wCtxKey] = wCtx.lWrite
	}
    return nil
}

func (wCtx *WorkerContext)WriteRow(key, idxVal []byte, h tidbkv.Handle) {
    var kvs []common.KvPair = make([]common.KvPair, 1, 1)
	kvs[0].Key = key
	kvs[0].Val = idxVal
	kvs[0].RowID = h.IntValue()
	wCtx.lWrite.WriteRow(wCtx.eInfo.backCtx.Ctx, nil, kvs)
}