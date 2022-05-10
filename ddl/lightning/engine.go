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
	"encoding/binary"
	"fmt"
	"strconv"
	"time"

	"github.com/pingcap/tidb/br/pkg/lightning/backend"
	"github.com/pingcap/tidb/br/pkg/lightning/backend/kv"
	"github.com/pingcap/tidb/br/pkg/lightning/checkpoints"
	"github.com/pingcap/tidb/br/pkg/lightning/config"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/sqlexec"
	"github.com/pkg/errors"
	"github.com/twmb/murmur3"
	"go.uber.org/zap"
)
type engineInfo struct {
	Id           int32
	key          string
	BackCtx      *BackendContext
	OpenedEngine *backend.OpenedEngine
	writer       *backend.LocalEngineWriter
	cfg          *backend.EngineConfig
	// TODO: use channel later;
	ref      int32
	tbl      *model.TableInfo
	isOpened bool
}

func (ei *engineInfo) Init(key string, cfg *backend.EngineConfig, bCtx *BackendContext, en *backend.OpenedEngine, tbl *model.TableInfo) {
	ei.key = key
	ei.cfg = cfg
	ei.BackCtx = bCtx
	ei.OpenedEngine = en
	ei.tbl = tbl
	ei.isOpened = false
}

var (
	ErrNotFound error = errors.New("not object in this cache")
	ErrWasInUse error = errors.New("this object was in used")
)

func (ei *engineInfo) getWriter() (*backend.LocalEngineWriter, error) {
	if ei.writer != nil {
		return ei.writer, nil
	}
	var err error
	ei.writer, err = ei.OpenedEngine.LocalWriter(context.TODO(), &backend.LocalWriterConfig{})
	if err != nil {
		return nil, err
	}
	return ei.writer, nil
}

func (ei *engineInfo) GetTableInfo() *model.TableInfo {
	return ei.tbl
}

func (ei *engineInfo) unsafeImportAndReset(ctx context.Context) error {
	ret, err := fetchTableRegionSizeStatsWitchEngine(ei)
	if err != nil {
		return fmt.Errorf("FinishIndexOp err:%w", err)
	}

	if err = ei.BackCtx.Backend.FlushAll(ctx); err != nil {
		//LogError("flush engine for disk quota failed, check again later : %v", err)
		return err
	}

	if ctx == nil {
		ctx = context.TODO()
	}
	ctx = context.WithValue(ctx, RegionSizeStats, ret)
	_, uuid := backend.MakeUUID(ei.tbl.Name.String(), ei.Id)
	return ei.BackCtx.Backend.UnsafeImportAndReset(ctx, uuid, int64(config.SplitRegionSize)*int64(config.MaxSplitRegionSizeRatio))
}

func GenEngineKey(schemaId int64, tableId int64, indexId int64) string {
	var engineKey string
	engineKey = strconv.Itoa(int(schemaId)) + strconv.Itoa(int(tableId)) + strconv.Itoa(int(indexId))
	return engineKey
}

// TODO: 1. checkpoint??
// TODO: 2. EngineID can use startTs for only.
func CreateEngine(ctx context.Context, job *model.Job, t *meta.Meta, backendKey string, engineKey string, tblInfo *model.TableInfo) (err error) {
	ei := new(engineInfo)
	cpt := checkpoints.TidbTableInfo{
		job.ID,
		// ddl.StartTs,
		job.SchemaName,
		job.TableName,
		tblInfo,
	}
	var cfg backend.EngineConfig
	cfg.TableInfo = &cpt
	cfg.Local = &backend.LocalEngineConfig{
		Compact:            true,
		CompactThreshold:   1024 * _mb,
		CompactConcurrency: 4,
	}

	// Open lightning engine
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], uint64(job.ID))
	h := murmur3.New32()
	h.Write(b[:])
	eid := int32(h.Sum32())

	bc := GlobalLightningEnv.BackendCache[backendKey]
	be := bc.Backend

	en, err := be.OpenEngine(ctx, &cfg, job.TableName, eid)
	if err != nil {
		return errors.Errorf("PrepareIndexOp.OpenEngine err:%v", err)
	}
	ei.Init(engineKey, &cfg, bc, en, tblInfo)
	GlobalLightningEnv.EngineManager.engineCache[engineKey] = ei
	bc.EngineCache[engineKey] = ei

	return nil
}

func FlushKeyValSync(ctx context.Context, keyEngineInfo string, cache *WorkerKVCache) (err error) {
	err = flushKeyValSync(ctx, keyEngineInfo, cache)
	if err != nil {
		return err
	}
	if sn.szInc.encodeSize < GlobalLightningEnv.diskQuota {
		return
	}

	ei, ok := GlobalLightningEnv.EngineManager.GetEngineInfo(keyEngineInfo)
	if ok != nil {
		return ok
	}

	start := time.Now()
	err = ei.unsafeImportAndReset(ctx)
	if err != nil {
		// LogError("unsafeImportAndReset %s cost %v", size2str(sn.szInc.encodeSize), time.Now().Sub(start))
		// Only import failed, next time can import continue.
		return nil
	}
	sn.importAndReset(start)
	//LogInfo("unsafeImportAndReset %s cost %v", size2str(sn.szInc.encodeSize), time.Now().Sub(start))
	sn.szInc.encodeSize = 0
	sn.szInc.count = 0
	return nil
}

func flushKeyValSync(ctx context.Context, keyEngineInfo string, cache *WorkerKVCache) (err error) {
	ei, err := GlobalLightningEnv.EngineManager.GetEngineInfo(keyEngineInfo)
	if err != nil {
		return err
	}
	start := time.Now()
	cb := func(err error) {
		sn.setErr(err)
		sn.writeCost(start)
		sn.inc(cache.Size())
		cache.Reset()
		// LogTest("FlushKeyValSync(%d,%d) report=%s", len(cache.pairs), cache.size, sn.Report())
	}

	lw, err := ei.getWriter()
	if err != nil {
		cb(err)
		return errors.New("IndexOperator.getWriter err")
	}
	err = lw.WriteRows(ctx, nil, cache.toKvRows())
	if err != nil {
		cb(err)
		return errors.New("IndexOperator.WriteRows err")
	}
	cb(nil)
	return err
}

func fetchTableRegionSizeStatsWitchEngine(ei *engineInfo) (ret map[uint64]int64, err error) {
	return nil, err
}

func fetchTableRegionSizeStats(tblId int64, exec sqlexec.RestrictedSQLExecutor) (ret map[uint64]int64, err error) {
	// must use '%?' to replace '?' in RestrictedSQLExecutor.
	query := "SELECT REGION_ID, APPROXIMATE_SIZE FROM information_schema.TIKV_REGION_STATUS WHERE TABLE_ID = %?"
	sn, err := exec.ParseWithParams(context.TODO(), query, tblId)
	if err != nil {
		return nil, errors.Errorf("ParseWithParams err: %v", err)
	}
	rows, _, err := exec.ExecRestrictedStmt(context.TODO(), sn)
	if err != nil {
		return nil, errors.Errorf("ExecRestrictedStmt err: %v", err)
	}
	// parse values;
	ret = make(map[uint64]int64, len(rows))
	var (
		regionID uint64
		size     int64
	)
	for idx, row := range rows {
		if 2 != row.Len() {
			return nil, errors.Errorf("row %d has %d fields", idx, row.Len())
		}
		regionID = row.GetUint64(0)
		size = row.GetInt64(1)
		ret[regionID] = size
	}
	//
	// d, _ := json.Marshal(ret)
	//LogTest("fetchTableRegionSizeStats table(%d) = %d.", tblId, len(ret))
	return ret, nil
}

// TODO: If multi thread call this function, how to handle this logic?
func FinishIndexOp(ctx context.Context, keyEngineInfo string, tbl table.Table, unique bool) (err error) {
	ei, err := GlobalLightningEnv.EngineManager.GetEngineInfo(keyEngineInfo)
	if err != nil {
		return err
	}
	defer func() {
		GlobalLightningEnv.EngineManager.ReleaseRef(keyEngineInfo)
		GlobalLightningEnv.EngineManager.ReleaseEngine(keyEngineInfo)
	}()
	//
	//LogInfo("FinishIndexOp %d.", startTs)
	start := time.Now()
	defer func() {
		sn.setErr(err)
		sn.finishCost(start)
		str := sn.Report()
		logutil.BgLogger().Info(str)
		fmt.Println(str)
	}()
	//
	ret, err := fetchTableRegionSizeStatsWitchEngine(ei)
	if err != nil {
		return fmt.Errorf("FinishIndexOp err:%w", err)
	}
	if ctx == nil {
		ctx = context.TODO()
	}
	ctx = context.WithValue(ctx, RegionSizeStats, ret)
	//
	indexEngine := ei.OpenedEngine
	cfg := ei.cfg
	//
	closeEngine, err1 := indexEngine.Close(ctx, cfg)
	if err1 != nil {
		return errors.New("engine.Close err")
	}
	err = closeEngine.Import(ctx, int64(config.SplitRegionSize))
	if err != nil {
		return errors.New("engine.Import err")
	}
	err = closeEngine.Cleanup(ctx)
	if err != nil {
		return errors.New("engine.Cleanup err")
	}
	if unique {
		hasDupe, err := ei.BackCtx.Backend.CollectRemoteDuplicateRows(ctx, tbl, ei.tbl.Name.O, &kv.SessionOptions{
			SQLMode: mysql.ModeStrictAllTables,
			SysVars: defaultImportantVariables,
		})
		if hasDupe {
			return errors.New("unique index conflicts detected")
		} else if err != nil {
			logutil.BgLogger().Error("fail to detect unique index conflicts, unknown index status", zap.Error(err))
			return errors.New("fail to detect unique index conflicts, unknown index status")
		}
	}
	// should release before ReleaseEngine
	GlobalLightningEnv.EngineManager.ReleaseRef(keyEngineInfo)
	GlobalLightningEnv.EngineManager.ReleaseEngine(keyEngineInfo)
	return nil
}
