package addindex

import (
	"context"
	"encoding/binary"
	"fmt"
	"github.com/pingcap/tidb/br/pkg/lightning/backend/local"
	"time"

	"github.com/pingcap/errors"
	"go.uber.org/zap"

	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/util/sqlexec"

	"github.com/twmb/murmur3"

	"github.com/pingcap/tidb/br/pkg/lightning/backend"
	"github.com/pingcap/tidb/br/pkg/lightning/backend/kv"
	"github.com/pingcap/tidb/br/pkg/lightning/checkpoints"
	"github.com/pingcap/tidb/br/pkg/lightning/config"
	"github.com/pingcap/tidb/br/pkg/lightning/log"
	tidbcfg "github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/util/logutil"
)

func InitIndexOptimize() {
	cfg := tidbcfg.GetGlobalConfig()
	cluster.PdAddr = cfg.AdvertiseAddress
	cluster.Port = cfg.Port
	cluster.Status = cfg.Status.StatusPort
	cluster.PdAddr = cfg.Path
	parseDiskQuota()
	fmt.Printf("InitOnce %+v.diskQuota=%d;ck=%v.\n", cluster, diskQuota, *ck)
	log.SetAppLogger(logutil.BgLogger())
}

// TODO: 1. checkpoint??
// TODO: 2. EngineID can use startTs for only.
func PrepareIndexOp(ctx context.Context, ddl DDLInfo) (err error) {
	ec.mtx.RLock()
	ei, ok := ec.cache[ddl.StartTs]
	if ok {
		ec.mtx.RUnlock()
		LogDebug("ddl %d has exist.", ddl.StartTs)
		return nil
	}
	// create engineInfo;
	ei = &engineInfo{}
	ec.cache[ddl.StartTs] = ei
	ec.mtx.RUnlock()

	// init engine;
	LogInfo("PrepareIndexOp %+v", ddl)
	sn.reset(fmt.Sprintf("%s.%s.%d", ddl.Schema, ddl.Table.Name, ddl.StartTs))
	start := time.Now()
	defer func() {
		sn.setErr(err)
		sn.preprareCost(start)
	}()
	// err == ErrNotFound
	info := cluster
	be, err := createLocalBackend(ctx, info, ddl.Unique)
	if err != nil {
		LogFatal("PrepareIndexOp.createLocalBackend err:%s.", err.Error())
		return errors.Errorf("PrepareIndexOp.createLocalBackend err:%v", err)
	}
	cpt := checkpoints.TidbTableInfo{
		genNextTblId(),
		// ddl.StartTs,
		ddl.Schema,
		ddl.Table.Name.String(),
		ddl.Table,
	}
	var cfg backend.EngineConfig
	cfg.TableInfo = &cpt
	cfg.Local = &backend.LocalEngineConfig{
		Compact:            true,
		CompactThreshold:   1024 * _mb,
		CompactConcurrency: 4,
	}
	//
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], ddl.StartTs)
	h := murmur3.New32()
	h.Write(b[:])
	eid := int32(h.Sum32())

	en, err := be.OpenEngine(ctx, &cfg, ddl.Table.Name.String(), eid)
	if err != nil {
		return errors.Errorf("PrepareIndexOp.OpenEngine err:%v", err)
	}
	ei.Init(eid, &cfg, be, en, ddl.Table, ddl.Exec)
	// ec.put(ddl.StartTs, NewEngineInfo(eid, &cfg, be, en, ddl.Table, ddl.Exec))
	return nil
}

func FlushKeyValSync(ctx context.Context, startTs uint64, cache *WorkerKVCache) (err error) {
	err = flushKeyValSync(ctx, startTs, cache)
	if err != nil {
		return err
	}
	if sn.szInc.encodeSize < diskQuota {
		return
	}
	ec.mtx.Lock()
	ei, ok := ec.cache[startTs]
	if !ok {
		return ErrNotFound
	}
	defer ec.mtx.Unlock()
	start := time.Now()
	err = ei.unsafeImportAndReset(ctx)
	if err != nil {
		LogError("unsafeImportAndReset %s cost %v", size2str(sn.szInc.encodeSize), time.Now().Sub(start))
		// 仅仅是导入失败，下次还是可以继续导入，不影响.
		return nil
	}
	sn.importAndReset(start)
	LogInfo("unsafeImportAndReset %s cost %v", size2str(sn.szInc.encodeSize), time.Now().Sub(start))
	sn.szInc.encodeSize = 0
	sn.szInc.count = 0
	return nil
}

func flushKeyValSync(ctx context.Context, startTs uint64, cache *WorkerKVCache) (err error) {
	ec.mtx.RLock()
	ei, ok := ec.cache[startTs]
	defer ec.mtx.RUnlock()
	if !ok {
		return ErrNotFound
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
		return errors.Annotate(err, "IndexOperator.getWriter err")
	}
	err = lw.WriteRows(ctx, nil, cache.toKvRows())
	if err != nil {
		cb(err)
		return errors.Annotate(err, "IndexOperator.WriteRows err")
	}
	cb(nil)
	return err
}

func fetchTableRegionSizeStatsWitchEngine(ei *engineInfo) (ret map[uint64]int64, err error) {
	exec, err := ei.exec.Get()
	if err != nil {
		return nil, err
	}
	ret, err = fetchTableRegionSizeStats(ei.tbl.ID, exec)
	ei.exec.Put(exec)
	return ret, err
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
	LogTest("fetchTableRegionSizeStats table(%d) = %d.", tblId, len(ret))
	return ret, nil
}

// TODO: 如果多个 线程 同时调用该函数，是否存在这样情况? 如果存在，该怎么处理?
func FinishIndexOp(ctx context.Context, startTs uint64, tbl table.Table, unique bool) (err error) {
	ei, err := ec.getEngineInfo(startTs)
	if err != nil {
		return err
	}
	defer func() {
		ec.releaseRef(startTs)
		ec.ReleaseEngine(startTs)
	}()
	//
	LogInfo("FinishIndexOp %d.", startTs)
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
	ctx = context.WithValue(ctx, local.RegionSizeStats, ret)
	//
	indexEngine := ei.OpenedEngine
	cfg := ei.cfg
	//
	closeEngine, err1 := indexEngine.Close(ctx, cfg)
	if err1 != nil {
		return errors.Annotate(err1, "engine.Close err")
	}
	err = closeEngine.Import(ctx, int64(config.SplitRegionSize))
	if err != nil {
		return errors.Annotate(err, "engine.Import err")
	}
	err = closeEngine.Cleanup(ctx)
	if err != nil {
		return errors.Annotate(err, "engine.Cleanup err")
	}
	if unique {
		hasDupe, err := ei.backend.CollectRemoteDuplicateRows(ctx, tbl, ei.tbl.Name.O, &kv.SessionOptions{
			SQLMode: mysql.ModeStrictAllTables,
			SysVars: defaultImportantVariables,
		})
		if hasDupe {
			return errors.Annotate(err, "unique index conflicts detected")
		} else if err != nil {
			logutil.BgLogger().Error("fail to detect unique index conflicts, unknown index status", zap.Error(err))
			return errors.Annotate(err, "fail to detect unique index conflicts, unknown index status")
		}
	}
	// should release before ReleaseEngine
	ec.releaseRef(startTs)
	ec.ReleaseEngine(startTs)
	return nil
}

var defaultImportantVariables = map[string]string{
	"max_allowed_packet":      "67108864",
	"div_precision_increment": "4",
	"time_zone":               "SYSTEM",
	"lc_time_names":           "en_US",
	"default_week_format":     "0",
	"block_encryption_mode":   "aes-128-ecb",
	"group_concat_max_len":    "1024",
}
