package addindex

import (
	"context"
	"encoding/binary"
	"fmt"
	"github.com/pingcap/errors"
	tidbcfg "github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/sqlexec"
	"github.com/twmb/murmur3"
	"sync/atomic"

	"github.com/pingcap/tidb/br/pkg/lightning/backend"
	"github.com/pingcap/tidb/br/pkg/lightning/backend/local"
	"github.com/pingcap/tidb/br/pkg/lightning/checkpoints"
	lightcommon "github.com/pingcap/tidb/br/pkg/lightning/common"
	lightcfg "github.com/pingcap/tidb/br/pkg/lightning/config"
	lightlog "github.com/pingcap/tidb/br/pkg/lightning/log"
	"github.com/pingcap/tidb/br/pkg/lightning/tikv"
)

// just test cycle reference.
//   this package used by table.index and ddl.index.
//   调用的地方，都有注释"TODO: delete later".
func IndexCycleReference() {
	tidbcfg.GetGlobalConfig()
	var _ table.Table
	var _ sqlexec.RestrictedSQLExecutor
	// refer lightning.
	RefLightning()
}

func RefLightning() (backend.Backend, error) {
	// refer lightning.
	lightlog.SetAppLogger(logutil.BgLogger())
	var minState tikv.StoreState
	_ = minState
	cfg := lightcfg.NewConfig()
	var tls *lightcommon.TLS
	return local.NewLocalBackend(nil, tls, cfg, nil, 0, nil)
}

//
var (
	// a 'add index' running once.
	once_index int32
	engine     *backend.OpenedEngine
	ddlinfo    *DDLInfo
	cfgp       *backend.EngineConfig
	// backendp   *backend.Backend
)

func finishClean() {
	if isAddIndex() {
		engine = nil
		ddlinfo = nil
		cfgp = nil
	}
	resetAddIndex()
}

func InitIndexOptimize() {
	cfg := tidbcfg.GetGlobalConfig()
	cluster.PdAddr = cfg.AdvertiseAddress
	cluster.Port = cfg.Port
	cluster.Status = cfg.Status.StatusPort
	cluster.PdAddr = cfg.Path
	fmt.Printf("InitOnce %+v.\n", cluster)
	lightlog.SetAppLogger(logutil.BgLogger())
}

func isAddIndex() bool {
	return 1 == atomic.LoadInt32(&once_index)
}

func doAddIndex() bool {
	return atomic.CompareAndSwapInt32(&once_index, 0, 1)
}

func resetAddIndex() bool {
	return atomic.CompareAndSwapInt32(&once_index, 1, 0)
}

func PrepareIndexOp(ctx context.Context, ddl DDLInfo) (err error) {
	if !doAddIndex() {
		LogDebug("ddl %d start failed.", ddl.StartTs)
		return nil
	}
	ddlinfo = &ddl
	// init engine;
	LogInfo("PrepareIndexOp %+v", ddl)
	// err == ErrNotFound
	info := cluster
	be, err := createLocalBackend(ctx, info, ddl.Unique)
	if err != nil {
		LogFatal("PrepareIndexOp.createLocalBackend err:%s.", err.Error())
		return errors.Errorf("PrepareIndexOp.createLocalBackend err:%v", err)
	}
	cpt := checkpoints.TidbTableInfo{
		ddl.Table.ID,
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

	engine = en
	cfgp = &cfg
	// backendp = &be
	// ec.put(ddl.StartTs, NewEngineInfo(eid, &cfg, be, en, ddl.Table, ddl.Exec))
	return nil
}

func FlushKeyValSync(ctx context.Context, startTs uint64, cache *WorkerKVCache) (err error) {
	if !isAddIndex() {
		LogError("FlushKeyValSync failed.")
		return errors.Errorf("add index was running")
	}
	lw, err := engine.LocalWriter(context.TODO(), &backend.LocalWriterConfig{})
	if err != nil {
		return errors.Annotate(err, "IndexOperator.getWriter err")
	}
	err = lw.WriteRows(ctx, nil, cache.toKvRows())
	if err != nil {
		return errors.Annotate(err, "IndexOperator.WriteRows err")
	}
	return err
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

func FinishIndexOp(ctx context.Context, done bool, startTs uint64) (err error) {
	//
	exec, err := ddlinfo.Exec.Get()
	if err != nil {
		return fmt.Errorf("FinishIndexOp err:%w", err)
	}
	ret, err := fetchTableRegionSizeStats(ddlinfo.Table.ID, exec)
	ddlinfo.Exec.Put(exec)
	if err != nil {
		return fmt.Errorf("FinishIndexOp err:%w", err)
	}
	if ctx == nil {
		ctx = context.TODO()
	}
	ctx = context.WithValue(ctx, "RegionSizeStats", ret)
	//

	indexEngine := engine
	//
	closeEngine, err1 := indexEngine.Close(ctx, cfgp)
	if err1 != nil {
		return errors.Annotate(err1, "engine.Close err")
	}
	// if done ok , import to tikv.
	if done {
		err = closeEngine.Import(ctx, int64(lightcfg.SplitRegionSize))
		if err != nil {
			return errors.Annotate(err, "engine.Import err")
		}
	}

	err = closeEngine.Cleanup(ctx)
	if err != nil {
		return errors.Annotate(err, "engine.Cleanup err")
	}
	return nil
}
