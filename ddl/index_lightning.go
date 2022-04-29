package ddl

import (
	"context"
	"strconv"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	lit "github.com/pingcap/tidb/ddl/lightning"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/metrics"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/table/tables"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/logutil"
	decoder "github.com/pingcap/tidb/util/rowDecoder"
)

// isAllowFastDDL is used to
func isAllowFastDDL(storage kv.Storage) bool {
	sessCtx := newContext(storage)
	if sessCtx.GetSessionVars().TiDBFastDDL {
		return true
	} else {
		return false
	}
}

// Below is the prepare lightning environment logic.
func prepareLightningEnv(ctx context.Context, unique bool, job *model.Job, t *meta.Meta, tbl *model.TableInfo, openEngine bool) (err error) {

	// Create lightning backend
	err = prepareBackend(ctx, unique, job)

	// ToDo: Bear here all the worker will share one openengine,
	if openEngine {
		err = prepareLightningEngine(job, t, 0, tbl)
	}
	return err
}

func prepareLightningContext(workId int, keyEngine string, kvCount int) (*lit.WorkerContext, error) {
	wctx, err := lit.GlobalLightningEnv.LitMemRoot.RegistWorkerContext(workId, keyEngine, kvCount)
	if err != nil {
		err = errors.New(lit.LERR_CREATE_CONTEX_FAILED)
		lit.GlobalLightningEnv.LitMemRoot.ReleaseKVCache(keyEngine + strconv.FormatUint(uint64(workId), 10))
		return wctx, err
	}
	return wctx, err
}

func prepareBackend(ctx context.Context, unique bool, job *model.Job) (err error) {
	bcKey := genBackendContextKey(job.ID)
	// Create and regist backend of lightning
	err = lit.GlobalLightningEnv.LitMemRoot.RegistBackendContext(ctx, unique, bcKey)
	if err != nil {
		err = errors.New(lit.LERR_CREATE_BACKEND_FAILED)
		lit.GlobalLightningEnv.LitMemRoot.DeleteBackendContext(bcKey, false)
		return err
	}

	return err
}

func prepareLightningEngine(job *model.Job, t *meta.Meta, workerId int64, tbl *model.TableInfo) (err error) {
	bcKey := genBackendContextKey(job.ID)
	enginKey := genEngineInfoKey(job.ID, workerId)
	_, err = lit.GlobalLightningEnv.LitMemRoot.RegistEngineInfo(job, t, bcKey, enginKey, tbl)
	if err != nil {
		err = errors.New(lit.LERR_CREATE_ENGINE_FAILED)
		lit.GlobalLightningEnv.LitMemRoot.DeleteBackendContext(bcKey, true)
	}
	return err
}

func genBackendContextKey(jobId int64) string {
	return strconv.FormatInt(jobId, 10)
}

func genEngineInfoKey(jobId int64, workerId int64) string {
	return genBackendContextKey(jobId) + strconv.FormatInt(workerId, 10)
}

func importIndexDataToStore(ctx context.Context, reorg *reorgInfo, unique bool, tbl table.Table) error {
	keyEngineInfo := genEngineInfoKey(reorg.ID, 0)
	if isAllowFastDDL(reorg.d.store) && reorg.IsLightningOk == false {
		// just log info.
		err := lit.FinishIndexOp(ctx, keyEngineInfo, tbl, unique)
		if err != nil {
			logutil.BgLogger().Error("FinishIndexOp err2" + err.Error())
			err = errors.Trace(err)
		}
	}
	return nil
}

func cleanUpLightningEnv(reorg *reorgInfo) {
	if isAllowFastDDL(reorg.d.store) && reorg.IsLightningOk == false {
		// Close backend
		keyBackend := genBackendContextKey(reorg.ID)
		lit.GlobalLightningEnv.LitMemRoot.DeleteBackendContext(keyBackend, false)
	}
}

// Below is lightning worker logic
type addIndexWorkerLit struct {
	addIndexWorker
	// Light needed structure.
	keyEngineInfo string
	workerctx     *lit.WorkerContext
	tbl           *model.TableInfo
}

func newAddIndexWorkerLit(sessCtx sessionctx.Context, worker *worker, id int, t table.PhysicalTable, indexInfo *model.IndexInfo, decodeColMap map[int64]decoder.Column, sqlMode mysql.SQLMode, jobId int64) (*addIndexWorkerLit, error) {
	var workerCtx *lit.WorkerContext
	index := tables.NewIndex(t.GetPhysicalID(), t.Meta(), indexInfo)
	rowDecoder := decoder.NewRowDecoder(t, t.WritableCols(), decodeColMap)
	// ToDo: Bear Currently, all the lightning worker share one openengine.
	keyEngineInfo := genEngineInfoKey(jobId, 0)

	ei, err := lit.GlobalLightningEnv.EngineManager.GetEngineInfo(keyEngineInfo)
	if err != nil {
		errors.New(lit.LERR_GET_ENGINE_FAILED)
		return nil, err
	}
	// build worker context for lightning worker.
	workerCtx, err = prepareLightningContext(id, keyEngineInfo, 1)
	if err != nil {
		return nil, err
	}
	// Add build openengine process.
	return &addIndexWorkerLit{
		addIndexWorker: addIndexWorker{
			baseIndexWorker: baseIndexWorker{
				backfillWorker: newBackfillWorker(sessCtx, worker, id, t),
				indexes:        []table.Index{index},
				rowDecoder:     rowDecoder,
				defaultVals:    make([]types.Datum, len(t.WritableCols())),
				rowMap:         make(map[int64]types.Datum, len(decodeColMap)),
				metricCounter:  metrics.BackfillTotalCounter.WithLabelValues("add_idx_rate"),
				sqlMode:        sqlMode,
			},
			index: index,
		},
		keyEngineInfo: keyEngineInfo,
		workerctx:     workerCtx,
		tbl:           ei.GetTableInfo(),
	}, err
}

// BackfillDataInTxn will backfill table index in a transaction. A lock corresponds to a rowKey if the value of rowKey is changed,
// Note that index columns values may change, and an index is not allowed to be added, so the txn will rollback and retry.
// BackfillDataInTxn will add w.batchCnt indices once, default value of w.batchCnt is 128.
func (w *addIndexWorkerLit) BackfillDataInTxn(handleRange reorgBackfillTask) (taskCtx backfillTaskContext, errInTxn error) {
	failpoint.Inject("errorMockPanic", func(val failpoint.Value) {
		if val.(bool) {
			panic("panic test")
		}
	})

	// ToDo: Bear need to change use write-reorg started one txn to be the txn scan the table.
	oprStartTime := time.Now()
	errInTxn = kv.RunInNewTxn(context.Background(), w.sessCtx.GetStore(), true, func(ctx context.Context, txn kv.Transaction) error {
		taskCtx.addedCount = 0
		taskCtx.scanCount = 0
		txn.SetOption(kv.Priority, w.priority)
		if tagger := w.ddlWorker.getResourceGroupTaggerForTopSQL(); tagger != nil {
			txn.SetOption(kv.ResourceGroupTagger, tagger)
		}

		idxRecords, nextKey, taskDone, err := w.fetchRowColVals(txn, handleRange)
		if err != nil {
			return errors.Trace(err)
		}
		taskCtx.nextKey = nextKey
		taskCtx.done = taskDone

		err = w.batchCheckUniqueKey(txn, idxRecords)
		if err != nil {
			return errors.Trace(err)
		}

		for _, idxRecord := range idxRecords {
			taskCtx.scanCount++
			// The index is already exists, we skip it, no needs to backfill it.
			// The following update, delete, insert on these rows, TiDB can handle it correctly.
			if idxRecord.skip {
				continue
			}

			// Create the index.
			key, idxVal, _, err := w.index.Create4SST(w.sessCtx, txn, idxRecord.vals, idxRecord.handle, idxRecord.rsData, table.WithIgnoreAssertion)
			if err != nil {
				return errors.Trace(err)
			}

			// Currently, only use one kVCache, later may use multi kvCache to parallel compute/io performance.
			w.workerctx.KVCache[0].PushKeyValue(key, idxVal, idxRecord.handle)

			taskCtx.addedCount++
		}

		return nil
	})

	// log.L().Info("[debug-fetch] finish idxRecord info", zap.Int("scanCount", taskCtx.scanCount), zap.Int("addedCount", taskCtx.addedCount))
	errInTxn = lit.FlushKeyValSync(context.TODO(), w.keyEngineInfo, w.workerctx.KVCache[0])
	if errInTxn != nil {
		//logutil.BgLogger().Error("FlushKeyValSync %d paris err: %v.", len(w.workerctx.KVCache[0].Fetch()), errInTxn.Error())
	}
	logSlowOperations(time.Since(oprStartTime), "AddIndexLightningBackfillDataInTxn", 3000)

	return
}
