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
package ddl

import (
	"context"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	lit "github.com/pingcap/tidb/ddl/lightning"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/metrics"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/table/tables"
	"github.com/pingcap/tidb/types"
	decoder "github.com/pingcap/tidb/util/rowDecoder"
)

// isAllowFastDDL is used to
func isAllowFastDDL(storage kv.Storage) bool {
	sessCtx := newContext(storage)
	// Only when both TiDBFastDDL is set to on and Lightning env is inited successful,
	// the add index could choose lightning path to do backfill procedure.   
	if sessCtx.GetSessionVars().TiDBFastDDL && lit.GlobalLightningEnv.IsInited {
		return true
	} else {
		return false
	}
}

func prepareBackend(ctx context.Context, unique bool, job *model.Job, sqlMode mysql.SQLMode) (err error) {
	bcKey := lit.GenBackendContextKey(job.ID)
	// Create and regist backend of lightning
	err = lit.GlobalLightningEnv.LitMemRoot.RegistBackendContext(ctx, unique, bcKey, sqlMode)
	if err != nil {
		lit.GlobalLightningEnv.LitMemRoot.DeleteBackendContext(bcKey)
		return err
	}

	return err
}

func prepareLightningEngine(job *model.Job, indexId int64, workerCnt int) (wCnt int, err error) {
	bcKey := lit.GenBackendContextKey(job.ID)
	enginKey := lit.GenEngineInfoKey(job.ID, indexId)
	wCnt, err = lit.GlobalLightningEnv.LitMemRoot.RegistEngineInfo(job, bcKey, enginKey, int32(indexId), workerCnt)
	if err != nil {
		lit.GlobalLightningEnv.LitMemRoot.DeleteBackendContext(bcKey)
	}
	return wCnt, err
}

func importIndexDataToStore(ctx context.Context, reorg *reorgInfo, unique bool, tbl table.Table) error {
	if reorg.IsLightningEnabled {
		engineInfoKey := lit.GenEngineInfoKey(reorg.ID, 0)
		// just log info.
		err := lit.FinishIndexOp(ctx, engineInfoKey, tbl, unique)
		if err != nil {
			err = errors.Trace(err)
		}
	}
	return nil
}

func cleanUpLightningEnv(reorg *reorgInfo) {
	if reorg.IsLightningEnabled {
		// Close backend
		bcKey := lit.GenBackendContextKey(reorg.ID)
		lit.GlobalLightningEnv.LitMemRoot.DeleteBackendContext(bcKey)
	}
}

// Below is lightning worker logic
type addIndexWorkerLit struct {
	addIndexWorker
    
	// Lightning related variable.
	writerContex *lit.WorkerContext
}

func newAddIndexWorkerLit(sessCtx sessionctx.Context, worker *worker, id int, t table.PhysicalTable, indexInfo *model.IndexInfo, decodeColMap map[int64]decoder.Column, sqlMode mysql.SQLMode, jobId int64) (*addIndexWorkerLit, error) {
	index := tables.NewIndex(t.GetPhysicalID(), t.Meta(), indexInfo)
	rowDecoder := decoder.NewRowDecoder(t, t.WritableCols(), decodeColMap)
	// ToDo: Bear Currently, all the lightning worker share one openengine.
	engineInfoKey := lit.GenEngineInfoKey(jobId, indexInfo.ID)

	lwCtx, err := lit.GlobalLightningEnv.LitMemRoot.RegistWorkerContext(engineInfoKey, id)
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
        writerContex: lwCtx,
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
            w.writerContex.WriteRow(key, idxVal, idxRecord.handle)

			taskCtx.addedCount++
		}
		return nil
	})
	logSlowOperations(time.Since(oprStartTime), "AddIndexLightningBackfillDataInTxn", 3000)
	return
}
