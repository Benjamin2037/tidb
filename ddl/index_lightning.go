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
	"bytes"
	"context"
	"strconv"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	lit "github.com/pingcap/tidb/ddl/lightning"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/metrics"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	trans "github.com/pingcap/tidb/store/driver/txn"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/table/tables"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/dbterror"
	"github.com/pingcap/tidb/util/logutil"
	decoder "github.com/pingcap/tidb/util/rowDecoder"
	"go.uber.org/zap"
)

const (
	// BackfillProgressPercent set a backfill ratio in whole reorg task.
	BackfillProgressPercent float64 = 0.6
)

var (
	telemetryAddIndexLightningUsage = metrics.TelemetryAddIndexLightningCnt
)

// IsEnableFastReorg check whether Fast Reorg is allowed.
func IsEnableFastReorg() bool {
	// Only when both TiDBDDLEnableFastReorg is set to on and Lightning env has been inited successful,
	// the add index could choose lightning path to do backfill procedure.
	if variable.EnableFastReorg.Load() && lit.GlobalEnv.IsInited {
		// Increase telemetryAddIndexLightningUsage
		telemetryAddIndexLightningUsage.Inc()
		return true
	}
	return false
}

// Check if PiTR is enabled in cluster.
func isPiTREnable(w *worker) bool {
	ctx, err := w.sessPool.get()
	if err != nil {
		return true
	}
	defer w.sessPool.put(ctx)
	failpoint.Inject("EnablePiTR", func() {
		logutil.BgLogger().Info("Lightning: Failpoint enable PiTR.")
		failpoint.Return(true)
	})
	return lit.CheckPiTR(ctx)
}

func prepareLightningEngine(job *model.Job, indexID int64, workerCnt int) (wCnt int, err error) {
	enginKey := lit.GenEngineInfoKey(job.ID, indexID)
	bc, _ := lit.BackCtxMgr.Load(job.ID)
	wCnt, err = bc.EngMgr.Register(bc, job, enginKey, indexID, workerCnt)
	if err != nil {
		lit.BackCtxMgr.Unregister(job.ID)
	}
	return wCnt, err
}

// importIndexDataToStore import local index sst file into TiKV.
func importIndexDataToStore(ctx context.Context, reorg *reorgInfo, indexID int64, unique bool, tbl table.Table) error {
	if bc, ok := lit.BackCtxMgr.Load(reorg.Job.ID); ok && bc.NeedRestore() {
		engineInfoKey := lit.GenEngineInfoKey(reorg.ID, indexID)
		err := bc.FinishImport(engineInfoKey, unique, tbl)
		if err != nil {
			err = errors.Trace(err)
			return err
		}
		// After import local data into TiKV, then the progress set to 85.
		metrics.GetBackfillProgressByLabel(metrics.LblAddIndex, reorg.SchemaName, reorg.TableName).Set(85)
	}
	return nil
}

// cleanUpLightningEnv will clean one DDL job's backend context.
func cleanUpLightningEnv(reorg *reorgInfo, isCanceled bool, indexID int64) {
	if _, ok := lit.BackCtxMgr.Load(reorg.Job.ID); ok {
		lit.BackCtxMgr.Unregister(reorg.ID)
	}
}

// cleanUpLightningEngines will clean one DDL job's engines.
func cleanUpLightningEngines(reorg *reorgInfo) {
	if bc, ok := lit.BackCtxMgr.Load(reorg.Job.ID); ok {
		bc.SetNeedRestore(false)
		bc.EngMgr.UnregisterAll()
	}
}

// Check if this reorg is a restore reorg task
// Check if current lightning reorg task can keep on executing.
// Otherwise, restart the reorg task.
func canRestoreReorgTask(job *model.Job, indexID int64) bool {
	// The reorg stage is just started, do nothing
	if job.SnapshotVer == 0 {
		return false
	}

	// Check if backend and engine are both active in memory.
	if !lit.CanRestoreReorgTask(job.ID, indexID) {
		return false
	}
	return true
}

// Below is lightning worker implementation
type addIndexWorkerLit struct {
	addIndexWorker

	// Lightning relative variable.
	writerCtx *lit.WorkerContext
}

func newAddIndexWorkerLit(sessCtx sessionctx.Context, worker *worker, id int, t table.PhysicalTable, decodeColMap map[int64]decoder.Column, reorgInfo *reorgInfo, jc *JobContext, jobID int64) (*addIndexWorkerLit, error) {
	var index table.Index
	for _, idx := range t.Indices() {
		if idx.Meta().ID == reorgInfo.currElement.ID {
			index = idx
			break
		}
	}
	rowDecoder := decoder.NewRowDecoder(t, t.WritableCols(), decodeColMap)
	engineInfoKey := lit.GenEngineInfoKey(jobID, index.Meta().ID)
	bc, _ := lit.BackCtxMgr.Load(jobID)
	ei, _ := bc.EngMgr.Load(engineInfoKey)
	lwCtx, err := ei.NewWorkerCtx(id)
	if err != nil {
		return nil, err
	}
	// Add build opened engine process.
	return &addIndexWorkerLit{
		addIndexWorker: addIndexWorker{
			baseIndexWorker: baseIndexWorker{
				backfillWorker: newBackfillWorker(sessCtx, id, t, reorgInfo),
				indexes:        []table.Index{index},
				rowDecoder:     rowDecoder,
				defaultVals:    make([]types.Datum, len(t.WritableCols())),
				rowMap:         make(map[int64]types.Datum, len(decodeColMap)),
				metricCounter:  metrics.BackfillTotalCounter.WithLabelValues("add_idx_rate"),
				sqlMode:        reorgInfo.ReorgMeta.SQLMode,
				jobContext:     jc,
			},
			index: index,
		},
		writerCtx: lwCtx,
	}, err
}

// BackfillDataInTxn will backfill table index by lightning.
func (w *addIndexWorkerLit) BackfillDataInTxn(handleRange reorgBackfillTask) (taskCtx backfillTaskContext, errInTxn error) {
	failpoint.Inject("errorMockPanic", func(val failpoint.Value) {
		if val.(bool) {
			panic("panic test")
		}
	})
	fetchTag := "AddIndexLightningFetchData" + strconv.Itoa(w.id)
	writeTag := "AddIndexLightningWriteData" + strconv.Itoa(w.id)
	txnTag := "AddIndexLightningBackfillDataInTxn" + strconv.Itoa(w.id)
	// Set a big batch size to enhance performance.
	w.batchCnt *= 16

	oprStartTime := time.Now()
	ctx := kv.WithInternalSourceType(context.Background(), w.jobContext.ddlJobSourceType())
	errInTxn = kv.RunInNewTxn(ctx, w.sessCtx.GetStore(), true, func(ctx context.Context, txn kv.Transaction) error {
		taskCtx.addedCount = 0
		taskCtx.scanCount = 0
		txn.SetOption(kv.Priority, w.priority)
		if tagger := w.reorgInfo.d.getResourceGroupTaggerForTopSQL(w.reorgInfo.Job); tagger != nil {
			txn.SetOption(kv.ResourceGroupTagger, tagger)
		}

		idxRecords, nextKey, taskDone, err := w.fetchRowColVals(txn, handleRange)
		logSlowOperations(time.Since(oprStartTime), fetchTag, 1000)
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
			err = w.writerCtx.WriteRow(key, idxVal)
			if err != nil {
				return errors.Trace(err)
			}
			taskCtx.addedCount++
		}
		logSlowOperations(time.Since(oprStartTime), writeTag, 1000)
		return nil
	})
	logSlowOperations(time.Since(oprStartTime), txnTag, 3000)
	return
}

func (w *backFillIndexWorker) batchCheckTemporaryUniqueKey(txn kv.Transaction, idxRecords []*temporaryIndexRecord) error {
	idxInfo := w.index.Meta()
	if !idxInfo.Unique {
		// non-unique key need not to check, just overwrite it,
		// because in most case, backfilling indices is not exists.
		return nil
	}

	if len(w.idxKeyBufs) < w.batchCnt {
		w.idxKeyBufs = make([][]byte, w.batchCnt)
	}
	w.batchCheckKeys = w.batchCheckKeys[:0]
	w.distinctCheckFlags = w.distinctCheckFlags[:0]

	stmtCtx := w.sessCtx.GetSessionVars().StmtCtx
	for i, record := range idxRecords {
		distinct := false
		if !record.delete && tablecodec.IndexKVIsUnique(record.vals) {
			distinct = true
		}
		// save the buffer to reduce memory allocations.
		w.idxKeyBufs[i] = record.key

		w.batchCheckKeys = append(w.batchCheckKeys, record.key)
		w.distinctCheckFlags = append(w.distinctCheckFlags, distinct)
	}

	batchVals, err := txn.BatchGet(context.Background(), w.batchCheckKeys)
	if err != nil {
		return errors.Trace(err)
	}

	// 1. unique-key/primary-key is duplicate and the handle is equal, skip it.
	// 2. unique-key/primary-key is duplicate and the handle is not equal, return duplicate error.
	// 3. non-unique-key is duplicate, skip it.
	for i, key := range w.batchCheckKeys {
		if val, found := batchVals[string(key)]; found {
			if w.distinctCheckFlags[i] {
				if !bytes.Equal(val, idxRecords[i].vals) {
					return kv.ErrKeyExists
				}
			}
			idxRecords[i].skip = true
		} else if w.distinctCheckFlags[i] {
			// The keys in w.batchCheckKeys also maybe duplicate,
			// so we need to backfill the not found key into `batchVals` map.
			batchVals[string(key)] = idxRecords[i].vals
		}
	}
	// Constrains is already checked.
	stmtCtx.BatchCheck = true
	return nil
}

func (w *backFillIndexWorker) batchSkipKey(txn kv.Transaction, store kv.Storage, idxRecords []*temporaryIndexRecord) error {
	if len(w.batchCheckTmpKeys) == 0 {
		return nil
	}
	w.skipAll = false
	// Lock keys to check whether they could be skipped or not.
	err := txn.LockKeys(context.Background(), new(kv.LockCtx), w.batchCheckTmpKeys...)
	if err != nil {
		return errors.Trace(err)
	}
	// Gen a current snapshot to get latest updated.
	snapshot := store.GetSnapshot(kv.MaxVersion)
	// Get duplicated key from temp index.
	batchVals, err := trans.NewBufferBatchGetter(txn.GetMemBuffer(), nil, snapshot).BatchGet(context.Background(), w.batchCheckTmpKeys)
	if err != nil {
		return errors.Trace(err)
	}
	count := len(w.batchCheckTmpKeys)
	for i, key := range w.batchCheckTmpKeys {
		if val, found := batchVals[string(key)]; found {
			var keyVer []byte
			length := len(val)
			keyVer = append(keyVer, val[length-1:]...)
			if bytes.Equal(keyVer, []byte("m")) {
				idxRecords[i].skip = true
				count--
				if i == 0 {
					// catch val for later use.
					w.firstVal = w.firstVal[:0]
					w.firstVal = append(w.firstVal, val...)
				}
			}
		}
	}
	if count == 0 {
		w.skipAll = true
	}
	return nil
}

// temporaryIndexRecord is the record information of an index.
type temporaryIndexRecord struct {
	key    []byte
	vals   []byte
	skip   bool // skip indicates that the index key is already exists, we should not add it.
	delete bool
	unique bool
	keyVer []byte
}
type backFillIndexWorker struct {
	*backfillWorker

	index table.Index

	// The following attributes are used to reduce memory allocation.
	idxKeyBufs         [][]byte
	batchCheckKeys     []kv.Key
	distinctCheckFlags []bool
	tmpIdxRecords      []*temporaryIndexRecord
	batchCheckTmpKeys  []kv.Key
	jobContext         *JobContext
	skipAll            bool
	firstVal           []byte
}

func newTempIndexWorker(sessCtx sessionctx.Context, worker *worker, id int, t table.PhysicalTable, indexInfo *model.IndexInfo, reorgInfo *reorgInfo, jc *JobContext) *backFillIndexWorker {
	index := tables.NewIndex(t.GetPhysicalID(), t.Meta(), indexInfo)

	// Add build openengine process.
	return &backFillIndexWorker{
		backfillWorker: newBackfillWorker(sessCtx, id, t, reorgInfo),
		index:          index,
		jobContext:     jc,
	}
}

// BackfillDataInTxn merge temp index data in txn.
func (w *backFillIndexWorker) BackfillDataInTxn(taskRange reorgBackfillTask) (taskCtx backfillTaskContext, errInTxn error) {
	oprStartTime := time.Now()
	ctx := kv.WithInternalSourceType(context.Background(), w.jobContext.ddlJobSourceType())
	errInTxn = kv.RunInNewTxn(ctx, w.sessCtx.GetStore(), true, func(ctx context.Context, txn kv.Transaction) error {
		taskCtx.addedCount = 0
		taskCtx.scanCount = 0
		txn.SetOption(kv.Priority, w.priority)
		if tagger := w.reorgInfo.d.getResourceGroupTaggerForTopSQL(w.reorgInfo.Job); tagger != nil {
			txn.SetOption(kv.ResourceGroupTagger, tagger)
		}

		temporaryIndexRecords, nextKey, taskDone, err := w.fetchTempIndexVals(txn, taskRange)
		if err != nil {
			return errors.Trace(err)
		}
		taskCtx.nextKey = nextKey
		taskCtx.done = taskDone

		err = w.batchCheckTemporaryUniqueKey(txn, temporaryIndexRecords)
		if err != nil {
			return errors.Trace(err)
		}

		// Skip merge change after mergeSync
		err = w.batchSkipKey(txn, w.sessCtx.GetStore(), temporaryIndexRecords)
		if err != nil {
			return errors.Trace(err)
		}
		endPos := len(temporaryIndexRecords)
		for i, idxRecord := range temporaryIndexRecords {
			// The index is already exists, we skip it, no needs to backfill it.
			// The following update, delete, insert on these rows, TiDB can handle it correctly.
			// If all batch are skipped, update first index key to make txn commit to release lock.
			if idxRecord.skip && !w.skipAll {
				continue
			}
			if w.skipAll {
				isDelete := false
				unique := false
				length := len(w.firstVal)
				w.firstVal = w.firstVal[:length-1]
				length--

				if bytes.Equal(w.firstVal, []byte("delete")) {
					isDelete = true
					w.firstVal = w.firstVal[:length-6]
				} else if bytes.Equal(w.firstVal, []byte("deleteu")) {
					isDelete = true
					unique = true
					w.firstVal = w.firstVal[:length-7]
				}
				if isDelete {
					if unique {
						err = txn.GetMemBuffer().DeleteWithFlags(w.batchCheckTmpKeys[0], kv.SetNeedLocked)
					} else {
						err = txn.GetMemBuffer().Delete(w.batchCheckTmpKeys[0])
					}
				} else {
					// Set latest key/val back to temp index.
					err = txn.GetMemBuffer().Set(w.batchCheckTmpKeys[0], w.firstVal)
				}
				if err != nil {
					return err
				}
				break
			}

			if idxRecord.delete {
				if idxRecord.unique {
					err = txn.GetMemBuffer().DeleteWithFlags(idxRecord.key, kv.SetNeedLocked)
				} else {
					err = txn.GetMemBuffer().Delete(idxRecord.key)
				}
			} else {
				err = txn.GetMemBuffer().Set(idxRecord.key, idxRecord.vals)
				// If the merge key is batch end should be deleted in temp index to avoid
				// merge twice when do parallel merge procession.
				if i == endPos-1 {
					if idxRecord.unique {
						err = txn.GetMemBuffer().DeleteWithFlags(w.batchCheckTmpKeys[i], kv.SetNeedLocked)
					} else {
						err = txn.GetMemBuffer().Delete(w.batchCheckTmpKeys[i])
					}
				}
			}
			if err != nil {
				return err
			}
		}
		return nil
	})
	logSlowOperations(time.Since(oprStartTime), "AddIndexMergeDataInTxn", 3000)
	return
}

func (w *backFillIndexWorker) AddMetricInfo(cnt float64) {
}

// mergeTempIndex handles the merge temp index state for a table.
func (w *worker) mergeTempIndex(t table.Table, idx *model.IndexInfo, reorgInfo *reorgInfo) error {
	var err error
	if tbl, ok := t.(table.PartitionedTable); ok {
		var finish bool
		for !finish {
			p := tbl.GetPartition(reorgInfo.PhysicalTableID)
			if p == nil {
				return dbterror.ErrCancelledDDLJob.GenWithStack("Can not find partition id %d for table %d", reorgInfo.PhysicalTableID, t.Meta().ID)
			}
			err = w.addPhysicalTempIndex(p, idx, reorgInfo)
			if err != nil {
				break
			}
			finish, err = w.updateMergeInfo(tbl, idx.ID, reorgInfo)
			if err != nil {
				return errors.Trace(err)
			}
		}
	} else {
		err = w.addPhysicalTempIndex(t.(table.PhysicalTable), idx, reorgInfo)
	}
	return errors.Trace(err)
}

// updateMergeInfo will find the next partition according to current reorgInfo.
// If no more partitions, or table t is not a partitioned table, returns true to
// indicate that the reorganize work is finished.
func (w *worker) updateMergeInfo(t table.PartitionedTable, idxID int64, reorg *reorgInfo) (bool, error) {
	pi := t.Meta().GetPartitionInfo()
	if pi == nil {
		return true, nil
	}

	pid, err := findNextPartitionID(reorg.PhysicalTableID, pi.Definitions)
	if err != nil {
		// Fatal error, should not run here.
		logutil.BgLogger().Error("[ddl] find next partition ID failed", zap.Reflect("table", t), zap.Error(err))
		return false, errors.Trace(err)
	}
	if pid == 0 {
		// Next partition does not exist, all the job done.
		return true, nil
	}

	start, end := tablecodec.GetTableIndexKeyRange(pid, tablecodec.TempIndexPrefix|idxID)

	reorg.StartKey, reorg.EndKey, reorg.PhysicalTableID = start, end, pid
	// Write the reorg info to store so the whole reorganize process can recover from panic.
	err = reorg.UpdateReorgMeta(reorg.StartKey, w.sessPool)
	logutil.BgLogger().Info("[ddl] job update MergeInfo", zap.Int64("jobID", reorg.Job.ID),
		zap.ByteString("elementType", reorg.currElement.TypeKey), zap.Int64("elementID", reorg.currElement.ID),
		zap.Int64("partitionTableID", pid), zap.String("startHandle", tryDecodeToHandleString(start)),
		zap.String("endHandle", tryDecodeToHandleString(end)), zap.Error(err))
	return false, errors.Trace(err)
}

func (w *worker) addPhysicalTempIndex(t table.PhysicalTable, indexInfo *model.IndexInfo, reorgInfo *reorgInfo) error {
	logutil.BgLogger().Info("[ddl] start to merge temp index", zap.String("job", reorgInfo.Job.String()), zap.String("reorgInfo", reorgInfo.String()))
	return w.writeTempIndexRecord(t, typeAddIndexWorker, indexInfo, nil, nil, reorgInfo)
}

func (w *backFillIndexWorker) fetchTempIndexVals(txn kv.Transaction, taskRange reorgBackfillTask) ([]*temporaryIndexRecord, kv.Key, bool, error) {
	startTime := time.Now()
	w.tmpIdxRecords = w.tmpIdxRecords[:0]
	w.batchCheckTmpKeys = w.batchCheckTmpKeys[:0]
	// taskDone means that the merged handle is out of taskRange.endHandle.
	taskDone := false
	oprStartTime := startTime
	err := iterateSnapshotIndexes(w.reorgInfo.d.jobContext(w.reorgInfo.Job), w.sessCtx.GetStore(), w.priority, w.table, txn.StartTS(), taskRange.startKey, taskRange.endKey, func(indexKey kv.Key, rawValue []byte) (more bool, err error) {
		oprEndTime := time.Now()
		logSlowOperations(oprEndTime.Sub(oprStartTime), "iterate temporary index in merge process", 0)
		oprStartTime = oprEndTime

		taskDone := indexKey.Cmp(taskRange.endKey) > 0

		if taskDone || len(w.tmpIdxRecords) >= w.batchCnt {
			return false, nil
		}

		isDelete := false
		unique := false
		skip := false
		var keyVer []byte
		length := len(rawValue)
		keyVer = append(keyVer, rawValue[length-1:]...)
		rawValue = rawValue[:length-1]
		length--
		// Just skip it.
		if bytes.Equal(keyVer, []byte("m")) {
			return true, nil
		}
		if bytes.Equal(rawValue, []byte("delete")) {
			isDelete = true
			rawValue = rawValue[:length-6]
		} else if bytes.Equal(rawValue, []byte("deleteu")) {
			isDelete = true
			unique = true
			rawValue = rawValue[:length-7]
		}
		var convertedIndexKey []byte
		convertedIndexKey = append(convertedIndexKey, indexKey...)
		tablecodec.TempIndexKey2IndexKey(w.index.Meta().ID, convertedIndexKey)
		idxRecord := &temporaryIndexRecord{key: convertedIndexKey, delete: isDelete, unique: unique, keyVer: keyVer, skip: skip}
		if !isDelete {
			idxRecord.vals = rawValue
		}
		w.tmpIdxRecords = append(w.tmpIdxRecords, idxRecord)
		w.batchCheckTmpKeys = append(w.batchCheckTmpKeys, indexKey)
		return true, nil
	})

	if len(w.tmpIdxRecords) == 0 {
		taskDone = true
	}
	var nextKey kv.Key
	if taskDone {
		nextKey = taskRange.endKey
	} else {
		var convertedNextKey []byte
		lastPos := len(w.tmpIdxRecords)
		convertedNextKey = append(convertedNextKey, w.tmpIdxRecords[lastPos-1].key...)
		tablecodec.IndexKey2TempIndexKey(w.index.Meta().ID, convertedNextKey)
		nextKey = convertedNextKey
	}

	logutil.BgLogger().Debug("[ddl] merge temp index txn fetches handle info", zap.Uint64("txnStartTS", txn.StartTS()),
		zap.String("taskRange", taskRange.String()), zap.Duration("takeTime", time.Since(startTime)))
	return w.tmpIdxRecords, nextKey.Next(), taskDone, errors.Trace(err)
}
