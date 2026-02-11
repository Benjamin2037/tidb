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
	"bytes"
	"context"
	"encoding/json"
	"path"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/config"
	dxfhandle "github.com/pingcap/tidb/pkg/dxf/framework/handle"
	"github.com/pingcap/tidb/pkg/dxf/framework/proto"
	"github.com/pingcap/tidb/pkg/dxf/framework/taskexecutor"
	"github.com/pingcap/tidb/pkg/dxf/framework/taskexecutor/execute"
	"github.com/pingcap/tidb/pkg/executor/importer"
	"github.com/pingcap/tidb/pkg/expression/exprctx"
	tidbkv "github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/lightning/backend/external"
	"github.com/pingcap/tidb/pkg/lightning/log"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/objstore/storeapi"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/table/tables"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/types"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

type regionMergeStepExecutor struct {
	taskexecutor.BaseStepExecutor
	task     *proto.TaskBase
	store    tidbkv.Storage
	taskMeta *TaskMeta
	logger   *zap.Logger

	tableImporter *importer.TableImporter
	indicesGenKV  map[int64]importer.GenKVIndex

	summary execute.SubtaskSummary
}

var _ execute.StepExecutor = &regionMergeStepExecutor{}

// NewRegionMergeStepExecutor creates a new executor for region merge and rebuild.
func NewRegionMergeStepExecutor(
	task *proto.TaskBase,
	store tidbkv.Storage,
	taskMeta *TaskMeta,
	logger *zap.Logger,
) execute.StepExecutor {
	return &regionMergeStepExecutor{
		task:         task,
		store:        store,
		taskMeta:     taskMeta,
		logger:       logger,
		indicesGenKV: importer.GetIndicesGenKV(taskMeta.Plan.TableInfo),
	}
}

func (e *regionMergeStepExecutor) Init(ctx context.Context) error {
	tableImporter, err := getTableImporter(ctx, e.task.ID, e.taskMeta, e.store, e.logger)
	if err != nil {
		return err
	}
	e.tableImporter = tableImporter
	return nil
}

func (e *regionMergeStepExecutor) RunSubtask(ctx context.Context, subtask *proto.Subtask) (err error) {
	logger := e.logger.With(zap.Int64("subtask-id", subtask.ID))
	task := log.BeginTask(logger, "run subtask")
	defer func() {
		task.End(zapcore.ErrorLevel, err)
	}()

	stMeta := &RegionMergeStepMeta{}
	if err = json.Unmarshal(subtask.Meta, stMeta); err != nil {
		return errors.Trace(err)
	}

	deltaURI := stMeta.DeltaStoreURI
	if deltaURI == "" {
		deltaURI = e.taskMeta.Plan.CloudStorageURI
	}
	accessRecDelta, deltaStore, err := dxfhandle.NewObjStoreWithRecording(ctx, deltaURI)
	if err != nil {
		return err
	}
	defer func() {
		deltaStore.Close()
		e.summary.MergeObjStoreRequests(&accessRecDelta.Requests)
		e.GetMeterRecorder().MergeObjStoreAccess(accessRecDelta)
	}()

	if stMeta.ExternalPath != "" {
		if err := stMeta.ReadJSONFromExternalStorage(ctx, deltaStore, stMeta); err != nil {
			return errors.Trace(err)
		}
	}

	baseURI := stMeta.BaseURI
	if baseURI == "" {
		baseURI = e.taskMeta.Plan.CloudStorageURI
	}
	accessRecBase, baseStore, err := dxfhandle.NewObjStoreWithRecording(ctx, baseURI)
	if err != nil {
		return err
	}
	defer func() {
		baseStore.Close()
		e.summary.MergeObjStoreRequests(&accessRecBase.Requests)
		e.GetMeterRecorder().MergeObjStoreAccess(accessRecBase)
	}()

	useRemoteS3Base := stMeta.BaseManifestPath == ""
	var baseManifest *BaseManifest
	if !useRemoteS3Base {
		baseManifest, err = ReadBaseManifest(ctx, baseStore, stMeta.BaseManifestPath)
		if err != nil {
			return errors.Trace(err)
		}
	} else {
		logger.Warn("base manifest missing, fallback to remote coprocessor scan on S3 SSTs")
	}

	changedRegions, err := ReadChangedRegionsManifest(ctx, deltaStore, stMeta.ChangedRegionsPath)
	if err != nil {
		return errors.Trace(err)
	}
	if !useRemoteS3Base && changedRegions.BaseID != "" && changedRegions.BaseID != baseManifest.BaseID {
		return errors.Errorf("base id mismatch: changed regions %s, base manifest %s", changedRegions.BaseID, baseManifest.BaseID)
	}

	if len(changedRegions.Regions) == 0 {
		if !useRemoteS3Base {
			// Rationale: no touched ranges means delta introduces no effective
			// changes. Reuse the previous manifest content and only rotate
			// version metadata to avoid unnecessary rewrite/ingest work.
			out := *baseManifest
			out.BaseID = stMeta.OutputBaseID
			out.BaseURI = stMeta.BaseURI
			out.CreateTime = time.Now().UTC()
			if err := WriteBaseManifest(ctx, baseStore, stMeta.OutputManifestPath, &out); err != nil {
				return errors.Trace(err)
			}
			return nil
		}
		changedRegions.Regions = append(changedRegions.Regions, ChangedRegionMeta{
			StartKey: "",
			EndKey:   "",
		})
	}

	encoder, err := e.tableImporter.GetKVEncoderForDupResolve()
	if err != nil {
		return errors.Trace(err)
	}
	defer func() {
		if err2 := encoder.Close(); err2 != nil && err == nil {
			err = err2
		}
	}()

	colTypes := make(map[int64]*types.FieldType, len(encoder.Columns))
	colIDsByIdx := make([]int64, len(encoder.Columns))
	for i, col := range encoder.Columns {
		colTypes[col.ID] = &col.FieldType
		colIDsByIdx[i] = col.ID
	}

	pkCols := []int64{model.ExtraHandleID}
	if e.taskMeta.Plan.TableInfo.IsCommonHandle {
		pkCols = tables.TryGetCommonPkColumnIds(e.taskMeta.Plan.TableInfo)
	} else if e.taskMeta.Plan.TableInfo.PKIsHandle {
		pkCols = []int64{e.taskMeta.Plan.TableInfo.GetPkColInfo().ID}
	}

	dataKVMemSize, perIndexMemSize := getWriterMemorySizeLimit(e.GetResource(), &e.taskMeta.Plan)
	dataBlockSize := external.GetAdjustedBlockSize(dataKVMemSize, config.MaxTxnEntrySizeLimit)
	indexBlockSize := external.GetAdjustedBlockSize(perIndexMemSize, external.DefaultBlockSize)

	changedRanges, err := decodeChangedRanges(changedRegions)
	if err != nil {
		return err
	}

	outputRegions := make([]BaseRegionMeta, 0, len(changedRanges))
	var totalRows, totalDataBytes, totalIndexBytes uint64

	if useRemoteS3Base {
		resolvedBaseID, baseDataFiles, err := collectBaseDataFiles(ctx, baseStore, stMeta.BaseID)
		if err != nil {
			return err
		}
		if stMeta.BaseID == "" && resolvedBaseID != "" {
			stMeta.BaseID = resolvedBaseID
		}
		for _, cr := range changedRanges {
			rebuilt, dataBytes, indexBytes, rowCount, err := e.rebuildRegion(
				ctx, baseStore, deltaStore, encoder,
				baseDataFiles, stMeta.DeltaDataFiles,
				cr.Start, cr.End,
				cr.StartHex, cr.EndHex,
				dataKVMemSize, perIndexMemSize,
				dataBlockSize, indexBlockSize,
				colTypes, colIDsByIdx, pkCols,
				stMeta.OutputBaseID,
			)
			if err != nil {
				return err
			}
			outputRegions = append(outputRegions, rebuilt)
			totalRows += rowCount
			totalDataBytes += dataBytes
			totalIndexBytes += indexBytes
		}
	} else {
		outputRegions = make([]BaseRegionMeta, 0, len(baseManifest.Regions)+len(changedRanges))
		covered := make([]bool, len(changedRanges))

		for _, region := range baseManifest.Regions {
			regionStart, err := decodeHexKey(region.StartKey)
			if err != nil {
				return errors.Annotate(err, "decode base region start key")
			}
			regionEnd, err := decodeHexKey(region.EndKey)
			if err != nil {
				return errors.Annotate(err, "decode base region end key")
			}
			needRebuild := false
			for i, cr := range changedRanges {
				if rangesOverlap(regionStart, regionEnd, cr.Start, cr.End) {
					needRebuild = true
					covered[i] = true
				}
			}
			if !needRebuild {
				outputRegions = append(outputRegions, region)
				continue
			}

			rebuilt, dataBytes, indexBytes, rowCount, err := e.rebuildRegion(
				ctx, baseStore, deltaStore, encoder,
				region.DataFiles, stMeta.DeltaDataFiles,
				regionStart, regionEnd,
				region.StartKey, region.EndKey,
				dataKVMemSize, perIndexMemSize,
				dataBlockSize, indexBlockSize,
				colTypes, colIDsByIdx, pkCols,
				stMeta.OutputBaseID,
			)
			if err != nil {
				return err
			}
			outputRegions = append(outputRegions, rebuilt)
			totalRows += rowCount
			totalDataBytes += dataBytes
			totalIndexBytes += indexBytes
		}

		for i, cr := range changedRanges {
			if covered[i] {
				continue
			}
			rebuilt, dataBytes, indexBytes, rowCount, err := e.rebuildRegion(
				ctx, baseStore, deltaStore, encoder,
				nil, stMeta.DeltaDataFiles,
				cr.Start, cr.End,
				cr.StartHex, cr.EndHex,
				dataKVMemSize, perIndexMemSize,
				dataBlockSize, indexBlockSize,
				colTypes, colIDsByIdx, pkCols,
				stMeta.OutputBaseID,
			)
			if err != nil {
				return err
			}
			outputRegions = append(outputRegions, rebuilt)
			totalRows += rowCount
			totalDataBytes += dataBytes
			totalIndexBytes += indexBytes
		}
	}

	sort.Slice(outputRegions, func(i, j int) bool {
		li, err1 := decodeHexKey(outputRegions[i].StartKey)
		lj, err2 := decodeHexKey(outputRegions[j].StartKey)
		if err1 != nil || err2 != nil {
			return outputRegions[i].StartKey < outputRegions[j].StartKey
		}
		return bytes.Compare(li, lj) < 0
	})

	rowCount := totalRows
	dataBytes := totalDataBytes
	indexBytes := totalIndexBytes
	if baseManifest != nil {
		rowCount = baseManifest.RowCount
		dataBytes = baseManifest.DataBytes
		indexBytes = baseManifest.IndexBytes
	}
	out := &BaseManifest{
		TableID:    e.taskMeta.Plan.TableInfo.ID,
		BaseID:     stMeta.OutputBaseID,
		BaseURI:    stMeta.BaseURI,
		CreateTime: time.Now().UTC(),
		RowCount:   rowCount,
		DataBytes:  dataBytes,
		IndexBytes: indexBytes,
		Regions:    outputRegions,
	}
	if err := WriteBaseManifest(ctx, baseStore, stMeta.OutputManifestPath, out); err != nil {
		return errors.Trace(err)
	}

	return nil
}

func (e *regionMergeStepExecutor) rebuildRegion(
	ctx context.Context,
	baseStore, deltaStore storeapi.Storage,
	encoder *importer.TableKVEncoder,
	baseDataFiles, deltaDataFiles []string,
	rangeStart, rangeEnd []byte,
	rangeStartHex, rangeEndHex string,
	dataKVMemSize, perIndexMemSize uint64,
	dataBlockSize, indexBlockSize int,
	colTypes map[int64]*types.FieldType,
	colIDsByIdx []int64,
	pkCols []int64,
	outputBaseID string,
) (BaseRegionMeta, uint64, uint64, uint64, error) {
	// Merge two sorted streams (base + delta) by row key, applying delta bitmap
	// to only overwrite columns present in the delta batch.
	prefix := pathForOutputRegion(outputBaseID, rangeStartHex, rangeEndHex)

	dataSummaryCh := make(chan *external.WriterSummary, 1)
	onDupData, err := getOnDupForKVGroup(e.indicesGenKV, external.DataKVGroup)
	if err != nil {
		return BaseRegionMeta{}, 0, 0, 0, err
	}
	dataWriter := external.NewWriterBuilder().
		SetOnCloseFunc(func(summary *external.WriterSummary) {
			dataSummaryCh <- summary
		}).
		SetMemorySizeLimit(dataKVMemSize).
		SetBlockSize(dataBlockSize).
		SetOnDup(onDupData).
		SetTiKVCodec(e.tableImporter.Backend().GetTiKVCodec()).
		Build(baseStore, prefix, path.Join(external.DataKVGroup, uuid.New().String()))

	indexWriters := make(map[int64]*external.Writer)
	indexSummaries := make([]*external.WriterSummary, 0, 4)
	indexSummaryMu := sync.Mutex{}
	getIndexWriter := func(indexID int64) (*external.Writer, error) {
		if w, ok := indexWriters[indexID]; ok {
			return w, nil
		}
		kvGroup := external.IndexID2KVGroup(indexID)
		onDup, err := getOnDupForKVGroup(e.indicesGenKV, kvGroup)
		if err != nil {
			return nil, err
		}
		builder := external.NewWriterBuilder().
			SetOnCloseFunc(func(summary *external.WriterSummary) {
				indexSummaryMu.Lock()
				indexSummaries = append(indexSummaries, summary)
				indexSummaryMu.Unlock()
			}).
			SetMemorySizeLimit(perIndexMemSize).
			SetBlockSize(indexBlockSize).
			SetOnDup(onDup).
			SetTiKVCodec(e.tableImporter.Backend().GetTiKVCodec())
		writerID := path.Join("index", kvGroup, uuid.New().String())
		w := builder.Build(baseStore, prefix, writerID)
		indexWriters[indexID] = w
		return w, nil
	}

	var baseIter, deltaIter *external.MergeKVIter
	if len(baseDataFiles) > 0 {
		baseIter, err = external.NewMergeKVIter(ctx, baseDataFiles, make([]uint64, len(baseDataFiles)), baseStore, external.DefaultReadBufferSize, false, 1)
		if err != nil {
			return BaseRegionMeta{}, 0, 0, 0, err
		}
		defer baseIter.Close()
	}
	if len(deltaDataFiles) > 0 {
		deltaIter, err = external.NewMergeKVIter(ctx, deltaDataFiles, make([]uint64, len(deltaDataFiles)), deltaStore, external.DefaultReadBufferSize, false, 1)
		if err != nil {
			return BaseRegionMeta{}, 0, 0, 0, err
		}
		defer deltaIter.Close()
	}

	loc := encoder.SessionCtx.GetExprCtx().GetEvalCtx().Location()
	exprCtx := encoder.SessionCtx.GetExprCtx()

	baseOK := advanceIter(baseIter, rangeStart, rangeEnd)
	deltaOK := advanceIter(deltaIter, rangeStart, rangeEnd)

	var rowCount uint64
	for baseOK || deltaOK {
		var key []byte
		var baseVal, deltaVal []byte
		useBase := false
		useDelta := false
		if baseOK && (!deltaOK || bytes.Compare(baseIter.Key(), deltaIter.Key()) < 0) {
			key = baseIter.Key()
			baseVal = baseIter.Value()
			useBase = true
			baseOK = advanceIter(baseIter, rangeStart, rangeEnd)
		} else if deltaOK && (!baseOK || bytes.Compare(deltaIter.Key(), baseIter.Key()) < 0) {
			key = deltaIter.Key()
			deltaVal = deltaIter.Value()
			useDelta = true
			deltaOK = advanceIter(deltaIter, rangeStart, rangeEnd)
		} else {
			key = baseIter.Key()
			baseVal = baseIter.Value()
			deltaVal = deltaIter.Value()
			useBase = true
			useDelta = true
			baseOK = advanceIter(baseIter, rangeStart, rangeEnd)
			deltaOK = advanceIter(deltaIter, rangeStart, rangeEnd)
		}

		handle, err := tablecodec.DecodeRowKey(key)
		if err != nil {
			return BaseRegionMeta{}, 0, 0, 0, err
		}
		var rowMap map[int64]types.Datum
		if useBase {
			rowMap, err = decodeRowToMap(baseVal, handle, colTypes, pkCols, loc)
			if err != nil {
				return BaseRegionMeta{}, 0, 0, 0, err
			}
		}
		if useDelta {
			bitmap, deltaMap, err := decodeDeltaRowToMap(deltaVal, handle, colTypes, pkCols, loc)
			if err != nil {
				return BaseRegionMeta{}, 0, 0, 0, err
			}
			rowMap = applyDeltaRowMap(rowMap, deltaMap, bitmap, colIDsByIdx)
		}
		if rowMap == nil {
			continue
		}

		row, err := buildRowFromMap(exprCtx, encoder.Columns, rowMap)
		if err != nil {
			return BaseRegionMeta{}, 0, 0, 0, err
		}

		rowID := int64(0)
		if handle != nil && handle.IsInt() {
			rowID = handle.IntValue()
		}
		kvPairs, err := encoder.EncodeFromDatumRow(row, rowID)
		if err != nil {
			return BaseRegionMeta{}, 0, 0, 0, err
		}
		for i := range kvPairs.Pairs {
			pair := kvPairs.Pairs[i]
			if tablecodec.IsRecordKey(pair.Key) {
				if err := dataWriter.WriteRow(ctx, pair.Key, pair.Val, nil); err != nil {
					return BaseRegionMeta{}, 0, 0, 0, err
				}
				continue
			}
			_, indexID, _, err := tablecodec.DecodeIndexKey(pair.Key)
			if err != nil {
				return BaseRegionMeta{}, 0, 0, 0, err
			}
			writer, err := getIndexWriter(indexID)
			if err != nil {
				return BaseRegionMeta{}, 0, 0, 0, err
			}
			if err := writer.WriteRow(ctx, pair.Key, pair.Val, nil); err != nil {
				return BaseRegionMeta{}, 0, 0, 0, err
			}
		}
		rowCount++
	}

	if err := dataWriter.Close(ctx); err != nil {
		return BaseRegionMeta{}, 0, 0, 0, err
	}
	for _, writer := range indexWriters {
		if err := writer.Close(ctx); err != nil {
			return BaseRegionMeta{}, 0, 0, 0, err
		}
	}

	dataSummary := <-dataSummaryCh
	dataFiles, statFiles := extractDataStatFiles(dataSummary.MultipleFilesStats)
	indexFiles, indexStatFiles := extractIndexFiles(indexSummaries)
	kvBytes := dataSummary.TotalSize
	var indexBytes uint64
	for _, summary := range indexSummaries {
		indexBytes += summary.TotalSize
		kvBytes += summary.TotalSize
	}

	return BaseRegionMeta{
		StartKey:       rangeStartHex,
		EndKey:         rangeEndHex,
		DataFiles:      dataFiles,
		StatFiles:      statFiles,
		IndexFiles:     indexFiles,
		IndexStatFiles: indexStatFiles,
		KVBytes:        kvBytes,
	}, dataSummary.TotalSize, indexBytes, rowCount, nil
}

func (e *regionMergeStepExecutor) Cleanup(ctx context.Context) error {
	if e.tableImporter != nil {
		if err := e.tableImporter.Close(); err != nil {
			e.logger.Warn("close table importer failed", zap.Error(err))
		}
		e.tableImporter = nil
	}
	return e.BaseStepExecutor.Cleanup(ctx)
}

func (e *regionMergeStepExecutor) RealtimeSummary() *execute.SubtaskSummary {
	e.summary.Update()
	return &e.summary
}

func (e *regionMergeStepExecutor) ResetSummary() {
	e.summary.Reset()
}

type changedRange struct {
	Start    []byte
	End      []byte
	StartHex string
	EndHex   string
}

func decodeChangedRanges(m *ChangedRegionsManifest) ([]changedRange, error) {
	ranges := make([]changedRange, 0, len(m.Regions))
	for _, r := range m.Regions {
		start, err := decodeHexKey(r.StartKey)
		if err != nil {
			return nil, errors.Annotate(err, "decode changed region start key")
		}
		end, err := decodeHexKey(r.EndKey)
		if err != nil {
			return nil, errors.Annotate(err, "decode changed region end key")
		}
		ranges = append(ranges, changedRange{
			Start:    start,
			End:      end,
			StartHex: r.StartKey,
			EndHex:   r.EndKey,
		})
	}
	return ranges, nil
}

func advanceIter(iter *external.MergeKVIter, start, end []byte) bool {
	if iter == nil {
		return false
	}
	for iter.Next() {
		key := iter.Key()
		// Iterators can return keys outside the target range; skip until within [start, end).
		if !withinRange(key, start, end) {
			if len(end) > 0 && bytes.Compare(key, end) >= 0 {
				return false
			}
			continue
		}
		return true
	}
	return false
}

func decodeRowToMap(value []byte, handle tidbkv.Handle, colTypes map[int64]*types.FieldType, pkCols []int64, loc *time.Location) (map[int64]types.Datum, error) {
	rowMap, err := tablecodec.DecodeRowToDatumMap(value, colTypes, loc)
	if err != nil {
		return nil, err
	}
	rowMap, err = tablecodec.DecodeHandleToDatumMap(handle, pkCols, colTypes, loc, rowMap)
	if err != nil {
		return nil, err
	}
	return rowMap, nil
}

func decodeDeltaRowToMap(value []byte, handle tidbkv.Handle, colTypes map[int64]*types.FieldType, pkCols []int64, loc *time.Location) ([]byte, map[int64]types.Datum, error) {
	bitmap, rowValue, err := importer.DecodeDeltaRowValue(value)
	if err != nil {
		return nil, nil, err
	}
	rowMap, err := tablecodec.DecodeRowToDatumMap(rowValue, colTypes, loc)
	if err != nil {
		return nil, nil, err
	}
	rowMap, err = tablecodec.DecodeHandleToDatumMap(handle, pkCols, colTypes, loc, rowMap)
	if err != nil {
		return nil, nil, err
	}
	return bitmap, rowMap, nil
}

func applyDeltaRowMap(baseMap map[int64]types.Datum, deltaMap map[int64]types.Datum, bitmap []byte, colIDsByIdx []int64) map[int64]types.Datum {
	// bitmap indicates which column offsets are present in delta; only those are overwritten.
	if baseMap == nil {
		baseMap = make(map[int64]types.Datum, len(colIDsByIdx))
	}
	for idx, colID := range colIDsByIdx {
		if !importer.ColumnInBitmap(bitmap, idx) {
			continue
		}
		if val, ok := deltaMap[colID]; ok {
			baseMap[colID] = val
		} else {
			baseMap[colID] = types.NewDatum(nil)
		}
	}
	return baseMap
}

func buildRowFromMap(ctx exprctx.ExprContext, cols []*table.Column, rowMap map[int64]types.Datum) ([]types.Datum, error) {
	row := make([]types.Datum, len(cols))
	defaultVals := make([]types.Datum, len(cols))
	for _, col := range cols {
		if col == nil {
			continue
		}
		if val, ok := rowMap[col.ID]; ok {
			row[col.Offset] = val
			continue
		}
		if col.IsGenerated() {
			row[col.Offset] = types.GetMinValue(&col.FieldType)
			continue
		}
		val, err := tables.GetColDefaultValue(ctx, col, defaultVals)
		if err != nil {
			return nil, err
		}
		row[col.Offset] = val
	}
	return row, nil
}

func extractDataStatFiles(stats []external.MultipleFilesStat) ([]string, []string) {
	dataFiles := make([]string, 0, len(stats))
	statFiles := make([]string, 0, len(stats))
	for _, stat := range stats {
		for _, files := range stat.Filenames {
			dataFiles = append(dataFiles, files[0])
			statFiles = append(statFiles, files[1])
		}
	}
	return dataFiles, statFiles
}

func extractIndexFiles(summaries []*external.WriterSummary) ([]string, []string) {
	indexFiles := make([]string, 0, len(summaries))
	statFiles := make([]string, 0, len(summaries))
	for _, summary := range summaries {
		for _, stat := range summary.MultipleFilesStats {
			for _, files := range stat.Filenames {
				indexFiles = append(indexFiles, files[0])
				if len(files) > 1 {
					statFiles = append(statFiles, files[1])
				}
			}
		}
	}
	return indexFiles, statFiles
}

func collectBaseDataFiles(ctx context.Context, store storeapi.Storage, baseID string) (string, []string, error) {
	if baseID != "" {
		files, err := listBaseDataFiles(ctx, store, path.Join(baseManifestDirName, baseID))
		if err != nil {
			return baseID, nil, err
		}
		if len(files) == 0 {
			return baseID, nil, errors.New("base data files not found")
		}
		return baseID, files, nil
	}

	filesByBase := make(map[string][]string)
	var (
		bestID       string
		bestJobID    int64
		bestJobFound bool
	)
	err := store.WalkDir(ctx, &storeapi.WalkOption{SubDir: baseManifestDirName}, func(p string, _ int64) error {
		id, ok := parseBaseIDFromDataPath(p)
		if !ok {
			return nil
		}
		filesByBase[id] = append(filesByBase[id], p)
		if jobID, ok := ParseBaseID(id); ok {
			if !bestJobFound || jobID > bestJobID {
				bestJobFound = true
				bestJobID = jobID
				bestID = id
			}
		} else if bestID == "" && !bestJobFound {
			bestID = id
		}
		return nil
	})
	if err != nil {
		return "", nil, errors.Trace(err)
	}
	if bestID == "" {
		return "", nil, errors.New("base data files not found")
	}
	return bestID, filesByBase[bestID], nil
}

func listBaseDataFiles(ctx context.Context, store storeapi.Storage, subdir string) ([]string, error) {
	files := make([]string, 0, 64)
	err := store.WalkDir(ctx, &storeapi.WalkOption{SubDir: subdir}, func(p string, _ int64) error {
		if _, ok := parseBaseIDFromDataPath(p); !ok {
			return nil
		}
		files = append(files, p)
		return nil
	})
	if err != nil {
		return nil, errors.Trace(err)
	}
	return files, nil
}

func parseBaseIDFromDataPath(p string) (string, bool) {
	parts := strings.Split(p, "/")
	if len(parts) < 6 {
		return "", false
	}
	if parts[0] != baseManifestDirName || parts[5] != external.DataKVGroup {
		return "", false
	}
	return parts[1], true
}

func pathForOutputRegion(baseID, startHex, endHex string) string {
	regionID := uuid.New().String()
	return path.Join(baseManifestDirName, baseID, "region", regionID, startHex+"-"+endHex)
}
