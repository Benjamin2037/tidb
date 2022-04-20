package request

import (
	"sync/atomic"

	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/statistics"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/memory"
	"github.com/pingcap/tidb/util/ranger"
)

// TableHandleRangesToKVRanges convert table handle ranges to "KeyRanges" for multiple tables.
func TableHandleRangesToKVRanges(sc *stmtctx.StatementContext, tid []int64, isCommonHandle bool, ranges []*ranger.Range, fb *statistics.QueryFeedback) ([]kv.KeyRange, error) {
	if !isCommonHandle {
		return tablesRangesToKVRanges(tid, ranges, fb), nil
	}
	return CommonHandleRangesToKVRanges(sc, tid, ranges)
}

// TableRangesToKVRanges converts table ranges to "KeyRange".
// Note this function should not be exported, but currently
// br refers to it, so have to keep it.
func TableRangesToKVRanges(tid int64, ranges []*ranger.Range, fb *statistics.QueryFeedback) []kv.KeyRange {
	return tablesRangesToKVRanges([]int64{tid}, ranges, fb)
}

// tablesRangesToKVRanges converts table ranges to "KeyRange".
func tablesRangesToKVRanges(tids []int64, ranges []*ranger.Range, fb *statistics.QueryFeedback) []kv.KeyRange {
	if fb == nil || fb.Hist == nil {
		return tableRangesToKVRangesWithoutSplit(tids, ranges)
	}
	krs := make([]kv.KeyRange, 0, len(ranges))
	feedbackRanges := make([]*ranger.Range, 0, len(ranges))
	for _, ran := range ranges {
		low := codec.EncodeInt(nil, ran.LowVal[0].GetInt64())
		high := codec.EncodeInt(nil, ran.HighVal[0].GetInt64())
		if ran.LowExclude {
			low = kv.Key(low).PrefixNext()
		}
		// If this range is split by histogram, then the high val will equal to one bucket's upper bound,
		// since we need to guarantee each range falls inside the exactly one bucket, `PrefixNext` will make the
		// high value greater than upper bound, so we store the range here.
		r := &ranger.Range{LowVal: []types.Datum{types.NewBytesDatum(low)},
			HighVal: []types.Datum{types.NewBytesDatum(high)}}
		feedbackRanges = append(feedbackRanges, r)

		if !ran.HighExclude {
			high = kv.Key(high).PrefixNext()
		}
		for _, tid := range tids {
			startKey := tablecodec.EncodeRowKey(tid, low)
			endKey := tablecodec.EncodeRowKey(tid, high)
			krs = append(krs, kv.KeyRange{StartKey: startKey, EndKey: endKey})
		}
	}
	fb.StoreRanges(feedbackRanges)
	return krs
}

func tableRangesToKVRangesWithoutSplit(tids []int64, ranges []*ranger.Range) []kv.KeyRange {
	krs := make([]kv.KeyRange, 0, len(ranges)*len(tids))
	for _, ran := range ranges {
		low, high := encodeHandleKey(ran)
		for _, tid := range tids {
			startKey := tablecodec.EncodeRowKey(tid, low)
			endKey := tablecodec.EncodeRowKey(tid, high)
			krs = append(krs, kv.KeyRange{StartKey: startKey, EndKey: endKey})
		}
	}
	return krs
}

func encodeHandleKey(ran *ranger.Range) ([]byte, []byte) {
	low := codec.EncodeInt(nil, ran.LowVal[0].GetInt64())
	high := codec.EncodeInt(nil, ran.HighVal[0].GetInt64())
	if ran.LowExclude {
		low = kv.Key(low).PrefixNext()
	}
	if !ran.HighExclude {
		high = kv.Key(high).PrefixNext()
	}
	return low, high
}

// IndexRangesToKVRangesWithInterruptSignal converts index ranges to "KeyRange".
// The process can be interrupted by set `interruptSignal` to true.
func IndexRangesToKVRangesWithInterruptSignal(sc *stmtctx.StatementContext, tid, idxID int64, ranges []*ranger.Range, fb *statistics.QueryFeedback, memTracker *memory.Tracker, interruptSignal *atomic.Value) ([]kv.KeyRange, error) {
	return indexRangesToKVRangesForTablesWithInterruptSignal(sc, []int64{tid}, idxID, ranges, fb, memTracker, interruptSignal)
}

// IndexRangesToKVRangesForTables converts indexes ranges to "KeyRange".
func IndexRangesToKVRangesForTables(sc *stmtctx.StatementContext, tids []int64, idxID int64, ranges []*ranger.Range, fb *statistics.QueryFeedback) ([]kv.KeyRange, error) {
	return indexRangesToKVRangesForTablesWithInterruptSignal(sc, tids, idxID, ranges, fb, nil, nil)
}

// IndexRangesToKVRangesForTablesWithInterruptSignal converts indexes ranges to "KeyRange".
// The process can be interrupted by set `interruptSignal` to true.
func indexRangesToKVRangesForTablesWithInterruptSignal(sc *stmtctx.StatementContext, tids []int64, idxID int64, ranges []*ranger.Range, fb *statistics.QueryFeedback, memTracker *memory.Tracker, interruptSignal *atomic.Value) ([]kv.KeyRange, error) {
	if fb == nil || fb.Hist == nil {
		return indexRangesToKVWithoutSplit(sc, tids, idxID, ranges, memTracker, interruptSignal)
	}
	feedbackRanges := make([]*ranger.Range, 0, len(ranges))
	for _, ran := range ranges {
		low, high, err := encodeIndexKey(sc, ran)
		if err != nil {
			return nil, err
		}
		feedbackRanges = append(feedbackRanges, &ranger.Range{LowVal: []types.Datum{types.NewBytesDatum(low)},
			HighVal: []types.Datum{types.NewBytesDatum(high)}, LowExclude: false, HighExclude: true})
	}
	feedbackRanges, ok := fb.Hist.SplitRange(sc, feedbackRanges, true)
	if !ok {
		fb.Invalidate()
	}
	krs := make([]kv.KeyRange, 0, len(feedbackRanges))
	for _, ran := range feedbackRanges {
		low, high := ran.LowVal[0].GetBytes(), ran.HighVal[0].GetBytes()
		if ran.LowExclude {
			low = kv.Key(low).PrefixNext()
		}
		ran.LowVal[0].SetBytes(low)
		// If this range is split by histogram, then the high val will equal to one bucket's upper bound,
		// since we need to guarantee each range falls inside the exactly one bucket, `PrefixNext` will make the
		// high value greater than upper bound, so we store the high value here.
		ran.HighVal[0].SetBytes(high)
		if !ran.HighExclude {
			high = kv.Key(high).PrefixNext()
		}
		for _, tid := range tids {
			startKey := tablecodec.EncodeIndexSeekKey(tid, idxID, low)
			endKey := tablecodec.EncodeIndexSeekKey(tid, idxID, high)
			krs = append(krs, kv.KeyRange{StartKey: startKey, EndKey: endKey})
		}
	}
	fb.StoreRanges(feedbackRanges)
	return krs, nil
}

// CommonHandleRangesToKVRanges converts common handle ranges to "KeyRange".
func CommonHandleRangesToKVRanges(sc *stmtctx.StatementContext, tids []int64, ranges []*ranger.Range) ([]kv.KeyRange, error) {
	rans := make([]*ranger.Range, 0, len(ranges))
	for _, ran := range ranges {
		low, high, err := encodeIndexKey(sc, ran)
		if err != nil {
			return nil, err
		}
		rans = append(rans, &ranger.Range{LowVal: []types.Datum{types.NewBytesDatum(low)},
			HighVal: []types.Datum{types.NewBytesDatum(high)}, LowExclude: false, HighExclude: true})
	}
	krs := make([]kv.KeyRange, 0, len(rans))
	for _, ran := range rans {
		low, high := ran.LowVal[0].GetBytes(), ran.HighVal[0].GetBytes()
		if ran.LowExclude {
			low = kv.Key(low).PrefixNext()
		}
		ran.LowVal[0].SetBytes(low)
		for _, tid := range tids {
			startKey := tablecodec.EncodeRowKey(tid, low)
			endKey := tablecodec.EncodeRowKey(tid, high)
			krs = append(krs, kv.KeyRange{StartKey: startKey, EndKey: endKey})
		}
	}
	return krs, nil
}

func indexRangesToKVWithoutSplit(sc *stmtctx.StatementContext, tids []int64, idxID int64, ranges []*ranger.Range, memTracker *memory.Tracker, interruptSignal *atomic.Value) ([]kv.KeyRange, error) {
	krs := make([]kv.KeyRange, 0, len(ranges))
	const CheckSignalStep = 8
	var estimatedMemUsage int64
	// encodeIndexKey and EncodeIndexSeekKey is time-consuming, thus we need to
	// check the interrupt signal periodically.
	for i, ran := range ranges {
		low, high, err := encodeIndexKey(sc, ran)
		if err != nil {
			return nil, err
		}
		if i == 0 {
			estimatedMemUsage += int64(cap(low) + cap(high))
		}
		for _, tid := range tids {
			startKey := tablecodec.EncodeIndexSeekKey(tid, idxID, low)
			endKey := tablecodec.EncodeIndexSeekKey(tid, idxID, high)
			if i == 0 {
				estimatedMemUsage += int64(cap(startKey)) + int64(cap(endKey))
			}
			krs = append(krs, kv.KeyRange{StartKey: startKey, EndKey: endKey})
		}
		if i%CheckSignalStep == 0 {
			if i == 0 && memTracker != nil {
				estimatedMemUsage *= int64(len(ranges))
				memTracker.Consume(estimatedMemUsage)
			}
			if interruptSignal != nil && interruptSignal.Load().(bool) {
				return nil, nil
			}
		}
	}
	return krs, nil
}

func encodeIndexKey(sc *stmtctx.StatementContext, ran *ranger.Range) ([]byte, []byte, error) {
	low, err := codec.EncodeKey(sc, nil, ran.LowVal...)
	if err != nil {
		return nil, nil, err
	}
	if ran.LowExclude {
		low = kv.Key(low).PrefixNext()
	}
	high, err := codec.EncodeKey(sc, nil, ran.HighVal...)
	if err != nil {
		return nil, nil, err
	}

	if !ran.HighExclude {
		high = kv.Key(high).PrefixNext()
	}

	var hasNull bool
	for _, highVal := range ran.HighVal {
		if highVal.IsNull() {
			hasNull = true
			break
		}
	}

	if hasNull {
		// Append 0 to make unique-key range [null, null] to be a scan rather than point-get.
		high = kv.Key(high).Next()
	}
	return low, high, nil
}

// IndexRangesToKVRanges converts index ranges to "KeyRange".
func IndexRangesToKVRanges(sc *stmtctx.StatementContext, tid, idxID int64, ranges []*ranger.Range, fb *statistics.QueryFeedback) ([]kv.KeyRange, error) {
	return IndexRangesToKVRangesWithInterruptSignal(sc, tid, idxID, ranges, fb, nil, nil)
}
