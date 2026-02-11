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
	"context"
	"path"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/executor/importer"
	tidbkv "github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/lightning/backend/external"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/objstore"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/pingcap/tidb/pkg/util/rowcodec"
	"github.com/stretchr/testify/require"
)

func TestApplyDeltaRowMap(t *testing.T) {
	baseMap := map[int64]types.Datum{
		1: types.NewIntDatum(1),
		2: types.NewIntDatum(2),
		3: types.NewIntDatum(3),
	}
	deltaMap := map[int64]types.Datum{
		2: types.NewIntDatum(20),
	}
	colIDsByIdx := []int64{1, 2, 3}
	bitmap := importer.BuildColumnBitmap([]bool{false, true, true})

	out := applyDeltaRowMap(baseMap, deltaMap, bitmap, colIDsByIdx)
	v1 := out[1]
	require.Equal(t, int64(1), v1.GetInt64())
	v2 := out[2]
	require.Equal(t, int64(20), v2.GetInt64())
	v3 := out[3]
	require.True(t, v3.IsNull())
}

func TestDecodeDeltaRowToMap(t *testing.T) {
	loc := time.UTC
	colTypes := map[int64]*types.FieldType{
		1: types.NewFieldType(mysql.TypeLonglong),
		2: types.NewFieldType(mysql.TypeLonglong),
	}
	rowValue, err := tablecodec.EncodeRow(loc, []types.Datum{types.NewIntDatum(42)}, []int64{2}, nil, nil, rowcodec.NoChecksum{}, &rowcodec.Encoder{})
	require.NoError(t, err)
	encoded := importer.EncodeDeltaRowValue(rowValue, importer.BuildColumnBitmap([]bool{false, true}))

	bitmap, rowMap, err := decodeDeltaRowToMap(encoded, tidbkv.IntHandle(7), colTypes, []int64{1}, loc)
	require.NoError(t, err)
	require.True(t, importer.ColumnInBitmap(bitmap, 1))
	r1 := rowMap[1]
	require.Equal(t, int64(7), r1.GetInt64())
	r2 := rowMap[2]
	require.Equal(t, int64(42), r2.GetInt64())
}

func TestBuildRowFromMap(t *testing.T) {
	sctx := mock.NewContext()
	defer sctx.Close()

	col1 := &table.Column{ColumnInfo: &model.ColumnInfo{
		ID:                 1,
		Offset:             0,
		FieldType:          *types.NewFieldType(mysql.TypeLonglong),
		OriginDefaultValue: int64(0),
	}}
	col2 := &table.Column{ColumnInfo: &model.ColumnInfo{
		ID:        2,
		Offset:    1,
		FieldType: *types.NewFieldType(mysql.TypeLonglong),
	}}
	cols := []*table.Column{col1, col2}

	rowMap := map[int64]types.Datum{
		2: types.NewIntDatum(100),
	}
	row, err := buildRowFromMap(sctx.GetExprCtx(), cols, rowMap)
	require.NoError(t, err)
	require.Len(t, row, 2)
	require.Equal(t, int64(0), row[0].GetInt64())
	require.Equal(t, int64(100), row[1].GetInt64())
}

func TestCollectBaseDataFiles(t *testing.T) {
	ctx := context.Background()
	store := objstore.NewMemStorage()

	writeFile := func(baseID, name string) {
		filePath := path.Join(baseManifestDirName, baseID, "region", "r1", "00-10", external.DataKVGroup, name)
		require.NoError(t, store.WriteFile(ctx, filePath, []byte("x")))
	}
	writeFile("base-1", "file-1")
	writeFile("base-2", "file-2")

	id, files, err := collectBaseDataFiles(ctx, store, "")
	require.NoError(t, err)
	require.Equal(t, "base-2", id)
	require.Len(t, files, 1)

	id, files, err = collectBaseDataFiles(ctx, store, "base-1")
	require.NoError(t, err)
	require.Equal(t, "base-1", id)
	require.Len(t, files, 1)

	_, _, err = collectBaseDataFiles(ctx, store, "base-3")
	require.Error(t, err)
}

func TestAdvanceIter(t *testing.T) {
	ctx := context.Background()
	store := objstore.NewMemStorage()

	var summary *external.WriterSummary
	writer := external.NewWriterBuilder().
		SetOnCloseFunc(func(s *external.WriterSummary) { summary = s }).
		Build(store, path.Join(baseManifestDirName, "base-1", "region", "r1", "00-10"), path.Join(external.DataKVGroup, "w1"))
	require.NoError(t, writer.WriteRow(ctx, []byte("a"), []byte("1"), nil))
	require.NoError(t, writer.WriteRow(ctx, []byte("b"), []byte("2"), nil))
	require.NoError(t, writer.WriteRow(ctx, []byte("c"), []byte("3"), nil))
	require.NoError(t, writer.Close(ctx))
	require.NotNil(t, summary)

	dataFiles, _ := extractDataStatFiles(summary.MultipleFilesStats)
	iter, err := external.NewMergeKVIter(ctx, dataFiles, make([]uint64, len(dataFiles)), store, external.DefaultReadBufferSize, false, 1)
	require.NoError(t, err)
	defer iter.Close()
	require.True(t, advanceIter(iter, []byte("b"), []byte("d")))
	require.Equal(t, []byte("b"), iter.Key())

	iter2, err := external.NewMergeKVIter(ctx, dataFiles, make([]uint64, len(dataFiles)), store, external.DefaultReadBufferSize, false, 1)
	require.NoError(t, err)
	defer iter2.Close()
	require.False(t, advanceIter(iter2, []byte("z"), nil))
}
