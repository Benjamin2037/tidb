// Copyright 2025 PingCAP, Inc.
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

package importer_test

import (
	"context"
	"testing"

	"github.com/pingcap/tidb/pkg/executor/importer"
	"github.com/pingcap/tidb/pkg/lightning/backend/encode"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/session"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/stretchr/testify/require"
)

func TestKVEncoderForDupResolve(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t(a bigint primary key nonclustered) SHARD_ROW_ID_BITS = 6")
	do, err := session.GetDomain(store)
	require.NoError(t, err)
	table, err := do.InfoSchema().TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("t"))
	require.NoError(t, err)

	doTestFn := func(t *testing.T, useIdentityAutoRowID bool, checkerFn func(handleVal int64)) {
		encodeCfg := &encode.EncodingConfig{
			Table:                table,
			UseIdentityAutoRowID: useIdentityAutoRowID,
		}
		controller := &importer.LoadDataController{
			ASTArgs: &importer.ASTArgs{},
			Plan:    &importer.Plan{},
			Table:   table,
		}
		encoder, err := importer.NewTableKVEncoderForDupResolve(encodeCfg, controller)
		require.NoError(t, err)
		for range 10 {
			pairs, err := encoder.Encode([]types.Datum{types.NewDatum(1)}, 1)
			require.NoError(t, err)
			require.Len(t, pairs.Pairs, 2)
			var metRecordKey bool
			for _, pair := range pairs.Pairs {
				if !tablecodec.IsRecordKey(pair.Key) {
					continue
				}
				metRecordKey = true
				handle, err := tablecodec.DecodeRowKey(pair.Key)
				require.NoError(t, err)
				checkerFn(handle.IntValue())
			}
			require.True(t, metRecordKey)
		}
	}

	t.Run("identity auto row id", func(t *testing.T) {
		doTestFn(t, true, func(handleVal int64) {
			require.EqualValues(t, 1, handleVal)
		})
	})

	t.Run("without identity auto row id", func(t *testing.T) {
		// we loop 10 times, at least one should have shard bit larger than 1
		var handleLargerThanOneCount int
		doTestFn(t, false, func(handleVal int64) {
			if handleVal > 1 {
				handleLargerThanOneCount++
			}
		})
		require.Greater(t, handleLargerThanOneCount, 1)
	})
}

func TestDeltaRowEncodingRecordOnly(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (a bigint primary key, b int, index idx_b(b))")
	do, err := session.GetDomain(store)
	require.NoError(t, err)
	table, err := do.InfoSchema().TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("t"))
	require.NoError(t, err)

	buildController := func(mode importer.UpsertMode) *importer.LoadDataController {
		cols := table.VisibleCols()
		mappings := make([]*importer.FieldMapping, 0, len(cols))
		for _, col := range cols {
			mappings = append(mappings, &importer.FieldMapping{Column: col})
		}
		return &importer.LoadDataController{
			ASTArgs:       &importer.ASTArgs{},
			Plan:          &importer.Plan{UpsertMode: mode},
			Table:         table,
			FieldMappings: mappings,
			InsertColumns: cols,
		}
	}

	encodeCfg := &encode.EncodingConfig{Table: table}
	deltaEnc, err := importer.NewTableKVEncoder(encodeCfg, buildController(importer.UpsertModeDelta))
	require.NoError(t, err)
	plainEnc, err := importer.NewTableKVEncoder(encodeCfg, buildController(importer.UpsertModeNone))
	require.NoError(t, err)

	row := []types.Datum{types.NewDatum(1), types.NewDatum(2)}
	deltaPairs, err := deltaEnc.Encode(row, 1)
	require.NoError(t, err)
	plainPairs, err := plainEnc.Encode(row, 1)
	require.NoError(t, err)

	plainByKey := make(map[string][]byte, len(plainPairs.Pairs))
	for _, pair := range plainPairs.Pairs {
		plainByKey[string(pair.Key)] = pair.Val
	}
	for _, pair := range deltaPairs.Pairs {
		plainVal, ok := plainByKey[string(pair.Key)]
		require.True(t, ok)
		if tablecodec.IsRecordKey(pair.Key) {
			bitmap, raw, err := importer.DecodeDeltaRowValue(pair.Val)
			require.NoError(t, err)
			require.NotEmpty(t, bitmap)
			require.Equal(t, plainVal, raw)
		} else {
			require.Equal(t, plainVal, pair.Val)
		}
	}
}
