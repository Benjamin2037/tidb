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

package addindexlittest

import (
	"testing"

	"github.com/pingcap/tidb/testkit"
	"github.com/pingcap/tidb/tests/realtikvtest"
	"github.com/stretchr/testify/require"
)

func initTest(t *testing.T) (*suiteContext, error) {
	store := realtikvtest.CreateMockStoreAndSetup(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("drop database if exists addindexlit;")
	tk.MustExec("create database addindexlit;")
	tk.MustExec("use addindexlit;")
	tk.MustExec(`set @@tidb_ddl_enable_fast_reorg=on`)

	ctx := newSuiteContext(t, tk)
	// create table
	err := createTable(ctx)
	return ctx, err
}

func TestCreateNonUniqueIndex(t *testing.T) {
	var colIDs [][]int = [][]int{
		{1, 4, 7, 10, 13, 16, 19, 22, 25},
		{2, 5, 8, 11, 14, 17, 20, 23, 26},
		{},
	}
	ctx, err := initTest(t)
	require.NoError(t, err)
	testOneColFrame(ctx, colIDs, addIndexLitNonUnique)
}

func TestCreateUniqueIndex(t *testing.T) {
	var colIDs [][]int = [][]int{
		{1, 6, 7, 8, 11, 13, 15, 16, 18, 19, 22, 26},
		{2, 9, 11, 17},
		{3, 12, 25},
	}
	ctx, err := initTest(t)
	require.NoError(t, err)
	testOneColFrame(ctx, colIDs, addIndexLitUnique)
}

func TestCreatePK(t *testing.T) {
	ctx, err := initTest(t)
	require.NoError(t, err)
	testOneIndexFrame(ctx, 0, addIndexLitPK)
}

func TestCreateGenColIndex(t *testing.T) {
	ctx, err := initTest(t)
	require.NoError(t, err)
	testOneIndexFrame(ctx, 29, addIndexLitGenCol)
}

func TestCreateMultiColsIndex(t *testing.T) {
	var coliIDs [][]int = [][]int{
		{1, 4, 7, 10, 13},
		{2, 5, 8, 11},
		{3, 6, 9, 12, 15},
	}
	var coljIDs [][]int = [][]int{
		{16, 19, 22, 25},
		{14, 17, 20, 23, 26},
		{18, 21, 24, 27},
	}
	ctx, err := initTest(t)
	require.NoError(t, err)
	testTwoColsFrame(ctx, coliIDs, coljIDs, addIndexLitMultiCols)
}
