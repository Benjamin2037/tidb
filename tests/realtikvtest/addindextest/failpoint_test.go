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

package addindextest

import (
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/stretchr/testify/require"
)

type failpointPath struct {
	failpath string
	interm   string
}

var failpoints []failpointPath = []failpointPath{
	{"github.com/pingcap/tidb/ddl/EnablePiTR", "return"},
	{"github.com/pingcap/tidb/ddl/mockHighLoadForAddIndex", "return"},
	{"github.com/pingcap/tidb/ddl/mockBackfillRunErr", "1*return"},
	{"github.com/pingcap/tidb/ddl/mockBackfillSlow", "return"},
	{"github.com/pingcap/tidb/ddl/MockCaseWhenParseFailure", "return(true)"},
	{"github.com/pingcap/tidb/ddl/checkBackfillWorkerNum", "return(true)"},
	{"github.com/pingcap/tidb/ddl/checkMergeWorkerNum", "return(true)"},
	{"github.com/pingcap/tidb/ddl/mockHighLoadForMergeIndex", "return"},
	{"github.com/pingcap/tidb/ddl/mockMergeRunErr", "1*return"},
	{"github.com/pingcap/tidb/ddl/mockMergeSlow", "return"},
}

func initTestFailpoint(t *testing.T) *suiteContext {
	ctx := initTest(t)
	ctx.isFailpointTest = true
	return ctx   
}

func TestCreateNonUniqueIndexFailPoint(t *testing.T) {
	var colIDs = [][]int{
		{1, 4, 7, 10, 13, 16, 19, 22, 25},
		{2, 5, 8, 11, 14, 17, 20, 23, 26},
		{3, 6, 9, 12, 15, 18, 21, 24, 27},
	}
	ctx := initTestFailpoint(t)
	testOneColFrame(ctx, colIDs, addIndexNonUnique)
}

func TestCreateUniqueIndexFailPoint(t *testing.T) {
	var colIDs [][]int = [][]int{
		{1, 6, 7, 8, 11, 13, 15, 16, 18, 19, 22, 26},
		{2, 9, 11, 17},
		{3, 12, 25},
	}
	ctx := initTestFailpoint(t)
	testOneColFrame(ctx, colIDs, addIndexUnique)
}

func TestCreatePrimaryKeyFailpoint(t *testing.T) {
	ctx := initTest(t)
	testOneIndexFrame(ctx, 0, addIndexPK)
}

func TestCreateGenColIndexFailpoint(t *testing.T) {
	ctx := initTestFailpoint(t)
	testOneIndexFrame(ctx, 29, addIndexGenCol)
}

func TestCreateMultiColsIndexFailpoint(t *testing.T) {
	var coliIDs = [][]int{
		{1, 4, 7, 10, 13},
		{2, 5, 8, 11},
		{3, 6, 9, 12, 15},
	}
	var coljIDs = [][]int{
		{16, 19, 22, 25},
		{14, 17, 20, 23, 26},
		{18, 21, 24, 27},
	}
	ctx := initTestFailpoint(t)
	testTwoColsFrame(ctx, coliIDs, coljIDs, addIndexMultiCols)
}

func UseFailpoint(t *testing.T, failpos int) {
	failpos %= 10
	require.NoError(t, failpoint.Enable(failpoints[failpos].failpath, failpoints[failpos].interm))
	
	time.Sleep(10 * time.Second)
	require.NoError(t, failpoint.Disable(failpoints[failpos].failpath))	
}