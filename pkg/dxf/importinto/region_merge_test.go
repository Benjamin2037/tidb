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
	"testing"

	"github.com/pingcap/tidb/pkg/executor/importer"
	"github.com/pingcap/tidb/pkg/types"
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
	require.Equal(t, int64(1), out[1].GetInt64())
	require.Equal(t, int64(20), out[2].GetInt64())
	require.True(t, out[3].IsNull())
}
