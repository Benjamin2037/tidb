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

package executor

import (
	"testing"
	"time"

	dxfmetering "github.com/pingcap/tidb/pkg/dxf/framework/metering"
	"github.com/pingcap/tidb/pkg/executor/importer"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/stretchr/testify/require"
)

func TestAggregateMeteringCost(t *testing.T) {
	items := []meteringItem{
		{
			timestamp:   1,
			getRequests: 1000,
			putRequests: 2000,
		},
		{
			timestamp:   1,
			getRequests: 1000,
		},
	}
	res := aggregateMeteringCost(items)
	row, ok := res[1]
	require.True(t, ok)
	require.InDelta(t, 0.0008, row.getCost, 0.0000001)
	require.InDelta(t, 0.01, row.putCost, 0.0000001)
	require.InDelta(t, 0.0108, row.totalCost, 0.0000001)
}

func TestBytesToGB(t *testing.T) {
	require.Equal(t, 0.0, bytesToGB(0))
	require.Equal(t, 0.0, bytesToGB(-1))
	require.InDelta(t, 1.0, bytesToGB(1024*1024*1024), 0.0000001)
}

func TestParseInt64Field(t *testing.T) {
	require.Equal(t, int64(0), parseInt64Field(nil, "k"))
	require.Equal(t, int64(0), parseInt64Field(map[string]any{}, "k"))
	require.Equal(t, int64(1), parseInt64Field(map[string]any{"k": 1}, "k"))
	require.Equal(t, int64(2), parseInt64Field(map[string]any{"k": int64(2)}, "k"))
	require.Equal(t, int64(3), parseInt64Field(map[string]any{"k": uint64(3)}, "k"))
	require.Equal(t, int64(4), parseInt64Field(map[string]any{"k": float64(4)}, "k"))
	require.Equal(t, int64(6), parseInt64Field(map[string]any{"k": "6"}, "k"))
	require.Equal(t, int64(0), parseInt64Field(map[string]any{"k": "bad"}, "k"))
}

func TestGetMeteringTimeRange(t *testing.T) {
	start := time.Date(2026, 2, 2, 10, 3, 30, 0, time.UTC)
	end := time.Date(2026, 2, 2, 10, 2, 20, 0, time.UTC)
	info := &importer.JobInfo{
		CreateTime: types.NewTime(types.FromGoTime(start), mysql.TypeDatetime, types.DefaultFsp),
		EndTime:    types.NewTime(types.FromGoTime(end), mysql.TypeDatetime, types.DefaultFsp),
	}
	gotStart, gotEnd, err := getMeteringTimeRange(info, time.UTC)
	require.NoError(t, err)
	require.Equal(t, end.Truncate(dxfmetering.FlushInterval), gotStart)
	require.Equal(t, start.Truncate(dxfmetering.FlushInterval), gotEnd)
}
