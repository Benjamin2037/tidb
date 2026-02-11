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

package local

import (
	"context"
	"path/filepath"
	"testing"

	sst "github.com/pingcap/kvproto/pkg/import_sstpb"
	"github.com/pingcap/tidb/pkg/lightning/metric"
	"github.com/pingcap/tidb/pkg/util/promutil"
	"github.com/stretchr/testify/require"
)

type stubImportClientFactory struct {
	closed bool
}

func (f *stubImportClientFactory) create(context.Context, uint64) (sst.ImportSSTClient, error) {
	return nil, nil
}

func (f *stubImportClientFactory) close() {
	f.closed = true
}

func TestNewBackendForTestCloseNilSafe(t *testing.T) {
	ctx := context.Background()
	cfg := BackendConfig{LocalStoreDir: filepath.Join(t.TempDir(), "sort")}
	backend, err := NewBackendForTest(ctx, cfg, mockStoreHelper{})
	require.NoError(t, err)
	require.NotNil(t, backend.writeLimiter)

	stub := &stubImportClientFactory{}
	backend.importClientFactory = stub
	backend.Close()
	require.True(t, stub.closed)
}

func TestNewBackendForTestAssignMetrics(t *testing.T) {
	cfg := BackendConfig{LocalStoreDir: filepath.Join(t.TempDir(), "sort")}
	metrics := metric.NewMetrics(promutil.NewDefaultFactory())
	ctx := metric.WithCommonMetric(context.Background(), metrics.Common)
	backend, err := NewBackendForTest(ctx, cfg, mockStoreHelper{})
	require.NoError(t, err)
	require.NotNil(t, backend.metrics)
}
