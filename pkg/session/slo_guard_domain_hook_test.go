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

package session

import (
	"sync/atomic"
	"testing"

	"github.com/pingcap/tidb/pkg/domain"
	"github.com/stretchr/testify/require"
)

func TestInitDistTaskLoopStartsSLOGuardWorker(t *testing.T) {
	var called atomic.Bool
	origin := domain.StartImportIntoSLOGuardWorker
	domain.StartImportIntoSLOGuardWorker = func(_ *domain.Domain, _ func(func(), string), _ <-chan struct{}) {
		called.Store(true)
	}
	t.Cleanup(func() {
		domain.StartImportIntoSLOGuardWorker = origin
	})

	store, dom := CreateStoreAndBootstrap(t)
	t.Cleanup(func() {
		dom.Close()
		require.NoError(t, store.Close())
	})

	require.True(t, called.Load())
}
