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
	"sync"

	"github.com/pingcap/tidb/pkg/executor/importer"
	"github.com/pingcap/tidb/pkg/lightning/config"
)

type sloGuardState int32

const (
	sloGuardStateNormal sloGuardState = iota
	sloGuardStateSlow
	sloGuardStatePause
)

type sloGuardRuntime struct {
	state              sloGuardState
	slowApplyRateLimit config.ByteSize
}

type sloGuardRuntimeStore struct {
	mu   sync.RWMutex
	jobs map[int64]sloGuardRuntime
}

var sloGuardRuntimes = sloGuardRuntimeStore{
	jobs: make(map[int64]sloGuardRuntime),
}

func setSLOGuardRuntime(jobID int64, state sloGuardState, slowApplyRateLimit config.ByteSize) {
	sloGuardRuntimes.mu.Lock()
	defer sloGuardRuntimes.mu.Unlock()
	sloGuardRuntimes.jobs[jobID] = sloGuardRuntime{
		state:              state,
		slowApplyRateLimit: slowApplyRateLimit,
	}
}

func clearSLOGuardRuntime(active map[int64]struct{}) {
	sloGuardRuntimes.mu.Lock()
	defer sloGuardRuntimes.mu.Unlock()
	for jobID := range sloGuardRuntimes.jobs {
		if jobID == importer.SLOGuardGlobalJobID {
			continue
		}
		if _, ok := active[jobID]; !ok {
			delete(sloGuardRuntimes.jobs, jobID)
		}
	}
}

func getSLOGuardRuntime(jobID int64) sloGuardRuntime {
	sloGuardRuntimes.mu.RLock()
	defer sloGuardRuntimes.mu.RUnlock()
	if runtime, ok := sloGuardRuntimes.jobs[jobID]; ok {
		return runtime
	}
	if runtime, ok := sloGuardRuntimes.jobs[importer.SLOGuardGlobalJobID]; ok {
		return runtime
	}
	return sloGuardRuntime{}
}

func getSLOGuardState(jobID int64) sloGuardState {
	return getSLOGuardRuntime(jobID).state
}

func isSLOGuardPaused() bool {
	return getSLOGuardState(importer.SLOGuardGlobalJobID) == sloGuardStatePause
}

func effectiveApplyRateLimit(jobID int64, baseLimit config.ByteSize) config.ByteSize {
	runtime := getSLOGuardRuntime(jobID)
	if runtime.state != sloGuardStateSlow && runtime.state != sloGuardStatePause {
		return baseLimit
	}
	if runtime.slowApplyRateLimit <= 0 {
		return baseLimit
	}
	if baseLimit <= 0 || runtime.slowApplyRateLimit < baseLimit {
		return runtime.slowApplyRateLimit
	}
	return baseLimit
}
