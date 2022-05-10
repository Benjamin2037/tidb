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
package lightning

import (
	"sync/atomic"

	"github.com/pingcap/tidb/br/pkg/lightning/backend"
)

type EngineManager struct {
	engineCache map[string]*engineInfo
}

func (em *EngineManager) init() {
	em.engineCache = make(map[string]*engineInfo)
}

func (em *EngineManager) Put(key string, ei *engineInfo) {
	em.engineCache[key] = ei

}

func (em *EngineManager) GetEngineInfo(key string) (*engineInfo, error) {
	ei, ok := em.engineCache[key]
	if !ok {
		return nil, ErrNotFound
	}

	return ei, nil
}

func (em *EngineManager) ReleaseRef(key string) {
	ei := em.engineCache[key]
	if ei == nil {
		return
	}
	atomic.CompareAndSwapInt32(&ei.ref, 1, 0)
}

func (em *EngineManager) GetWriter(key string) (*backend.LocalEngineWriter, error) {
	ei, err := em.GetEngineInfo(key)
	if err != nil {
		return nil, err
	}
	return ei.getWriter()
}

func (em *EngineManager) ReleaseEngine(key string) {
	ei, exist := em.engineCache[key]
	if !exist {
		return
	}
	ei.isOpened = false
}
