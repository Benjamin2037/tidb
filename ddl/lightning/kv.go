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
	lkv "github.com/pingcap/tidb/br/pkg/lightning/backend/kv"
	"github.com/pingcap/tidb/br/pkg/lightning/common"
	"github.com/pingcap/tidb/kv"
)

type WorkerContext struct {
	WorkerId    int
	Ei          *engineInfo
	KVCache     []*WorkerKVCache
	CacheCount  int
}

func (wc *WorkerContext) init(id int, ei *engineInfo, cacheCount int) {
	wc.WorkerId = id
	wc.Ei = ei
	wc.KVCache = make([]*WorkerKVCache, cacheCount)
	wc.CacheCount = cacheCount

	i := 0
	for _, k := range wc.KVCache {
		k = NewWorkerKVCache()
		wc.KVCache[i] = k
		i++
	}
}

// woker is addIndexWorker. one worker one cache.
type WorkerKVCache struct {
	pairs    []common.KvPair
	buf      []byte
	last     int
	cap      int
	needGrow bool // need grow
	size     int
}

func NewWorkerKVCache() *WorkerKVCache {
	bs := make([]byte, flush_size)
	return &WorkerKVCache{
		pairs:    make([]common.KvPair, 0, flush_size/32),
		buf:      bs,
		last:     0,
		cap:      flush_size,
		needGrow: false,
	}
}

func (wc *WorkerKVCache) Reset() {
	wc.last = 0
	wc.pairs = wc.pairs[:0]
	if wc.needGrow {
		wc.cap = wc.cap * 2
		wc.buf = make([]byte, wc.cap)
		wc.needGrow = false
	}
}
func (wc *WorkerKVCache) Size() (count, size int) {
	return len(wc.pairs), wc.size
}

func (wc *WorkerKVCache) Fetch() []common.KvPair {
	return wc.pairs
}

func (wc *WorkerKVCache) PushKeyValue(k, v []byte, h kv.Handle) {
	p := common.KvPair{
		Key:   wc.cloneBytes(k),
		Val:   wc.cloneBytes(v),
		RowID: h.IntValue(),
	}
	wc.pairs = append(wc.pairs, p)
}

func (wc *WorkerKVCache) cloneBytes(v []byte) []byte {
	if false == wc.needGrow {
		ret := wc.pushBytes(v)
		if ret != nil {
			return ret
		}
		// not enough cache;
	}
	ret := make([]byte, len(v))
	copy(ret, v)
	return ret
}

func (wc *WorkerKVCache) pushBytes(v []byte) []byte {
	vl := len(v)
	s := wc.last
	wc.last += vl
	if wc.last > wc.cap {
		wc.needGrow = true
		return nil
	}
	ret := wc.buf[s:wc.last]
	copy(ret, v)
	return ret
}

func (wc *WorkerKVCache) toKvRows() lkv.Rows {
	return lkv.MakeRowsFromKvPairs(wc.pairs)
}
