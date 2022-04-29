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
	delete(em.engineCache, key)
}
