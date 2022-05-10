package lightning

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/pingcap/tidb/br/pkg/lightning/backend"
	"github.com/pingcap/tidb/br/pkg/lightning/log"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/parser/model"
	"go.uber.org/zap"
)

type BuildType int

const (
	// Build type
	ALLOC_BACKEND_CONTEXT BuildType = 1
	ALLOC_ENGINE_INFO     BuildType = 2
	ALLOC_WORKER_CONTEXT  BuildType = 3
	ALLOC_KV_PAIRS_CACHE  BuildType = 4

	// Used to mark the object size did not stored in map
	firstAlloc            int = -1
	allocFailed           int = 0
)

// MemoryRoot is used to trace the memory usage of all light DDL environment.
type LightningMemoryRoot struct {
	maxLimit       uint64
	currUsage      uint64
	engineUsage    uint64
	writeBuffer    uint64
	backendCache   BackendCache
	engineManager  EngineManager
	workerCTXCache WorkerContextCache
	// This map is use to store all object memory allocated size.
	structSize     map[BuildType]int
	mLock          sync.Mutex
}

func (m *LightningMemoryRoot) init(maxMemUsage uint64) {
	// Set lightning memory quota to 2 times flush_size
	if maxMemUsage < 2 * flush_size {
		m.maxLimit = 2 * flush_size
	} else {
	    m.maxLimit = maxMemUsage
	}

	atomic.StoreUint64(&m.currUsage, 0)
	atomic.StoreUint64(&m.engineUsage, 0)
	atomic.StoreUint64(&m.writeBuffer, 0)

	m.backendCache.init()
	m.engineManager.init()
	m.workerCTXCache.init()
	m.structSize = make(map[BuildType]int)
}

func (m *LightningMemoryRoot) Reset(maxMemUsage uint64) {
	m.mLock.Lock()
	defer func() {
		m.mLock.Unlock()
	}()
	// Set lightning memory quota to 2 times flush_size
	if maxMemUsage < 2 * flush_size {
	    m.maxLimit = 2 * flush_size
	} else {
		m.maxLimit = maxMemUsage
	}
	m.currUsage = 0
}

// trace mem usage, need to refine the implement.
func (m *LightningMemoryRoot) checkMemoryUsage(t BuildType, count int) (usage int, err error) {
	var (
		requiredMem int = 0
		exist       bool = false
	)
	switch t {
	case ALLOC_BACKEND_CONTEXT:
		requiredMem, exist = m.structSize[ALLOC_BACKEND_CONTEXT]
	case ALLOC_ENGINE_INFO:
		requiredMem, exist = m.structSize[ALLOC_ENGINE_INFO]
	case ALLOC_WORKER_CONTEXT:
		requiredMem, exist = m.structSize[ALLOC_WORKER_CONTEXT]
	case ALLOC_KV_PAIRS_CACHE:
		requiredMem, exist = m.structSize[ALLOC_KV_PAIRS_CACHE]
		requiredMem = requiredMem * count
	default:
		return allocFailed, errors.New(LERR_NO_MEM_TYPE)
	}

	if !exist {
		requiredMem = firstAlloc
		return requiredMem, nil
	}
	if m.currUsage + uint64(requiredMem) > m.maxLimit {
		return allocFailed, errors.New(LERR_OUT_OF_MAX_MEM)
	}
	return requiredMem , err
}


// check and create one backend
func (m *LightningMemoryRoot) RegistBackendContext(ctx context.Context, unique bool, key string) error {
	var (
		err        error = nil
		memRequire int   = 0
		bc         *BackendContext
		bd         backend.Backend
		exist      bool = false
	)
	m.mLock.Lock()
	defer func() {
		m.mLock.Unlock()
	}()
	// First to check the m
	memRequire, err = m.checkMemoryUsage(ALLOC_BACKEND_CONTEXT, 1)
	if err != nil {
		return err
	}

	// Firstly, get backend Context from backend cache.
	bc, exist = m.backendCache.bcCache[key]
	// if bc not exist, build one
	if exist == false {
		bc = new(BackendContext)
		if bc == nil {
			return errors.New(LERR_ALLOC_MEM_FAILED)
		}
		m.backendCache.bcCache[key] = bc
		bc.Ctx = ctx
		bd, err = createLocalBackend(ctx, unique)
		bc.Backend = &bd
	}

	if memRequire == firstAlloc {
		m.structSize[ALLOC_BACKEND_CONTEXT] = int(unsafe.Sizeof(bc))
	}
	// Count memory usage.
	m.currUsage += uint64(m.structSize[ALLOC_BACKEND_CONTEXT])
	return err
}

// Uniform entry to close backend and release related memory allocated
func (m *LightningMemoryRoot) DeleteBackendContext(bcKey string) {
	// Only acquire/release lock here.
	m.mLock.Lock()
	defer func() {
		m.mLock.Unlock()
	}()
	// Close key specif
	bc, exist := m.backendCache.bcCache[bcKey]
	if !exist {
		return
	}

	// Delete all workerContext and engineInfo registed under one backend
	m.deleteBcWorkerCtx(bcKey)
	m.deleteBcEngine(bcKey)

	// Close and delete backend by key
	bc.Backend.Close()
	m.currUsage -= uint64(m.structSize[ALLOC_BACKEND_CONTEXT])
	return
}

// Check and allocate one EngineInfo
func (m *LightningMemoryRoot) RegistEngineInfo(job *model.Job, t *meta.Meta, bcKey string, engineKey string, tbl *model.TableInfo) (error) {
	var (
		err           error = nil
	    memRequire    int = 0
	    ei            *engineInfo
    )
	m.mLock.Lock()
	defer func() {
		m.mLock.Unlock()
	}()
	// First to check current memory usage.
	memRequire, err = m.checkMemoryUsage(ALLOC_ENGINE_INFO, 1)
	if err != nil {
		return err
	}

	bc := m.backendCache.bcCache[bcKey]

	// Create one slice for one backend on one stmt, current we share one engine
	err = CreateEngine(bc.Ctx, job, t, bcKey, engineKey, tbl)
	ei = bc.EngineCache[engineKey]

	if memRequire == firstAlloc {
		m.structSize[ALLOC_ENGINE_INFO] = int(unsafe.Sizeof(ei))
	}
	// Count memory usage.
	m.currUsage += uint64(m.structSize[ALLOC_ENGINE_INFO])
	return  err
}

// Check and allocate one slice engineCache
func (m *LightningMemoryRoot) RegistWorkerContext(workerId int, keyEngineInfo string, kvcount int) (*WorkerContext, error) {
	var (
		err             error = nil
	    memRequire      int = 0
	    memRequireKV    int = 0
	    kvctx           *WorkerContext
	    workerCtxKey    string
		isFirstAlloc    bool = false
	)
	m.mLock.Lock()
	defer func() {
		m.mLock.Unlock()
	}()
	// First to check the current memory usage
	memRequire, err = m.checkMemoryUsage(ALLOC_WORKER_CONTEXT, 1)
	memRequireKV, err = m.checkMemoryUsage(ALLOC_KV_PAIRS_CACHE, kvcount)
	if err != nil {
		return nil, err
	}

	if memRequire == firstAlloc || memRequireKV == firstAlloc {
		isFirstAlloc = true
	}

	// Generate key of worker context, that is engineinfoId + workerId
	workerCtxKey = keyEngineInfo + strconv.Itoa(int(workerId))

	ei := m.engineManager.engineCache[keyEngineInfo]
	// Create backend
	kvctx = new(WorkerContext)
	// Current only allocate one kv cache for lightning worker
	kvctx.init(workerId, ei, 1)
	m.workerCTXCache.workerCtxCache[workerCtxKey] = kvctx

	if isFirstAlloc {
		m.structSize[ALLOC_WORKER_CONTEXT] = int(unsafe.Sizeof(kvctx))
		m.structSize[ALLOC_KV_PAIRS_CACHE] = int(unsafe.Sizeof(kvctx.KVCache[0]))
	}
	// Count memory usage.
	m.currUsage += uint64(memRequire + memRequireKV)
	return kvctx, err
}

// Uniform entry to release KVCache slice related memory allocated
func (m *LightningMemoryRoot) deleteBcEngine(bcKey string) error {
	var err    error = nil
	var count  int = 0
	bc, exist := m.getBackendContext(bcKey)
	if !exist {
		return err
	}
	count = 0
	// Delete EngienInfo registed in m.engineManager.engineCache
	for _, ei := range bc.EngineCache {
       eiKey := ei.key
	   delete(m.engineManager.engineCache, eiKey)
	   count++
	}

	bc.EngineCache = make(map[string]*engineInfo)
	m.currUsage -= uint64(m.structSize[ALLOC_ENGINE_INFO] * count)
	return err
}

// Uniform entry to release KVCache slice related memory allocated
func (m *LightningMemoryRoot) deleteBcWorkerCtx(bcKey string) error {
	var err error = nil
	var count  int = 0
	var wCtxKey string
	var memSize int = 0
	bc, exist := m.getBackendContext(bcKey)
	if !exist {
		return err
	}
	count = 0
	// Delete worker context registed in m.workerCTXCache.workerCtxCache
	for _, wCtx := range bc.WCtx {
		wCtxKey = wCtx.Ei.key + strconv.Itoa(wCtx.WorkerId)
		memSize += wCtx.CacheCount * m.structSize[ALLOC_KV_PAIRS_CACHE]
		for _, kvPairCache := range wCtx.KVCache {
			kvPairCache.Reset()
		}
		delete(m.workerCTXCache.workerCtxCache, wCtxKey)
		count++
	}

	memSize += count * m.structSize[ALLOC_WORKER_CONTEXT]
    bc.WCtx = make(map[string]*WorkerContext)
	m.currUsage -= uint64(memSize)
	return err
}

func (m *LightningMemoryRoot) getBackendContext(bcKey string) (*BackendContext, bool) {
	bc, exist := m.backendCache.bcCache[bcKey]
	if !exist {
		log.L().Warn(LWAR_BACKEND_NOT_EXIST, zap.String("backend key:", bcKey))
		return nil, false
	}
	return bc, exist
}