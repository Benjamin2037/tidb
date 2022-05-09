package lightning

import (
	"context"
	"errors"
	"strconv"
	"sync/atomic"

	"github.com/pingcap/tidb/br/pkg/lightning/backend"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/parser/model"
)

type BuildType int

const (
	// Build type
	BUILD_BACKEND_CONTEXT BuildType = 1
	BUILD_ENGINE_INFO     BuildType = 2
	BUILD_WORKER_CONTEXT  BuildType = 3
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
}

func (m *LightningMemoryRoot) init(maxMemUsage uint64) {
	m.maxLimit = maxMemUsage
	atomic.StoreUint64(&m.currUsage, 0)
	atomic.StoreUint64(&m.engineUsage, 0)
	atomic.StoreUint64(&m.writeBuffer, 0)

	m.backendCache.init()
	m.engineManager.init()
	m.workerCTXCache.init()
}

func (m *LightningMemoryRoot) Reset(max uint64) {
	m.maxLimit = max
	atomic.StoreUint64(&m.currUsage, 0)
}

// trace mem usage, need to refine the implement.
func (m *LightningMemoryRoot) checkMemoryUsage(t BuildType) (usage int, err error) {
	switch t {
	case BUILD_BACKEND_CONTEXT:
		requiredMem := 5
		if atomic.AddUint64(&m.currUsage, uint64(requiredMem)) > m.maxLimit {
			return 0, errors.New(LERR_OUT_OF_MAX_MEM)
		}
	case BUILD_ENGINE_INFO:
		requiredMem := 10
		if atomic.AddUint64(&m.currUsage, uint64(requiredMem)) > m.maxLimit {
			return 0, errors.New(LERR_OUT_OF_MAX_MEM)
		}
	case BUILD_WORKER_CONTEXT:
		requiredMem := 8 * _mb
		if atomic.AddUint64(&m.currUsage, uint64(requiredMem)) > m.maxLimit {
			return 0, errors.New(LERR_OUT_OF_MAX_MEM)
		}
	default:
		return 0, errors.New(LERR_NO_MEM_TYPE)
	}
	return 0, err
}

// check and create one backend
func (m *LightningMemoryRoot) RegistBackendContext(ctx context.Context, unique bool, key string) error {
	var (
		err        error = nil
		memRequire int   = 0
		bc         *BackendContext
		bd         backend.Backend
	)

	// First to check the m
	memRequire, err = m.checkMemoryUsage(BUILD_BACKEND_CONTEXT)

	if memRequire == 0 || err != nil {
		return errors.New(LERR_OUT_OF_MAX_MEM)
	}

	// Todo Create backend
	bc = m.backendCache.bcCache[key]
	// if bc not exist, build one
	if bc == nil {
		bc = new(BackendContext)
		if bc == nil {
			return errors.New(LERR_ALLOC_MEM_FAILED)
		}
		m.backendCache.bcCache[key] = bc
		bc.Ctx = ctx
		bd, err = createLocalBackend(ctx, unique)
		bc.Backend = &bd
	}
	return err
}

// Uniform entry to close backend and release related memory allocated
func (m *LightningMemoryRoot) DeleteBackendContext(bcKey string, deleteAll bool) error {
	var err error = nil
	// Close key specif
	bc := GlobalLightningEnv.LitMemRoot.backendCache.bcCache[bcKey]
	// If deletAll equal to true that means some error occurs in prepare lightning env period.
	// the worker context and engine that were built successed before should also be delete or closed.
	if deleteAll {

	}

	// close and delete backend by key
	bc.Backend.Close()
	return err
}

// check and allocate one slice engineCache
func (m *LightningMemoryRoot) RegistEngineInfo(job *model.Job, t *meta.Meta, bcKey string, engineKey string, tbl *model.TableInfo) (*engineInfo, error) {
	var err error = nil
	var memRequire int = 0
	var ei *engineInfo

	// First to check the m
	memRequire, err = m.checkMemoryUsage(BUILD_ENGINE_INFO)

	if memRequire == 0 || err != nil {
		return nil, err
	}

	bc := m.backendCache.bcCache[bcKey]

	// Create one slice for one backend on one stmt, current we share one engine
	err = CreateEngine(bc.Ctx, job, t, bcKey, engineKey, tbl)
	ei = bc.EngineCache[engineKey]

	return ei, err
}

// check and allocate one slice engineCache
func (m *LightningMemoryRoot) RegistWorkerContext(workerId int, keyEngineInfo string, kvcount int) (*WorkerContext, error) {
	var err error = nil
	var memRequire int = 0
	var kvctx *WorkerContext
	var workerCtxKey string

	// First to check the m
	memRequire, err = m.checkMemoryUsage(BUILD_WORKER_CONTEXT)

	if memRequire == 0 || err != nil {
		return nil, err
	}

	// Generate key of worker context, that is engineinfoId + workerId
	workerCtxKey = keyEngineInfo + strconv.Itoa(int(workerId))

	ei := m.engineManager.engineCache[keyEngineInfo]
	// Create backend
	kvctx = new(WorkerContext)
	// Current only allocate one kv cache for lightning worker
	kvctx.init(workerId, ei, 1)
	m.workerCTXCache.workerCtxCache[workerCtxKey] = kvctx

	return kvctx, err
}

// Uniform entry to release KVCache slice related memory allocated
func (m *LightningMemoryRoot) ReleaseKVCache(key string) error {
	var err error = nil
	return err
}
