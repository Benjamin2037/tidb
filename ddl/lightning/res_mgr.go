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
	"context"
	"errors"
	"strconv"
	"sync"
	"unsafe"

	"github.com/docker/go-units"
	"github.com/pingcap/tidb/br/pkg/lightning/backend"
	"github.com/pingcap/tidb/br/pkg/lightning/common"
	"github.com/pingcap/tidb/br/pkg/lightning/glue"
	"github.com/pingcap/tidb/br/pkg/lightning/log"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"go.uber.org/zap"
)

const (
	// Build type
	ALLOC_BACKEND_CONTEXT string = "AllocBackendContext"
	ALLOC_ENGINE_INFO     string = "AllocEngineInfo"
	ALLOC_WORKER_CONTEXT  string = "AllocWorkerCONTEXT"

	// Used to mark the object size did not stored in map
	firstAlloc            int64 = -1
	allocFailed           int64 = 0
)

// MemoryRoot is used to trace the memory usage of all light DDL environment.
type LightningMemoryRoot struct {
	maxLimit       int64
	currUsage      int64
	engineUsage    int64
	writeBuffer    int64
	backendCache   map[string]*BackendContext
	EngineMgr      EngineManager
	// This map is use to store all object memory allocated size.
	structSize     map[string]int64
	mLock          sync.Mutex
}

func (m *LightningMemoryRoot) init(maxMemUsage int64) {
	// Set lightning memory quota to 2 times flush_size
	if maxMemUsage < 2 * flush_size {
		m.maxLimit = 2 * flush_size
	} else {
	    m.maxLimit = maxMemUsage
	}

	m.currUsage = 0
	m.engineUsage = 0
	m.writeBuffer = 0

	m.backendCache = make(map[string]*BackendContext)
	m.EngineMgr.init()
	m.structSize = make(map[string]int64)
}

// Reset memory quota. if the 
func (m *LightningMemoryRoot) Reset(maxMemUsage int64) {
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
}

// trace mem usage, need to refine the implement.
func (m *LightningMemoryRoot) checkMemoryUsage(t string) (usage int64, err error) {
	var (
		requiredMem int64 = 0
		exist       bool = false
	)
	switch t {
	case ALLOC_BACKEND_CONTEXT:
		requiredMem, exist = m.structSize[ALLOC_BACKEND_CONTEXT]
	case ALLOC_ENGINE_INFO:
		requiredMem, exist = m.structSize[ALLOC_ENGINE_INFO]
	case ALLOC_WORKER_CONTEXT:
		requiredMem, exist = m.structSize[ALLOC_WORKER_CONTEXT]
	default:
		return allocFailed, errors.New(LERR_NO_MEM_TYPE)
	}

	if !exist {
		requiredMem = firstAlloc
		return requiredMem, nil
	}
	if m.currUsage + requiredMem > m.maxLimit {
		return allocFailed, errors.New(LERR_OUT_OF_MAX_MEM)
	}
	return requiredMem , err
}

// check and create one backend
func (m *LightningMemoryRoot) RegistBackendContext(ctx context.Context, unique bool, key string, sqlMode mysql.SQLMode) error {
	var (
		err        error = nil
		memRequire int64   = 0
		bc         *BackendContext
		bd         backend.Backend
		exist      bool = false
	)
	m.mLock.Lock()
	defer func() {
		m.mLock.Unlock()
	}()
	// First to check the memory usage
    m.totalMemoryConsume()
	memRequire, err = m.checkMemoryUsage(ALLOC_BACKEND_CONTEXT)
	if err != nil {
		log.L().Warn(LERR_ALLOC_MEM_FAILED, zap.String("backend key", key),
		    zap.String("Current Memory Usage:", strconv.FormatInt(m.currUsage,10)),
	        zap.String("Memory limitation:", strconv.FormatInt(m.maxLimit, 10)))
		return err
	}

	// Firstly, get backend Context from backend cache.
	bc, exist = m.backendCache[key]
	// If bc not exist, build one
	if exist == false {
		bc = new(BackendContext)
		if bc == nil {
			return errors.New(LERR_ALLOC_MEM_FAILED)
		}
		m.backendCache[key] = bc
		bc.Ctx = ctx
		db, err := DBFromConfig(ctx, bc.cfg.TiDB)
		if err != nil {
			return err
		}
		bc.tidbGlue = glue.NewExternalTiDBGlue(db, sqlMode)
		bc.cfg, err = generateLightningConfig(ctx, unique)
		adjustImportMemory(bc.cfg)
		if err != nil {
			log.L().Error(LERR_CREATE_BACKEND_FAILED, zap.String("backend key", key))
			return err
		}
		bd, err = createLocalBackend(ctx, bc.cfg, bc.tidbGlue)
		if err != nil {
			log.L().Error(LERR_CREATE_BACKEND_FAILED, zap.String("backend key", key))
			return err
		}
		bc.Backend = &bd
		// Init important variables
		bc.sysVars = ObtainImportantVariables(ctx, bc.tidbGlue, true)
	}
    
	if memRequire == firstAlloc {
		m.structSize[ALLOC_BACKEND_CONTEXT] = int64(unsafe.Sizeof(*bc))
	}
	// Count memory usage.
	m.currUsage += m.structSize[ALLOC_BACKEND_CONTEXT]
	log.L().Info(LINFO_CREATE_BACKEND, zap.String("backend key", key),
        zap.String("Current Memory Usage:", strconv.FormatInt(m.currUsage,10)),
	    zap.String("Memory limitation:", strconv.FormatInt(m.maxLimit, 10)))
	return err
}

// Uniform entry to close backend and release related memory allocated
func (m *LightningMemoryRoot) DeleteBackendContext(bcKey string) {
	// Only acquire/release lock here.
	m.mLock.Lock()
	defer func() {
		m.mLock.Unlock()
	}()
	// Close key specific backend 
	bc, exist := m.backendCache[bcKey]
	if !exist {
		log.L().Error(LERR_GET_BACKEND_FAILED, zap.String("backend key", bcKey))
		return
	}

	// Delete all workerContext and engineInfo registed under one backend
	m.deleteBcEngine(bcKey)

	// Close and delete backend by key
	bc.Backend.Close()
	m.currUsage -= m.structSize[bc.Key]
	m.currUsage -= m.structSize[ALLOC_BACKEND_CONTEXT]
	log.L().Info(LINFO_CLOSE_BACKEND, zap.String("backend key", bcKey),
	    zap.String("Current Memory Usage:", strconv.FormatInt(m.currUsage,10)),
	    zap.String("Memory limitation:", strconv.FormatInt(m.maxLimit, 10)))
	return
}

// Check and allocate one EngineInfo
func (m *LightningMemoryRoot) RegistEngineInfo(job *model.Job, bcKey string, engineKey string, indexId int32, workerCount int) (int, error) {
	var (
		err           error = nil
	    memRequire    int64 = 0
	    ei            *engineInfo
    )
	m.mLock.Lock()
	defer func() {
		m.mLock.Unlock()
	}()
	// Firstly, update and check the memory usage
    m.totalMemoryConsume()
	memRequire, err = m.checkMemoryUsage(ALLOC_ENGINE_INFO)
	if err != nil {
		log.L().Warn(LERR_ALLOC_MEM_FAILED, zap.String("backend key", bcKey),
		    zap.String("Engine key", engineKey),
		    zap.String("Current Memory Usage:", strconv.FormatInt(m.currUsage,10)),
	        zap.String("Memory limitation:", strconv.FormatInt(m.maxLimit, 10)))
		return 0, err
	}

	bc := m.backendCache[bcKey]
	// Caculate lightning concurrecy degree and set memory usage.
	// and pre-allocate memory usage for worker 
    workerCount = m.CaculateConcurrentDegree(workerCount)
	// When return workerCount is 0, means there is no memory available for lightning worker.
	if (workerCount == int(allocFailed)) {
		return 0, errors.New(LERR_CREATE_ENGINE_FAILED)
	}

	// Create one slice for one backend on one stmt, current we share one engine
	err = CreateEngine(bc.Ctx, job, bcKey, engineKey, indexId)
	if err != nil {
		return 0, errors.New(LERR_CREATE_ENGINE_FAILED)
	}

	if memRequire == firstAlloc {
		m.structSize[ALLOC_ENGINE_INFO] = int64(unsafe.Sizeof(*ei))
	}
	// Count memory usage.
	m.currUsage += m.structSize[ALLOC_ENGINE_INFO]
	m.engineUsage += m.structSize[ALLOC_ENGINE_INFO]
	log.L().Info(LINFO_OPEN_ENGINE, zap.String("backend key", bcKey),
	zap.String("Engine key", engineKey),
	zap.String("Current Memory Usage:", strconv.FormatInt(m.currUsage,10)),
	zap.String("Memory limitation:", strconv.FormatInt(m.maxLimit, 10)))
	return  workerCount, nil
}

// Create one
func (m *LightningMemoryRoot) RegistWorkerContext(engineInfoKey string, id int) (*WorkerContext, error) {
	var (
		err           error = nil
	    memRequire    int64 = 0
		wCtx          *WorkerContext
	)
	m.mLock.Lock()
	defer func() {
		m.mLock.Unlock()
	}()
	// First to check the memory usage
    m.totalMemoryConsume()
	memRequire, err = m.checkMemoryUsage(ALLOC_WORKER_CONTEXT)
	if err != nil {
		log.L().Error(LERR_ALLOC_MEM_FAILED, zap.String("Engine key", engineInfoKey),
		zap.String("worer Id:", strconv.Itoa(id)),
		zap.String("Memory allocate:", strconv.FormatInt(memRequire, 10)),
		zap.String("Current Memory Usage:", strconv.FormatInt(m.currUsage,10)),
		zap.String("Memory limitation:", strconv.FormatInt(m.maxLimit, 10)))
		return nil, err
	}

	wCtx = new(WorkerContext)
	err = wCtx.InitWorkerContext(engineInfoKey, id)
	if err != nil {
		log.L().Error(LERR_CREATE_CONTEX_FAILED, zap.String("Engine key", engineInfoKey),
		zap.String("worer Id:", strconv.Itoa(id)),
		zap.String("Memory allocate:", strconv.FormatInt(memRequire, 10)),
		zap.String("Current Memory Usage:", strconv.FormatInt(m.currUsage,10)),
		zap.String("Memory limitation:", strconv.FormatInt(m.maxLimit, 10)))
		return nil, err
	}

	if memRequire == firstAlloc {
		m.structSize[ALLOC_ENGINE_INFO] = int64(unsafe.Sizeof(*wCtx))
	}
	// Count memory usage.
	m.currUsage += m.structSize[ALLOC_WORKER_CONTEXT]
	log.L().Info(LINFO_CREATE_WRITER, zap.String("Engine key", engineInfoKey),
	zap.String("worer Id:", strconv.Itoa(id)),
	zap.String("Memory allocate:", strconv.FormatInt(memRequire, 10)),
	zap.String("Current Memory Usage:", strconv.FormatInt(m.currUsage,10)),
	zap.String("Memory limitation:", strconv.FormatInt(m.maxLimit, 10)))
	return wCtx, err
}

// Uniform entry to release KVCache slice related memory allocated
func (m *LightningMemoryRoot) deleteBcEngine(bcKey string) error {
	var err    error = nil
	var count  int = 0
	bc, exist := m.getBackendContext(bcKey)
	if !exist {
		log.L().Error(LERR_GET_BACKEND_FAILED, zap.String("backend key", bcKey))
		return err
	}
	count = 0
	// Delete EngienInfo registed in m.engineManager.engineCache
	for _, ei := range bc.EngineCache {
       eiKey := ei.key
	   delete(m.EngineMgr.engineCache, eiKey)
	   count++
	}

	bc.EngineCache = make(map[string]*engineInfo)
	m.currUsage -= m.structSize[ALLOC_ENGINE_INFO] * int64(count)
	m.engineUsage -= m.structSize[ALLOC_ENGINE_INFO] * int64(count)
	log.L().Info(LINFO_CLOSE_BACKEND, zap.String("backend key", bcKey),
	    zap.String("Current Memory Usage:", strconv.FormatInt(m.currUsage,10)),
	    zap.String("Memory limitation:", strconv.FormatInt(m.maxLimit, 10)))
	return err
}

func (m *LightningMemoryRoot) getBackendContext(bcKey string) (*BackendContext, bool) {
	bc, exist := m.backendCache[bcKey]
	if !exist {
		log.L().Warn(LWAR_BACKEND_NOT_EXIST, zap.String("backend key:", bcKey))
		return nil, false
	}
	return bc, exist
}

func (m *LightningMemoryRoot) totalMemoryConsume() {
	var memSize int64 = 0
    for _, bc := range m.backendCache {
		memSize += bc.Backend.TotalMemoryConsume()
		m.structSize[bc.Key] = memSize
	}

	m.currUsage += memSize
	return
}

func (m *LightningMemoryRoot) CaculateConcurrentDegree(workerCnt int) int {
    var kvp common.KvPair
	size := unsafe.Sizeof(kvp)
	// If only one worker's memory init requirement still bigger than mem limitation.
	if int64(size * units.MiB) + m.currUsage > m.maxLimit {
      return int(allocFailed)
	}

	if workerCnt == 1 {
		return workerCnt
	}

	for int64(size * units.MiB * uintptr(workerCnt)) + m.currUsage > m.maxLimit || workerCnt == 1 {
		workerCnt /= 2
	}

	m.currUsage += int64(size * units.MiB * uintptr(workerCnt))
	return workerCnt
} 