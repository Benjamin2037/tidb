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

	"github.com/pingcap/tidb/br/pkg/lightning/backend"
	"github.com/pingcap/tidb/br/pkg/lightning/common"
	"github.com/pingcap/tidb/br/pkg/lightning/config"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

type defaultType string

// Default struct need to be count.
const (
	AllocBackendContext defaultType = "AllocBackendContext"
	AllocEngineInfo     defaultType = "AllocEngineInfo"
	AllocWorkerContext  defaultType = "AllocWorkerCONTEXT"

	// Used to mark the object size did not store in map
	allocFailed int64 = 0
)

// MemoryRoot traces the memory usage of all light DDL environment.
type MemoryRoot struct {
	maxLimit     int64
	currUsage    int64
	engineUsage  int64
	writeBuffer  int64
	backendCache map[string]*BackendContext
	EngineMgr    EngineManager
	// This map is used to store all object memory allocated size.
	structSize map[string]int64
	mLock      sync.Mutex
}

func (m *MemoryRoot) init(maxMemUsage int64) {
	// Set lightning memory quota to 2 times flushSize
	if maxMemUsage < flushSize {
		m.maxLimit = flushSize
	} else {
		m.maxLimit = maxMemUsage
	}

	m.currUsage = 0
	m.engineUsage = 0
	m.writeBuffer = 0

	m.backendCache = make(map[string]*BackendContext, 10)
	m.EngineMgr.init()
	m.structSize = make(map[string]int64, 10)
	m.initDefaultStructMemSize()
}

// init calculates the memory struct size and save it into map.
func (m *MemoryRoot) initDefaultStructMemSize() {
	var (
		bc   BackendContext
		ei   engineInfo
		wCtx WorkerContext
	)

	m.structSize[string(AllocBackendContext)] = int64(unsafe.Sizeof(bc))
	m.structSize[string(AllocEngineInfo)] = int64(unsafe.Sizeof(ei))
	m.structSize[string(AllocWorkerContext)] = int64(unsafe.Sizeof(wCtx))
}

// Reset memory quota. but not less than flushSize(8 MB)
func (m *MemoryRoot) Reset(maxMemUsage int64) {
	m.mLock.Lock()
	defer func() {
		m.mLock.Unlock()
	}()
	// Set lightning memory quota to flushSize
	if maxMemUsage < flushSize {
		m.maxLimit = flushSize
	} else {
		m.maxLimit = maxMemUsage
	}
}

// checkMemoryUsage check if there is enough memory to allocate struct for lighting execution.
func (m *MemoryRoot) checkMemoryUsage(t defaultType) error {
	var requiredMem int64
	switch t {
	case AllocBackendContext:
		requiredMem = m.structSize[string(AllocBackendContext)]
	case AllocEngineInfo:
		requiredMem = m.structSize[string(AllocEngineInfo)]
	case AllocWorkerContext:
		requiredMem = m.structSize[string(AllocWorkerContext)]
	default:
		return errors.New(LitErrUnknownMemType)
	}

	if m.currUsage+requiredMem > m.maxLimit {
		return errors.New(LitErrOutMaxMem)
	}
	return nil
}

// RegisterBackendContext checks if exist backend or will create one backend.
func (m *MemoryRoot) RegisterBackendContext(ctx context.Context, unique bool, key string, sqlMode mysql.SQLMode) error {
	var (
		err   error
		bd    backend.Backend
		exist bool
		cfg   *config.Config
	)
	m.mLock.Lock()
	defer func() {
		m.mLock.Unlock()
	}()
	// Firstly, get backend context from backend cache.
	_, exist = m.backendCache[key]
	// If bc not exist, build a new backend for reorg task, otherwise reuse exist backend
	// to continue the task.
	if !exist {
		// Firstly, update real time memory usage, check if memory is enough.
		m.updateTotalMemoryConsumption()
		err = m.checkMemoryUsage(AllocBackendContext)
		if err != nil {
			logutil.BgLogger().Warn(LitErrAllocMemFail, zap.String("backend key", key),
				zap.String("Current Memory Usage:", strconv.FormatInt(m.currUsage, 10)),
				zap.String("Memory limitation:", strconv.FormatInt(m.maxLimit, 10)))
			return err
		}
		cfg, err = generateLightningConfig(key, unique)
		if err != nil {
			logutil.BgLogger().Warn(LitErrAllocMemFail, zap.String("backend key", key),
				zap.String("Generate config for lightning error:", err.Error()))
			return err
		}
		glue := glueLit{}
		bd, err = createLocalBackend(ctx, cfg, glue)
		if err != nil {
			logutil.BgLogger().Error(LitErrCreateBackendFail, zap.String("backend key", key),
				zap.String("Error", err.Error()), zap.Stack("stack trace"))
			return err
		}

		// Init important variables
		sysVars := obtainImportantVariables()

		m.backendCache[key] = newBackendContext(ctx, key, &bd, cfg, sysVars)

		// Count memory usage.
		m.currUsage += m.structSize[string(AllocBackendContext)]
		logutil.BgLogger().Info(LitInfoCreateBackend, zap.String("backend key", key),
			zap.String("Current Memory Usage:", strconv.FormatInt(m.currUsage, 10)),
			zap.String("Memory limitation:", strconv.FormatInt(m.maxLimit, 10)),
			zap.String("Unique Index:", strconv.FormatBool(unique)))
	}
	return err
}

// DeleteBackendContext uniform entry to close backend and release related memory allocated
func (m *MemoryRoot) DeleteBackendContext(bcKey string) {
	// Only acquire/release lock here.
	m.mLock.Lock()
	defer func() {
		delete(m.backendCache, bcKey)
		m.mLock.Unlock()
	}()
	// Close backend logic
	bc, exist := m.backendCache[bcKey]
	if !exist {
		logutil.BgLogger().Error(LitErrGetBackendFail, zap.String("backend key", bcKey))
		return
	}

	// Close and delete backend by key
	_ = m.DeleteBackendEngines(bcKey)
	bc.Backend.Close()

	// Reclaim memory.
	m.currUsage -= m.structSize[bc.Key]
	delete(m.structSize, bcKey)
	m.currUsage -= m.structSize[string(AllocBackendContext)]
	if m.currUsage < 0 {
		m.currUsage = 0
	}
	logutil.BgLogger().Info(LitInfoCloseBackend, zap.String("backend key", bcKey),
		zap.String("Current Memory Usage:", strconv.FormatInt(m.currUsage, 10)),
		zap.String("Memory limitation:", strconv.FormatInt(m.maxLimit, 10)))
}

// ClearEngines in exception case, clear intermediate files that lightning engine generated for index.
func (m *MemoryRoot) ClearEngines(jobID int64, indexIDs ...int64) {
	for _, indexID := range indexIDs {
		eiKey := GenEngineInfoKey(jobID, indexID)
		ei, exist := m.EngineMgr.enginePool[eiKey]
		if exist {
			indexEngine := ei.openedEngine
			closedEngine, err := indexEngine.Close(ei.backCtx.Ctx, ei.cfg)
			if err != nil {
				logutil.BgLogger().Error(LitErrCloseEngineErr, zap.String("Engine key", eiKey))
			}
			// Here the local intermediate file will be removed.
			err = closedEngine.Cleanup(ei.backCtx.Ctx)
			if err != nil {
				logutil.BgLogger().Error(LitErrCleanEngineErr, zap.String("Engine key", eiKey))
			}
		}
	}
}

// RegisterEngineInfo check and allocate one EngineInfo, delete engineInfo are packed into close backend flow
// The worker count means at this time the engine need pre-check memory for workers usage.
func (m *MemoryRoot) RegisterEngineInfo(job *model.Job, bcKey string, engineKey string, indexID int64, workerCount int) (int, error) {
	var err error = nil
	m.mLock.Lock()
	defer func() {
		m.mLock.Unlock()
	}()
	bc, exist := m.backendCache[bcKey]
	if !exist {
		logutil.BgLogger().Warn(LitWarnBackendNOTExist, zap.String("Backend key", bcKey))
		return 0, err
	}

	// Calculate lightning concurrency degree and set memory usage.
	// and pre-allocate memory usage for worker
	newWorkerCount := m.workerDegree(workerCount, engineKey, bcKey)
	en, exist1 := bc.EngineCache[engineKey]
	if !exist1 {
		// When return workerCount is 0, means there is no memory available for lightning worker.
		if workerCount == int(allocFailed) {
			logutil.BgLogger().Warn(LitErrAllocMemFail, zap.String("Backend key", bcKey),
				zap.String("Engine key", engineKey),
				zap.String("Expected worker count:", strconv.Itoa(workerCount)),
				zap.String("Currnt alloc wroker count:", strconv.Itoa(newWorkerCount)))
			return 0, errors.New(LitErrCleanEngineErr)
		}
		// Firstly, update and check the current memory usage
		m.updateTotalMemoryConsumption()
		err = m.checkMemoryUsage(AllocEngineInfo)
		if err != nil {
			logutil.BgLogger().Warn(LitErrAllocMemFail, zap.String("Backend key", bcKey),
				zap.String("Engine key", engineKey),
				zap.String("Current Memory Usage:", strconv.FormatInt(m.currUsage, 10)),
				zap.String("Memory limitation:", strconv.FormatInt(m.maxLimit, 10)))
			return 0, err
		}
		// Create one slice for one backend on one stmt, current we share one engine
		err = CreateEngine(bc.Ctx, job, bcKey, engineKey, indexID, workerCount)
		if err != nil {
			return 0, errors.New(LitErrCreateEngineFail)
		}

		// Count memory usage.
		m.currUsage += m.structSize[string(AllocEngineInfo)]
		m.engineUsage += m.structSize[string(AllocEngineInfo)]
	} else {
		// If engine exist, then add newWorkerCount.
		en.WriterCount += newWorkerCount
	}
	logutil.BgLogger().Info(LitInfoOpenEngine, zap.String("backend key", bcKey),
		zap.String("Engine key", engineKey),
		zap.String("Current Memory Usage:", strconv.FormatInt(m.currUsage, 10)),
		zap.String("Memory limitation:", strconv.FormatInt(m.maxLimit, 10)),
		zap.String("Expected Worker Count", strconv.Itoa(workerCount)),
		zap.String("Allocated worker count", strconv.Itoa(newWorkerCount)))
	return newWorkerCount, nil
}

// RegisterWorkerContext create one lightning local writer context for one backfill worker.
// It will be clean within close backend process.
func (m *MemoryRoot) RegisterWorkerContext(engineInfoKey string, id int) (*WorkerContext, error) {
	var (
		err        error
		wCtx       *WorkerContext
		memRequire int64 = m.structSize[string(AllocWorkerContext)]
	)
	m.mLock.Lock()
	defer func() {
		m.mLock.Unlock()
	}()
	// First to check the memory usage
	m.updateTotalMemoryConsumption()
	err = m.checkMemoryUsage(AllocWorkerContext)
	if err != nil {
		logutil.BgLogger().Error(LitErrAllocMemFail, zap.String("Engine key", engineInfoKey),
			zap.String("worker Id:", strconv.Itoa(id)),
			zap.String("Memory allocate:", strconv.FormatInt(memRequire, 10)),
			zap.String("Current Memory Usage:", strconv.FormatInt(m.currUsage, 10)),
			zap.String("Memory limitation:", strconv.FormatInt(m.maxLimit, 10)))
		return nil, err
	}

	wCtx = &WorkerContext{}
	err = wCtx.InitWorkerContext(engineInfoKey, id)
	if err != nil {
		logutil.BgLogger().Error(LitErrCreateContextFail, zap.String("Engine key", engineInfoKey),
			zap.String("worer Id:", strconv.Itoa(id)),
			zap.String("Memory allocate:", strconv.FormatInt(memRequire, 10)),
			zap.String("Current Memory Usage:", strconv.FormatInt(m.currUsage, 10)),
			zap.String("Memory limitation:", strconv.FormatInt(m.maxLimit, 10)))
		return nil, err
	}

	// Count memory usage.
	m.currUsage += memRequire
	logutil.BgLogger().Info(LitInfoCreateWrite, zap.String("Engine key", engineInfoKey),
		zap.String("worker Id:", strconv.Itoa(id)),
		zap.String("Memory allocate:", strconv.FormatInt(memRequire, 10)),
		zap.String("Current Memory Usage:", strconv.FormatInt(m.currUsage, 10)),
		zap.String("Memory limitation:", strconv.FormatInt(m.maxLimit, 10)))
	return wCtx, err
}

// DeleteBackendEngines uniforms entry to release Engine info.
func (m *MemoryRoot) DeleteBackendEngines(bcKey string) error {
	var (
		err   error
		count int
	)
	bc, exist := m.getBackendContext(bcKey, true)
	if !exist {
		logutil.BgLogger().Error(LitErrGetBackendFail, zap.String("backend key", bcKey))
		return err
	}
	count = 0
	// Delete EngienInfo registered in m.engineManager.engineCache
	for _, ei := range bc.EngineCache {
		eiKey := ei.key
		wCnt := ei.WriterCount
		m.currUsage -= m.structSize[eiKey]
		delete(m.structSize, eiKey)
		delete(m.EngineMgr.enginePool, eiKey)
		m.currUsage -= m.structSize[string(AllocWorkerContext)] * int64(wCnt)
		count++
		logutil.BgLogger().Info(LitInfoCloseEngine, zap.String("backend key", bcKey),
			zap.String("engine id", eiKey),
			zap.String("Current Memory Usage:", strconv.FormatInt(m.currUsage, 10)),
			zap.String("Memory limitation:", strconv.FormatInt(m.maxLimit, 10)))
	}

	bc.EngineCache = make(map[string]*engineInfo, 10)
	m.currUsage -= m.structSize[string(AllocEngineInfo)] * int64(count)
	m.engineUsage -= m.structSize[string(AllocEngineInfo)] * int64(count)
	logutil.BgLogger().Info(LitInfoCloseBackend, zap.String("backend key", bcKey),
		zap.String("Current Memory Usage:", strconv.FormatInt(m.currUsage, 10)),
		zap.String("Memory limitation:", strconv.FormatInt(m.maxLimit, 10)))
	return err
}

func (m *MemoryRoot) getBackendContext(bcKey string, needLog bool) (*BackendContext, bool) {
	bc, exist := m.backendCache[bcKey]
	if !exist {
		if needLog {
			logutil.BgLogger().Warn(LitWarnBackendNOTExist, zap.String("backend key:", bcKey))
		}
		return nil, false
	}
	return bc, exist
}

// updateTotalMemoryConsumption calculates and updates the current total memory consumption.
func (m *MemoryRoot) updateTotalMemoryConsumption() {
	var diffSize int64 = 0
	for _, bc := range m.backendCache {
		curSize := bc.Backend.TotalMemoryConsume()
		bcSize, exist := m.structSize[bc.Key]
		if !exist {
			diffSize += curSize
			m.structSize[bc.Key] = curSize
		} else {
			diffSize += curSize - bcSize
			m.structSize[bc.Key] += curSize - bcSize
		}
		m.structSize[bc.Key] = curSize
	}
	m.currUsage += diffSize
}

// workerDegree adjust worker count according the available memory.
// return 0 means there is no enough memory for one lightning worker.
func (m *MemoryRoot) workerDegree(workerCnt int, engineKey string, bcKey string) int {
	var enSize int64
	var currWorkerNum int
	bc, exist := m.backendCache[bcKey]
	if !exist {
		return 0
	}

	_, exist = m.structSize[engineKey]
	if !exist {
		enSize = int64(bc.cfg.TikvImporter.EngineMemCacheSize)
	} else {
		en, exist1 := bc.EngineCache[engineKey]
		if !exist1 {
			return 0
		}
		currWorkerNum = en.WriterCount
	}
	if currWorkerNum+workerCnt > bc.cfg.TikvImporter.RangeConcurrency {
		workerCnt = bc.cfg.TikvImporter.RangeConcurrency - currWorkerNum
		if workerCnt == 0 {
			return workerCnt
		}
	}

	size := int64(bc.cfg.TikvImporter.LocalWriterMemCacheSize)

	// If only one worker's memory init requirement still bigger than mem limitation.
	if enSize+size+m.currUsage > m.maxLimit {
		return int(allocFailed)
	}

	for enSize+size*int64(workerCnt)+m.currUsage > m.maxLimit && workerCnt > 1 {
		workerCnt /= 2
	}

	m.currUsage += size * int64(workerCnt)

	if !exist {
		m.structSize[engineKey] = size * int64(workerCnt)
	} else {
		m.structSize[engineKey] += size * int64(workerCnt)
	}
	return workerCnt
}

// DiskStat check total lightning disk usage and storage available space.
func (m *MemoryRoot) DiskStat() (uint64, uint64) {
	var totalDiskUsed int64
	for _, bc := range m.backendCache {
		_, _, bcDiskUsed, _ := bc.Backend.CheckDiskQuota(GlobalEnv.diskQuota)
		totalDiskUsed += bcDiskUsed
	}
	sz, err := common.GetStorageSize(GlobalEnv.SortPath)
	if err != nil {
		logutil.BgLogger().Error(LitErrGetStorageQuota,
			zap.String("OS error:", err.Error()),
			zap.String("default disk quota", strconv.FormatInt(GlobalEnv.diskQuota, 10)))
		return uint64(totalDiskUsed), uint64(GlobalEnv.diskQuota)
	}
	return uint64(totalDiskUsed), sz.Available
}

// SetFull set memory used up, only used for testing
func (m *MemoryRoot) SetFull() {
	m.maxLimit = m.currUsage
}
