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
	"os"
	"strconv"
	"syscall"

	lcom "github.com/pingcap/tidb/br/pkg/lightning/common"
	"github.com/pingcap/tidb/br/pkg/lightning/log"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

const (
	prefix_str      = "------->"
	_kb             = 1024
	_mb             = 1024 * _kb
	_gb             = 1024 * _mb
	flush_size      = 8 * _mb
	diskQuota       = 512 * _mb

	// 
)
type ClusterInfo struct {
	PdAddr string
	// TidbHost string - 127.0.0.1
	Port   uint
	Status uint
}
type LightningEnv struct {
	limit int64
	ClusterInfo
	SortPath      string
	LitMemRoot    LightningMemoryRoot
	diskQuota     int64
	IsInited      bool
}

var (
	GlobalLightningEnv LightningEnv
	maxMemLimit  uint64 = 128 * _mb
)

func init() {
	GlobalLightningEnv.limit = 1024           // Init a default value 1024 for limit.
	GlobalLightningEnv.diskQuota = 10 * _gb   // default disk quota set to 10 GB
	var rLimit syscall.Rlimit
	err := syscall.Getrlimit(syscall.RLIMIT_NOFILE, &rLimit)
	if err != nil {
		logutil.BgLogger().Warn(LERR_GET_SYS_LIMIT_ERR, zap.String("OS error:", err.Error()), zap.String("Default: ", "1024."))
	} else {
		GlobalLightningEnv.limit = int64(rLimit.Cur)
	}
	GlobalLightningEnv.IsInited = false
	GlobalLightningEnv.diskQuota = diskQuota

}

func InitGolbalLightningBackendEnv() {
	var bufferSize uint64

	GlobalLightningEnv.LitMemRoot.init(int64(maxMemLimit))
	log.SetAppLogger(logutil.BgLogger())

	cfg := config.GetGlobalConfig()
	GlobalLightningEnv.PdAddr = cfg.AdvertiseAddress
	GlobalLightningEnv.Port = cfg.Port
	GlobalLightningEnv.Status = cfg.Status.StatusPort
    GlobalLightningEnv.SortPath = genLightningDataDir()
    err := GlobalLightningEnv.parseDiskQuota()
	// Set Memory usage limitation to 1 GB
	sbz := variable.GetSysVar("sort_buffer_size")
	bufferSize, err = strconv.ParseUint(sbz.Value, 10, 64)
	// If get bufferSize err, then maxMemLimtation is 128 MB
	// Otherwise, the ddl maxMemLimitation is 1 GB
	if err == nil {
		maxMemLimit  = bufferSize * 4 * _kb
		log.L().Info(LINFO_GEN_MEM_LIMIT,
			zap.String("Memory limitation set to:", strconv.FormatUint(maxMemLimit, 10)))
	} else {
		log.L().Info(LWAR_GEN_MEM_LIMIT,
			zap.Error(err),
			zap.String("will use default memory limitation:", strconv.FormatUint(maxMemLimit, 10)))
	}
	log.L().Info(LInfo_ENV_INIT_SUCC,
		zap.String("Memory limitation set to:", strconv.FormatUint(maxMemLimit, 10)),
	    zap.String("Sort Path disk quota:", strconv.FormatUint(uint64(GlobalLightningEnv.diskQuota), 10)),
	    zap.String("Max open file number:", strconv.Itoa(int(GlobalLightningEnv.limit))))
	GlobalLightningEnv.IsInited = true
	return
}

func (l *LightningEnv) parseDiskQuota() error {
	sz, err := lcom.GetStorageSize(l.SortPath)
	if err != nil {
		log.L().Error(LERR_GET_STORAGE_QUOTA,
			zap.String("Os error:", err.Error()),
			zap.String("default disk quota", strconv.FormatInt(l.diskQuota, 10)))
		return err
	}
	l.diskQuota = int64(sz.Available)
    return err
}

// Generate lightning local store dir in TiDB datadir. 
func genLightningDataDir() string {
	dataDir := variable.GetSysVar(variable.DataDir)
	var sortPath string = "/tmp/lightning/"
	// If DataDir is not a dir path(strat with /), then set lightning to /tmp/lightning
	if string(dataDir.Value[0]) == "/" {
		sortPath = dataDir.Value + "/lightning/"
	}
	shouldCreate := true
	if info, err := os.Stat(sortPath); err != nil {
		if !os.IsNotExist(err) {
			log.L().Error(LERR_CREATE_DIR_FAILED, zap.String("Sort path:", sortPath),
		        zap.String("Error:", err.Error()))
			return "/tmp/lightning/"
		}
	} else if info.IsDir() {
		shouldCreate = false
	}

	if shouldCreate {
		err := os.Mkdir(sortPath, 0o700)
		if err != nil {
			log.L().Error(LERR_CREATE_DIR_FAILED, zap.String("Sort path:", sortPath),
		        zap.String("Error:", err.Error()))
			return "/tmp/lightning/"
		}
	}
	log.L().Info(LInfo_SORTED_DIR, zap.String("data path:", sortPath))
	return sortPath
}

