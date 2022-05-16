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
	GlobalLightningEnv.diskQuota = 10 * _gb // default disk quota set to 10 GB
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
	cfg := config.GetGlobalConfig()
	GlobalLightningEnv.PdAddr = cfg.AdvertiseAddress
	GlobalLightningEnv.Port = cfg.Port
	GlobalLightningEnv.Status = cfg.Status.StatusPort
    GlobalLightningEnv.SortPath = genLightningDataDir()
    GlobalLightningEnv.parseDiskQuota()
	// Set Memory usage limitation to 1 GB
	sbz := variable.GetSysVar("sort_buffer_size")
	bufferSize, err := strconv.ParseUint(sbz.Value, 10, 32)
	// If get bufferSize err, then maxMemLimtation is 128 MB
	// Otherwise, the ddl maxMemLimitation is 1 GB
	if err == nil {
		maxMemLimit  = bufferSize * 4 * _kb
	}
	GlobalLightningEnv.LitMemRoot.init(int64(maxMemLimit))
	log.SetAppLogger(logutil.BgLogger())
	log.L().Info(LInfo_ENV_INIT_SUCC)
	GlobalLightningEnv.IsInited = true
	return
}

func (l *LightningEnv) parseDiskQuota() {
	sz, err := lcom.GetStorageSize(l.SortPath)
	if err != nil {
		log.L().Warn(LERR_GET_STORAGE_QUOTA, zap.String("Os error:", err.Error()), zap.String("default size", "10G"))
		return
	}
	l.diskQuota = int64(sz.Available)
}

// Generate lightning local store dir in TiDB datadir. 
func genLightningDataDir() string {
	dataDir := variable.GetSysVar(variable.DataDir)
	var sortPath string
	// If DataDir is not a dir path(strat with /), then set lightning to /tmp/lightning
	if string(dataDir.Value[0]) != "/" {
		sortPath = "/tmp/lightning"
	} else {
		sortPath = dataDir.Value + "lightning"
	}
	return sortPath
}

