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
	"syscall"

	lcom "github.com/pingcap/tidb/br/pkg/lightning/common"
	"github.com/pingcap/tidb/br/pkg/lightning/log"
	tidbcfg "github.com/pingcap/tidb/config"
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
	RegionSizeStats = "RegionSizeStats"
	maxMemLimation  = 10 * _gb
)

var defaultImportantVariables = map[string]string{
	"max_allowed_packet":      "67108864",
	"div_precision_increment": "4",
	"time_zone":               "SYSTEM",
	"lc_time_names":           "en_US",
	"default_week_format":     "0",
	"block_encryption_mode":   "aes-128-ecb",
	"group_concat_max_len":    "1024",
}
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
	BackendCache  map[string]*BackendContext
	EngineManager *EngineManager
	diskQuota     int64
	IsInited      bool
}

var GlobalLightningEnv LightningEnv

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
}

func InitGolbalLightningBackendEnv() {
	cfg := tidbcfg.GetGlobalConfig()
	GlobalLightningEnv.PdAddr = cfg.AdvertiseAddress
	GlobalLightningEnv.Port = cfg.Port
	GlobalLightningEnv.Status = cfg.Status.StatusPort
	if err := GlobalLightningEnv.initSortPath(); err != nil {
		GlobalLightningEnv.IsInited = false
		log.L().Warn(LWAR_ENV_INIT_FAILD, zap.String("Os error", err.Error()))
		return
	}
	if GlobalLightningEnv.IsInited {
		GlobalLightningEnv.parseDiskQuota()
	}
	// Todo need to set Memory limitation, temp set to 128 G
	GlobalLightningEnv.LitMemRoot.init(maxMemLimation)
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

// This init SortPath func will clean up the file in dir, the file exist will be keep as it is be
func (l *LightningEnv) initSortPath() error {
	shouldCreate := true
    l.SortPath = GenLightningDataDir()
	if info, err := os.Stat(l.SortPath); err != nil {
		if !os.IsNotExist(err) {
			return err
		}
	} else if info.IsDir() {
		shouldCreate = false
	}

	if shouldCreate {
		err := os.Mkdir(l.SortPath, 0o700)
		if err != nil {
			return err
		}
	}
	return nil
}

// Generate lightning path in TiDB datadir. 
func GenLightningDataDir() string {
	dataDir := variable.GetSysVar(variable.DataDir)
	var sortPath string
	// If DataDir is not a dir path, then set lightning to /tmp/lightning
	if string(dataDir.Value[0]) != "/" {
		sortPath = "/tmp/lightning"
	} else {
		sortPath = dataDir.Value + "lightning"
	}
	return sortPath
}

