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
	"fmt"
	"syscall"

	lcom "github.com/pingcap/tidb/br/pkg/lightning/common"
	"github.com/pingcap/tidb/br/pkg/lightning/log"
	tidbcfg "github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/util/logutil"
)

const (
	prefix_str      = "------->"
	_kb             = 1024
	_mb             = 1024 * _kb
	_gb             = 1024 * _mb
	flush_size      = 8 * _mb
	RegionSizeStats = "RegionSizeStats"
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
	BackendCache  *BackendCache
	EngineManager *EngineManager
	diskQuota     int64
}

var GlobalLightningEnv LightningEnv

func init() {
	GlobalLightningEnv.limit = 1024           // Init a default value 1024 for limit.
	GlobalLightningEnv.diskQuota = 1024 * _mb // default disk quota set to 1 GB
	var rLimit syscall.Rlimit
	err := syscall.Getrlimit(syscall.RLIMIT_NOFILE, &rLimit)
	if err != nil {
		logutil.BgLogger().Warn(fmt.Sprintf("GetSystemRLimit err:%s;use default 1024.", err.Error()))
	} else {
		GlobalLightningEnv.limit = int64(rLimit.Cur)
	}
}

func InitGolbalLightningBackendEnv() {
	cfg := tidbcfg.GetGlobalConfig()
	GlobalLightningEnv.PdAddr = cfg.AdvertiseAddress
	GlobalLightningEnv.Port = cfg.Port
	GlobalLightningEnv.Status = cfg.Status.StatusPort
	// Todo need to get the specific lightning sort part from config.
	GlobalLightningEnv.SortPath = ""
	// Todo need to set Memory limitation, temp set to 128 G
	GlobalLightningEnv.LitMemRoot.init(128 * _gb)
	log.SetAppLogger(logutil.BgLogger())
	log.L().Info("Init global lightning backend environment finished.")

}

func (l *LightningEnv) parseDiskQuota() {
	sz, err := lcom.GetStorageSize(l.SortPath)
	if err != nil {
		fmt.Println(fmt.Sprintf("GetStorageSize err:%s;use default 10G.", err.Error()))
		return
	}
	if l.diskQuota > int64(sz.Available) {
		l.diskQuota = int64(sz.Available)
	}
}
