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
	"path/filepath"
	"strconv"

	"github.com/pingcap/tidb/br/pkg/lightning/config"
	tidbconf "github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

func generateLightningConfig(bcKey string, unique bool) (*config.Config, error) {
	cfg := config.NewConfig()
	cfg.TikvImporter.Backend = config.BackendLocal
	// Each backend will build a single dir in lightning dir.
	cfg.TikvImporter.SortedKVDir = filepath.Join(GlobalEnv.SortPath, bcKey)
	_, err := cfg.AdjustCommon()
	if err != nil {
		logutil.BgLogger().Warn(LitWarnConfigError, zap.Error(err))
		return nil, err
	}
	adjustImportMemory(cfg)
	cfg.Checkpoint.Enable = true
	if unique {
		cfg.TikvImporter.DuplicateResolution = config.DupeResAlgRecord
	} else {
		cfg.TikvImporter.DuplicateResolution = config.DupeResAlgNone
	}
	cfg.TiDB.PdAddr = GlobalEnv.PdAddr
	cfg.TiDB.Host = "127.0.0.1"
	cfg.TiDB.StatusPort = int(GlobalEnv.Status)
	// Set TLS related information
	cfg.Security.CAPath = tidbconf.GetGlobalConfig().Security.ClusterSSLCA
	cfg.Security.CertPath = tidbconf.GetGlobalConfig().Security.ClusterSSLCert
	cfg.Security.KeyPath = tidbconf.GetGlobalConfig().Security.ClusterSSLKey

	return cfg, err
}

// Adjust lightning memory parameters according memory root's max limitation
func adjustImportMemory(cfg *config.Config) {
	var scale int64
	// Try aggressive resource usage successful.
	if tryAggressiveMemory(cfg) {
		return
	}

	defaultMemSize := int64(cfg.TikvImporter.LocalWriterMemCacheSize) * int64(cfg.TikvImporter.RangeConcurrency)
	defaultMemSize += 4 * int64(cfg.TikvImporter.EngineMemCacheSize)
	logutil.BgLogger().Info(LitInfoInitMemSetting,
		zap.String("LocalWriterMemCacheSize:", strconv.FormatInt(int64(cfg.TikvImporter.LocalWriterMemCacheSize), 10)),
		zap.String("EngineMemCacheSize:", strconv.FormatInt(int64(cfg.TikvImporter.LocalWriterMemCacheSize), 10)),
		zap.String("range concurrency:", strconv.Itoa(cfg.TikvImporter.RangeConcurrency)))

	if defaultMemSize > GlobalEnv.LitMemRoot.maxLimit {
		scale = defaultMemSize / GlobalEnv.LitMemRoot.maxLimit
	}

	if scale == 1 || scale == 0 {
		return
	}

	cfg.TikvImporter.LocalWriterMemCacheSize /= config.ByteSize(scale)
	cfg.TikvImporter.EngineMemCacheSize /= config.ByteSize(scale)
	// TODO: adjust range concurrency number to control total concurrency in future.
	logutil.BgLogger().Info(LitInfoChgMemSetting,
		zap.String("LocalWriterMemCacheSize:", strconv.FormatInt(int64(cfg.TikvImporter.LocalWriterMemCacheSize), 10)),
		zap.String("EngineMemCacheSize:", strconv.FormatInt(int64(cfg.TikvImporter.LocalWriterMemCacheSize), 10)),
		zap.String("range concurrency:", strconv.Itoa(cfg.TikvImporter.RangeConcurrency)))
}

// tryAggressiveMemory lightning memory parameters according memory root's max limitation.
func tryAggressiveMemory(cfg *config.Config) bool {
	var defaultMemSize int64
	defaultMemSize = int64(int(cfg.TikvImporter.LocalWriterMemCacheSize) * cfg.TikvImporter.RangeConcurrency)
	defaultMemSize += int64(cfg.TikvImporter.EngineMemCacheSize)

	if (defaultMemSize + GlobalEnv.LitMemRoot.currUsage) > GlobalEnv.LitMemRoot.maxLimit {
		return false
	}
	logutil.BgLogger().Info(LitInfoChgMemSetting,
		zap.String("LocalWriterMemCacheSize:", strconv.FormatInt(int64(cfg.TikvImporter.LocalWriterMemCacheSize), 10)),
		zap.String("EngineMemCacheSize:", strconv.FormatInt(int64(cfg.TikvImporter.LocalWriterMemCacheSize), 10)),
		zap.String("range concurrency:", strconv.Itoa(cfg.TikvImporter.RangeConcurrency)))
	return true
}

// defaultImportantVariables is used in ObtainImportantVariables to retrieve the system
// variables from downstream which may affect KV encode result. The values record the default
// values if missing.
var defaultImportantVariables = map[string]string{
	"max_allowed_packet":      "67108864",
	"div_precision_increment": "4",
	"time_zone":               "SYSTEM",
	"lc_time_names":           "en_US",
	"default_week_format":     "0",
	"block_encryption_mode":   "aes-128-ecb",
	"group_concat_max_len":    "1024",
}

// defaultImportVariablesTiDB is used in ObtainImportantVariables to retrieve the system
// variables from downstream in local/importer backend. The values record the default
// values if missing.
var defaultImportVariablesTiDB = map[string]string{
	"tidb_row_format_version": "1",
}

func obtainImportantVariables() map[string]string {
	// Convert the result into a map. Fill the missing variables with default values.
	result := make(map[string]string, len(defaultImportantVariables)+len(defaultImportVariablesTiDB))
	for key, value := range defaultImportantVariables {
		result[key] = value
		v := variable.GetSysVar(key)
		if v.Value != value {
			result[key] = value
		}
	}

	for key, value := range defaultImportVariablesTiDB {
		result[key] = value
		v := variable.GetSysVar(key)
		if v.Value != value {
			result[key] = value
		}
	}
	return result
}
