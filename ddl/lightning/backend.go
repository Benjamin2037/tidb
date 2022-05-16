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
	"strconv"

	"github.com/pingcap/tidb/br/pkg/lightning/backend"
	"github.com/pingcap/tidb/br/pkg/lightning/backend/local"
	"github.com/pingcap/tidb/br/pkg/lightning/config"
	"github.com/pingcap/tidb/br/pkg/lightning/glue"
	"github.com/pingcap/tidb/br/pkg/lightning/log"
)

type BackendContext struct {
	Key         string // Currently, backend key used ddl job id string
	Backend     *backend.Backend
	Ctx         context.Context
	cfg         *config.Config
	EngineCache map[string]*engineInfo
	// Session level tidb glue
	tidbGlue    *glue.ExternalTiDBGlue
	sysVars     map[string]string
}

func (bc *BackendContext) init(k string, b *backend.Backend) {
	bc.Key = k
	bc.Backend = b
	bc.EngineCache = make(map[string]*engineInfo)
}

func generateLightningConfig(ctx context.Context, unique bool) (*config.Config, error) {
	cfg := config.NewConfig()
	gCfg := config.NewGlobalConfig()
	cfg.LoadFromGlobal(gCfg)

    cfg.TikvImporter.Backend = config.BackendLocal
	// Should not output err, after go through cfg.adjust function.
	err :=cfg.Adjust(ctx)
	if err != nil {
		log.L().Warn(LWAR_CONFIG_ERROR)
		return nil, err
	}
	cfg.Checkpoint.Enable = false
	cfg.TikvImporter.SortedKVDir = GlobalLightningEnv.SortPath
	if unique {
		cfg.TikvImporter.DuplicateResolution = config.DupeResAlgRecord
	} else {
		cfg.TikvImporter.DuplicateResolution = config.DupeResAlgNone
	}

	cfg.TiDB.PdAddr = GlobalLightningEnv.PdAddr
	cfg.TiDB.StatusPort = int(GlobalLightningEnv.Status)
	return cfg, err
}

func createLocalBackend(ctx context.Context, cfg *config.Config, glue glue.Glue) (backend.Backend, error) {
	tls, err := cfg.ToTLS()
	if err != nil {
		return backend.Backend{}, err
	}
    
	return local.NewLocalBackend(ctx, tls, cfg, glue, int(GlobalLightningEnv.limit), nil)
}

func CloseBackend(bcKey string) {
	GlobalLightningEnv.LitMemRoot.DeleteBackendContext(bcKey)
	return
}

func GenBackendContextKey(jobId int64) string {
	return strconv.FormatInt(jobId, 10)
}

func adjustImportMemory(cfg *config.Config) {
	var scale int64
	defaultMemSize := int64(cfg.TikvImporter.LocalWriterMemCacheSize) * int64(cfg.TikvImporter.RangeConcurrency)
	defaultMemSize += 4 * int64(cfg.TikvImporter.EngineMemCacheSize)
    
	if defaultMemSize > GlobalLightningEnv.LitMemRoot.maxLimit {
		scale = defaultMemSize / GlobalLightningEnv.LitMemRoot.maxLimit
	}
    
	if scale == 1 {
		return
	}

	cfg.TikvImporter.LocalWriterMemCacheSize /= config.ByteSize(scale)
	cfg.TikvImporter.EngineMemCacheSize /= config.ByteSize(scale)
    // ToDo adjust rangecourrency nubmer to control total concurrency.
	return
}
