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
	"fmt"
	"io/ioutil"
	"os"

	"github.com/docker/go-units"
	"github.com/pingcap/tidb/br/pkg/lightning/backend"
	"github.com/pingcap/tidb/br/pkg/lightning/backend/local"
	"github.com/pingcap/tidb/br/pkg/lightning/config"
	"github.com/pingcap/tidb/util/logutil"
)

type BackendContext struct {
	Key         string // Currently, backend key used ddl job id string
	Backend     *backend.Backend
	Ctx         context.Context
	EngineCache map[string]*engineInfo
	WCtx        map[string]*WorkerContext
}

func (bc *BackendContext) init(k string, b *backend.Backend) {
	bc.Key = k
	bc.Backend = b
	bc.EngineCache = make(map[string]*engineInfo)
	bc.WCtx = make(map[string]*WorkerContext)
}

func generateLightningConfig(unique bool) *config.Config {
	cfg := config.NewConfig()
	cfg.DefaultVarsForImporterAndLocalBackend()
	name, err := ioutil.TempDir(GlobalLightningEnv.SortPath, "lightning")
	if err != nil {
		logutil.BgLogger().Warn(fmt.Sprintf("TempDir err:%s.", err.Error()))
		name = "/tmp/lightning"
	}
	os.Remove(name)

	cfg.Checkpoint.Enable = false
	cfg.TikvImporter.SortedKVDir = name
	if unique {
		cfg.TikvImporter.DuplicateResolution = config.DupeResAlgRecord
	} else {
		cfg.TikvImporter.DuplicateResolution = config.DupeResAlgNone
	}
	cfg.TikvImporter.RangeConcurrency = 32
	cfg.TikvImporter.EngineMemCacheSize = 512 * units.MiB
	cfg.TikvImporter.LocalWriterMemCacheSize = 128 * units.MiB
	cfg.TiDB.PdAddr = GlobalLightningEnv.PdAddr
	cfg.TiDB.Host = "127.0.0.1"
	cfg.TiDB.StatusPort = int(GlobalLightningEnv.Status)
	return cfg
}

func createLocalBackend(ctx context.Context, unique bool) (backend.Backend, error) {
	cfg := generateLightningConfig(unique)
	tls, err := cfg.ToTLS()
	if err != nil {
		return backend.Backend{}, err
	}
	return local.NewLocalBackend(ctx, tls, cfg, nil, int(GlobalLightningEnv.limit), nil)
}

func CloseBackend(bcKey string) {
	GlobalLightningEnv.LitMemRoot.DeleteBackendContext(bcKey)
	return
}
