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

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/br/pkg/lightning/backend"
	"github.com/pingcap/tidb/br/pkg/lightning/backend/kv"
	"github.com/pingcap/tidb/br/pkg/lightning/config"
	tikv "github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

// BackendContext store a backend info for add index reorg task.
type BackendContext struct {
	key         string // Currently, backend key used ddl job id string
	backend     *backend.Backend
	ctx         context.Context
	cfg         *config.Config
	EngMgr      EngineManager
	sysVars     map[string]string
	needRestore bool
}

func (bc *BackendContext) Finish(engineInfoKey string, unique bool, tbl table.Table) error {
	var errMsg string
	var keyMsg string
	ei, exist := bc.EngMgr.Load(engineInfoKey)
	if !exist {
		return errors.New(LitErrGetEngineFail)
	}
	defer func() {
		bc.EngMgr.Drop(engineInfoKey)
	}()

	err := ei.ImportAndClean()
	if err != nil {
		return err
	}

	// Check Remote duplicate value for index
	if unique {
		hasDupe, err := bc.backend.CollectRemoteDuplicateIndex(bc.ctx, tbl, ei.tableName, &kv.SessionOptions{
			SQLMode: mysql.ModeStrictAllTables,
			SysVars: ei.backCtx.sysVars,
		}, ei.id)
		if err != nil {
			errMsg = LitErrRemoteDupCheckrr + keyMsg
			logutil.BgLogger().Error(errMsg)
			return errors.New(errMsg)
		} else if hasDupe {
			errMsg = LitErrRemoteDupExistErr + keyMsg
			logutil.BgLogger().Error(errMsg)
			return tikv.ErrKeyExists
		}
	}
	return nil
}

func (bc *BackendContext) Flush(engineKey string) error {
	ei, exist := bc.EngMgr.Load(engineKey)
	if !exist {
		logutil.BgLogger().Error(LitErrGetEngineFail, zap.String("Engine key:", engineKey))
		return errors.New(LitErrGetEngineFail)
	}

	totalStorageUsed, totalStorageAvail := GlobalEnv.DiskStat()
	GlobalEnv.checkAndResetQuota()
	if GlobalEnv.NeedImportEngineData(totalStorageUsed, totalStorageAvail) {
		// ToDo it should be changed according checkpoint solution.
		// Flush writer cached data into local disk for engine first.
		err := ei.Flush(bc.ctx)
		if err != nil {
			return err
		}
		logutil.BgLogger().Info(LitInfoUnsafeImport, zap.String("Engine key:", engineKey), zap.String("Current total available disk:", strconv.FormatUint(totalStorageAvail, 10)))
		err = bc.backend.UnsafeImportAndReset(ei.backCtx.ctx, ei.uuid, int64(config.SplitRegionSize)*int64(config.MaxSplitRegionSizeRatio), int64(config.SplitRegionKeys))
		if err != nil {
			logutil.BgLogger().Error(LitErrIngestDataErr, zap.String("Engine key:", engineKey),
				zap.String("import partial file failed, current disk storage remains", strconv.FormatUint(totalStorageAvail, 10)))
			return err
		}
	}
	return nil
}

func (bc *BackendContext) NeedRestore() bool {
	return bc.needRestore
}

func (bc *BackendContext) SetNeedRestore(needRestore bool) {
	bc.needRestore = needRestore
}
