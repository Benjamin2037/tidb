// Copyright 2026 PingCAP, Inc.
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

package executor

import (
	"context"
	"strings"

	"github.com/pingcap/errors"
	fstorage "github.com/pingcap/tidb/pkg/dxf/framework/storage"
	"github.com/pingcap/tidb/pkg/executor/importer"
	"github.com/pingcap/tidb/pkg/executor/internal/exec"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/privilege"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/dbterror/exeerrors"
	"github.com/pingcap/tidb/pkg/util/dbterror/plannererrors"
	"github.com/tikv/client-go/v2/util"
)

type ImportIntoSLOGuardExec struct {
	exec.BaseExecutor
	jobID   int64
	options []*ast.LoadDataOpt
}

var _ exec.Executor = (*ImportIntoSLOGuardExec)(nil)

type sloGuardOptionValues struct {
	configJSON string
}

// Next implements the Executor Next interface.
func (e *ImportIntoSLOGuardExec) Next(ctx context.Context, _ *chunk.Chunk) (err error) {
	ctx = kv.WithInternalSourceType(ctx, kv.InternalImportInto)

	optValues, err := parseSLOGuardOptions(e.Ctx(), e.options)
	if err != nil {
		return err
	}

	var hasSuperPriv bool
	if pm := privilege.GetPrivilegeManager(e.Ctx()); pm != nil {
		hasSuperPriv = pm.RequestVerification(e.Ctx().GetSessionVars().ActiveRoles, "", "", "", mysql.SuperPriv)
	}

	taskManager, err := fstorage.GetTaskManager()
	ctx = util.WithInternalSourceType(ctx, kv.InternalDistTask)
	if err != nil {
		return err
	}

	user := ""
	if currentUser := e.Ctx().GetSessionVars().User; currentUser != nil {
		user = currentUser.String()
	}

	return taskManager.WithNewSession(func(se sessionctx.Context) error {
		exec := se.GetSQLExecutor()
		if e.jobID == importer.SLOGuardGlobalJobID {
			if !hasSuperPriv {
				return plannererrors.ErrSpecificAccessDenied.GenWithStackByArgs("SUPER")
			}
		} else {
			if _, err := importer.GetJob(ctx, exec, e.jobID, user, hasSuperPriv); err != nil {
				return err
			}
		}

		cfg, found, err := importer.GetSLOGuardConfig(ctx, exec, e.jobID)
		if err != nil {
			return err
		}
		baseConfigJSON := ""
		if found {
			baseConfigJSON = cfg.ConfigJSON
		}
		mergedCfg, mergedJSON, err := importer.MergeSLOGuardConfigJSON(baseConfigJSON, optValues.configJSON)
		if err != nil {
			return err
		}
		mergedCfg.JobID = e.jobID
		mergedCfg.ConfigJSON = mergedJSON
		mergedCfg.UpdatedBy = user
		return importer.UpsertSLOGuardConfig(ctx, exec, mergedCfg)
	})
}

func parseSLOGuardOptions(sctx sessionctx.Context, opts []*ast.LoadDataOpt) (sloGuardOptionValues, error) {
	values := sloGuardOptionValues{}
	seen := make(map[string]struct{}, len(opts))
	for _, opt := range opts {
		name := strings.ToLower(strings.TrimSpace(opt.Name))
		if name == "" {
			return values, exeerrors.ErrUnknownOption.FastGenByArgs(opt.Name)
		}
		if _, ok := seen[name]; ok {
			return values, exeerrors.ErrDuplicateOption.FastGenByArgs(opt.Name)
		}
		seen[name] = struct{}{}

		switch name {
		case "config":
			if opt.Value == nil {
				return values, exeerrors.ErrInvalidOptionVal.FastGenByArgs(opt.Name)
			}
			datum, err := evalSLOGuardOptionDatum(sctx, opt)
			if err != nil {
				return values, err
			}
			text, err := parseSLOGuardString(datum)
			if err != nil || strings.TrimSpace(text) == "" {
				return values, exeerrors.ErrInvalidOptionVal.FastGenByArgs(opt.Name)
			}
			values.configJSON = text
		default:
			return values, exeerrors.ErrUnknownOption.FastGenByArgs(opt.Name)
		}
	}
	if strings.TrimSpace(values.configJSON) == "" {
		return values, exeerrors.ErrInvalidOptionVal.FastGenByArgs("config")
	}
	return values, nil
}

func evalSLOGuardOptionDatum(sctx sessionctx.Context, opt *ast.LoadDataOpt) (types.Datum, error) {
	if opt.Value == nil {
		return types.Datum{}, errors.New("option value is nil")
	}
	return expression.EvalSimpleAst(sctx.GetExprCtx(), opt.Value)
}

func parseSLOGuardString(d types.Datum) (string, error) {
	switch d.Kind() {
	case types.KindString:
		return strings.TrimSpace(d.GetString()), nil
	case types.KindBytes:
		return strings.TrimSpace(string(d.GetBytes())), nil
	}
	return "", errors.New("invalid string")
}
