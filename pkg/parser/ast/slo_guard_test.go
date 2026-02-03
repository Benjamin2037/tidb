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

package ast_test

import (
	"errors"
	"strings"
	"testing"

	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/format"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/stretchr/testify/require"
)

func TestAlterImportIntoSLOGuardRestoreAndSem(t *testing.T) {
	jobID := int64(3)
	stmt := &ast.AlterImportIntoSLOGuardStmt{
		JobID: &jobID,
		Options: []*ast.LoadDataOpt{
			{
				Name:  "config",
				Value: ast.NewValueExpr("{\"enable\":true}", mysql.DefaultCharset, mysql.DefaultCollationName),
			},
			{
				Name:  "pause_threshold",
				Value: ast.NewValueExpr("20ms", mysql.DefaultCharset, mysql.DefaultCollationName),
			},
		},
	}
	var sb strings.Builder
	ctx := format.NewRestoreCtx(format.DefaultRestoreFlags, &sb)
	require.NoError(t, stmt.Restore(ctx))
	require.Contains(t, sb.String(), "ALTER IMPORT SLO GUARD JOB 3 WITH config")
	require.Equal(t, ast.AlterImportIntoSLOGuardCommand, stmt.SEMCommand())
}

func TestShowImportIntoSLOGuardRestoreAndSem(t *testing.T) {
	jobID := int64(7)
	stmt := &ast.ShowStmt{Tp: ast.ShowImportIntoSLOGuard, ImportJobID: &jobID}
	var sb strings.Builder
	ctx := format.NewRestoreCtx(format.DefaultRestoreFlags, &sb)
	require.NoError(t, stmt.Restore(ctx))
	require.Equal(t, "SHOW IMPORT SLO GUARD JOB 7", sb.String())
	require.Equal(t, ast.ShowImportIntoSLOGuardCommand, stmt.SEMCommand())
}

type sloGuardVisitor struct {
	enter int
	leave int
}

func (v *sloGuardVisitor) Enter(n ast.Node) (ast.Node, bool) {
	v.enter++
	return n, true
}

func (v *sloGuardVisitor) Leave(n ast.Node) (ast.Node, bool) {
	v.leave++
	return n, true
}

func TestAlterImportIntoSLOGuardAccept(t *testing.T) {
	stmt := &ast.AlterImportIntoSLOGuardStmt{}
	visitor := &sloGuardVisitor{}
	stmt.Accept(visitor)
	require.Equal(t, 1, visitor.enter)
	require.Equal(t, 1, visitor.leave)
}

func TestAlterImportIntoSLOGuardRestoreMinimal(t *testing.T) {
	stmt := &ast.AlterImportIntoSLOGuardStmt{}
	var sb strings.Builder
	ctx := format.NewRestoreCtx(format.DefaultRestoreFlags, &sb)
	require.NoError(t, stmt.Restore(ctx))
	require.Equal(t, "ALTER IMPORT SLO GUARD", sb.String())
}

type errRestoreExpr struct {
	ast.ValueExpr
}

func (*errRestoreExpr) Restore(*format.RestoreCtx) error {
	return errors.New("restore failed")
}

func TestAlterImportIntoSLOGuardRestoreOptionError(t *testing.T) {
	stmt := &ast.AlterImportIntoSLOGuardStmt{
		Options: []*ast.LoadDataOpt{
			{
				Name:  "config",
				Value: &errRestoreExpr{},
			},
		},
	}
	var sb strings.Builder
	ctx := format.NewRestoreCtx(format.DefaultRestoreFlags, &sb)
	err := stmt.Restore(ctx)
	require.Error(t, err)
	require.Contains(t, err.Error(), "AlterImportIntoSLOGuardStmt option")
}
