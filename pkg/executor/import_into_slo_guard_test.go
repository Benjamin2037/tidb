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

package executor_test

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/pingcap/tidb/pkg/executor/importer"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/auth"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/privilege"
	"github.com/pingcap/tidb/pkg/privilege/privileges"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

type denySuperPrivManager struct {
	privilege.Manager
}

func (m *denySuperPrivManager) RequestVerification(activeRoles []*auth.RoleIdentity, db, table, column string, priv mysql.PrivilegeType) bool {
	if priv == mysql.SuperPriv {
		return false
	}
	return m.Manager.RequestVerification(activeRoles, db, table, column, priv)
}

func (m *denySuperPrivManager) RequestVerificationWithUser(ctx context.Context, db, table, column string, priv mysql.PrivilegeType, user *auth.UserIdentity) bool {
	if priv == mysql.SuperPriv {
		return false
	}
	return m.Manager.RequestVerificationWithUser(ctx, db, table, column, priv, user)
}

func TestAlterAndShowImportIntoSLOGuard(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (id int primary key)")
	tk.Session().GetSessionVars().User = &auth.UserIdentity{Username: "root", Hostname: "%"}

	tbl, err := dom.InfoSchema().TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("t"))
	require.NoError(t, err)

	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalImportInto)
	exec := tk.Session().(sessionctx.Context).GetSQLExecutor()
	params := &importer.ImportParameters{
		FileLocation: "local://",
		Format:       "csv",
	}
	createdBy := tk.Session().GetSessionVars().User.String()
	jobID, err := importer.CreateJob(ctx, exec, "test", "t", tbl.Meta().ID, createdBy, "", params, 0)
	require.NoError(t, err)

	rows := tk.MustQuery(fmt.Sprintf("show import slo guard job %d", jobID)).Rows()
	require.Len(t, rows, 1)
	require.Equal(t, fmt.Sprintf("%d", jobID), rows[0][0])
	var cfg map[string]any
	require.NoError(t, json.Unmarshal([]byte(fmt.Sprint(rows[0][1])), &cfg))
	require.Equal(t, false, cfg["enable"])
	require.Equal(t, "10ms", cfg["pause_threshold"])
	require.Equal(t, float64(64), cfg["slow_apply_rate_limit_mb_per_sec"])
	require.Equal(t, "0", rows[0][2])
	require.Equal(t, "10ms", rows[0][3])
	require.Equal(t, "64", rows[0][4])

	tk.MustExec(fmt.Sprintf(
		"alter import slo guard job %d with config='{\"enable\":true,\"pause_threshold\":\"20ms\",\"slow_apply_rate_limit_mb_per_sec\":128}'",
		jobID,
	))

	row := tk.MustQuery(fmt.Sprintf("show import slo guard job %d", jobID)).Rows()[0]
	require.Equal(t, fmt.Sprintf("%d", jobID), row[0])
	require.NoError(t, json.Unmarshal([]byte(fmt.Sprint(row[1])), &cfg))
	require.Equal(t, true, cfg["enable"])
	require.Equal(t, "20ms", cfg["pause_threshold"])
	require.Equal(t, float64(128), cfg["slow_apply_rate_limit_mb_per_sec"])
	require.Equal(t, "1", row[2])
	require.Equal(t, "20ms", row[3])
	require.Equal(t, "128", row[4])
}

func TestShowImportIntoSLOGuardAllSuperFallbackGlobal(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (id int primary key)")

	tbl, err := dom.InfoSchema().TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("t"))
	require.NoError(t, err)
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalImportInto)
	exec := tk.Session().(sessionctx.Context).GetSQLExecutor()
	createdBy := "root@%"
	jobID, err := importer.CreateJob(ctx, exec, "test", "t", tbl.Meta().ID, createdBy, "", &importer.ImportParameters{}, 0)
	require.NoError(t, err)

	tk.MustExec("delete from mysql.tidb_import_slo_guard where job_id = 0")
	tk.MustExec(fmt.Sprintf(
		"insert into mysql.tidb_import_slo_guard (job_id, enable, pause_threshold, slow_apply_rate_limit_mb_per_sec, updated_by) values (%d, 1, '20ms', 128, 'root')",
		jobID,
	))

	rows := tk.MustQuery("show import slo guard").Rows()
	require.Len(t, rows, 2)
	require.Equal(t, "0", rows[0][0])
	require.Equal(t, fmt.Sprintf("%d", jobID), rows[1][0])
}

func TestShowImportIntoSLOGuardAllNoSuper(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tkRoot := testkit.NewTestKit(t, store)
	tk := testkit.NewTestKit(t, store)
	oldSkip := privileges.SkipWithGrant
	t.Cleanup(func() { privileges.SkipWithGrant = oldSkip })
	privileges.SkipWithGrant = false
	tkRoot.MustExec("use test")
	tkRoot.MustExec("create table t (id int primary key)")
	tk.MustExec("use test")

	tk.Session().GetSessionVars().User = &auth.UserIdentity{Username: "user1", Hostname: "%"}
	pm := privilege.GetPrivilegeManager(tk.Session())
	require.NotNil(t, pm)
	pm.AuthSuccess("user1", "%")
	privilege.BindPrivilegeManager(tk.Session(), &denySuperPrivManager{Manager: pm})

	rows := tk.MustQuery("show import slo guard").Rows()
	require.Len(t, rows, 0)

	tbl, err := dom.InfoSchema().TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("t"))
	require.NoError(t, err)
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalImportInto)
	exec := tkRoot.Session().(sessionctx.Context).GetSQLExecutor()
	createdBy := tk.Session().GetSessionVars().User.String()
	jobID, err := importer.CreateJob(ctx, exec, "test", "t", tbl.Meta().ID, createdBy, "", &importer.ImportParameters{}, 0)
	require.NoError(t, err)

	// No config row yet, should be empty.
	rows = tk.MustQuery("show import slo guard").Rows()
	require.Len(t, rows, 0)

	tkRoot.MustExec(fmt.Sprintf(
		"insert into mysql.tidb_import_slo_guard (job_id, enable, pause_threshold, slow_apply_rate_limit_mb_per_sec, updated_by) values (%d, 0, '10ms', 64, 'root')",
		jobID,
	))
	rows = tk.MustQuery("show import slo guard").Rows()
	require.Len(t, rows, 1)
	require.Equal(t, fmt.Sprintf("%d", jobID), rows[0][0])
}
