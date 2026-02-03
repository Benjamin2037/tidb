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

package importinto

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/ddl"
	"github.com/pingcap/tidb/pkg/executor/importer"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/objstore"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	utilmock "github.com/pingcap/tidb/pkg/util/mock"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestPlanCompaction(t *testing.T) {
	now := time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC)
	oldBase := baseVersion{baseID: "base-1", createTime: now.Add(-11 * 24 * time.Hour)}
	newBase := baseVersion{baseID: "base-2", createTime: now.Add(-1 * 24 * time.Hour)}

	t.Run("retention disabled", func(t *testing.T) {
		compact, gc := planCompaction(now, 0, []baseVersion{oldBase, newBase}, false, false)
		require.Nil(t, compact)
		require.Empty(t, gc)
	})

	t.Run("no old base", func(t *testing.T) {
		compact, gc := planCompaction(now, 10, []baseVersion{newBase}, false, false)
		require.Nil(t, compact)
		require.Empty(t, gc)
	})

	t.Run("schedule compaction", func(t *testing.T) {
		compact, gc := planCompaction(now, 10, []baseVersion{oldBase, newBase}, false, false)
		require.NotNil(t, compact)
		require.Equal(t, "base-2", compact.baseID)
		require.Empty(t, gc)
	})

	t.Run("gc after compaction", func(t *testing.T) {
		compacted := newBase
		compacted.isCompaction = true
		compact, gc := planCompaction(now, 10, []baseVersion{oldBase, compacted}, false, false)
		require.Nil(t, compact)
		require.Len(t, gc, 1)
		require.Equal(t, "base-1", gc[0].baseID)
	})

	t.Run("active job blocks", func(t *testing.T) {
		compact, gc := planCompaction(now, 10, []baseVersion{oldBase, newBase}, true, false)
		require.Nil(t, compact)
		require.Empty(t, gc)
	})

	t.Run("running compaction blocks", func(t *testing.T) {
		compact, gc := planCompaction(now, 10, []baseVersion{oldBase, newBase}, false, true)
		require.Nil(t, compact)
		require.Empty(t, gc)
	})
}

func TestIsCompactionJob(t *testing.T) {
	job := &importer.JobInfo{Step: importer.JobStepCompacting}
	require.True(t, isCompactionJob(job))

	job = &importer.JobInfo{Parameters: importer.ImportParameters{Options: map[string]any{"compaction": true}}}
	require.True(t, isCompactionJob(job))

	job = &importer.JobInfo{Parameters: importer.ImportParameters{Options: map[string]any{"compaction": "true"}}}
	require.True(t, isCompactionJob(job))

	job = &importer.JobInfo{Parameters: importer.ImportParameters{Options: map[string]any{"compaction": false}}}
	require.False(t, isCompactionJob(job))
}

func TestRunImportIntoCompactionOncePaused(t *testing.T) {
	setSLOGuardRuntime(importer.SLOGuardGlobalJobID, sloGuardStatePause, 0)
	t.Cleanup(func() {
		setSLOGuardRuntime(importer.SLOGuardGlobalJobID, sloGuardStateNormal, 0)
	})

	err := runImportIntoCompactionOnce(nil, zap.NewNop())
	require.NoError(t, err)
}

func TestResolveTableInfo(t *testing.T) {
	p := parser.New()
	stmt, err := p.ParseOneStmt("create table t (id int primary key)", "", "")
	require.NoError(t, err)
	sctx := utilmock.NewContext()
	tblInfo, err := ddl.MockTableInfo(sctx, stmt.(*ast.CreateTableStmt), 1)
	require.NoError(t, err)
	is := infoschema.MockInfoSchema([]*model.TableInfo{tblInfo})
	state := &tableCompactionState{schemaName: "test", tableName: "t", tableID: tblInfo.ID}
	tbl, dbInfo, ok := resolveTableInfo(is, state)
	require.True(t, ok)
	require.NotNil(t, tbl)
	require.NotNil(t, dbInfo)
	require.Equal(t, ast.NewCIStr("test"), dbInfo.Name)
}

func TestFillBaseVersionFromJobTime(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	store, err := objstore.NewLocalStorage(dir)
	require.NoError(t, err)

	baseID := "base-1"
	require.NoError(t, WriteBaseManifest(ctx, store, BaseManifestPath(baseID), &BaseManifest{BaseID: baseID}))

	job := &importer.JobInfo{
		CreateTime: types.NewTime(types.FromGoTime(time.Date(2026, 2, 1, 12, 0, 0, 0, time.UTC)), mysql.TypeTimestamp, 6),
	}
	base := baseVersion{
		baseID:  baseID,
		baseURI: "local://" + filepath.ToSlash(dir),
		job:     job,
	}
	require.NoError(t, fillBaseVersion(ctx, &base))
	require.False(t, base.createTime.IsZero())
}
