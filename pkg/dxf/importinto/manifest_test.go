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
	"errors"
	"path"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/objstore"
	"github.com/pingcap/tidb/pkg/objstore/storeapi"
	"github.com/stretchr/testify/require"
)

func TestBaseManifestReadWrite(t *testing.T) {
	store := objstore.NewMemStorage()
	ctx := context.Background()
	baseID := "base-1"
	manifestPath := BaseManifestPath(baseID)
	now := time.Date(2026, 1, 28, 0, 0, 0, 0, time.UTC)

	in := &BaseManifest{
		TableID:    100,
		BaseID:     baseID,
		BaseURI:    "s3://bucket/path/base-1",
		CreateTime: now,
		RowCount:   10,
		DataBytes:  123,
		IndexBytes: 456,
		Regions: []BaseRegionMeta{
			{
				StartKey:   "00",
				EndKey:     "ff",
				DataFiles:  []string{"data-1"},
				StatFiles:  []string{"stat-1"},
				IndexFiles: []string{"idx-1"},
				Checksum:   "abc",
				KVBytes:    1000,
			},
		},
	}

	require.NoError(t, WriteBaseManifest(ctx, store, manifestPath, in))
	out, err := ReadBaseManifest(ctx, store, manifestPath)
	require.NoError(t, err)
	require.Equal(t, in, out)
}

func TestChangedRegionsReadWrite(t *testing.T) {
	store := objstore.NewMemStorage()
	ctx := context.Background()
	path := ChangedRegionsPath(123)

	in := &ChangedRegionsManifest{
		BaseID: "base-1",
		JobID:  123,
		Regions: []ChangedRegionMeta{
			{StartKey: "00", EndKey: "10", ChangedRows: 2, KVBytes: 20},
			{StartKey: "10", EndKey: "20", ChangedRows: 3, KVBytes: 30},
		},
	}

	require.NoError(t, WriteChangedRegionsManifest(ctx, store, path, in))
	out, err := ReadChangedRegionsManifest(ctx, store, path)
	require.NoError(t, err)
	require.Equal(t, in, out)
}

func TestParseBaseID(t *testing.T) {
	jobID, ok := ParseBaseID("base-123")
	require.True(t, ok)
	require.Equal(t, int64(123), jobID)

	_, ok = ParseBaseID("base-")
	require.False(t, ok)

	_, ok = ParseBaseID("other-123")
	require.False(t, ok)
}

func TestResolveLatestBaseManifest(t *testing.T) {
	store := objstore.NewMemStorage()
	ctx := context.Background()

	_, _, err := ResolveLatestBaseManifest(ctx, store)
	require.ErrorIs(t, err, ErrBaseManifestNotFound)

	base1 := &BaseManifest{TableID: 1, BaseID: "base-1"}
	base2 := &BaseManifest{TableID: 1, BaseID: "base-2"}
	require.NoError(t, WriteBaseManifest(ctx, store, BaseManifestPath(base1.BaseID), base1))
	require.NoError(t, WriteBaseManifest(ctx, store, BaseManifestPath(base2.BaseID), base2))

	id, path, err := ResolveLatestBaseManifest(ctx, store)
	require.NoError(t, err)
	require.Equal(t, "base-2", id)
	require.Equal(t, BaseManifestPath("base-2"), path)
}

func TestRemoveBaseVersion(t *testing.T) {
	store := objstore.NewMemStorage()
	ctx := context.Background()

	require.Error(t, RemoveBaseVersion(ctx, store, ""))
	require.NoError(t, RemoveBaseVersion(ctx, store, "base-empty"))

	baseID := "base-1"
	require.NoError(t, WriteBaseManifest(ctx, store, BaseManifestPath(baseID), &BaseManifest{BaseID: baseID}))
	require.NoError(t, store.WriteFile(ctx, path.Join(baseManifestDirName, baseID, "extra"), []byte("extra")))

	require.NoError(t, RemoveBaseVersion(ctx, store, baseID))
	var files []string
	require.NoError(t, store.WalkDir(ctx, &storeapi.WalkOption{SubDir: path.Join(baseManifestDirName, baseID)}, func(p string, _ int64) error {
		files = append(files, p)
		return nil
	}))
	require.Empty(t, files)
}

type errWalkStorage struct {
	*objstore.MemStorage
}

func (s *errWalkStorage) WalkDir(ctx context.Context, opt *storeapi.WalkOption, fn func(path string, size int64) error) error {
	return errors.New("walk failed")
}

func TestRemoveBaseVersionWalkError(t *testing.T) {
	store := &errWalkStorage{MemStorage: objstore.NewMemStorage()}
	ctx := context.Background()
	err := RemoveBaseVersion(ctx, store, "base-1")
	require.Error(t, err)
	require.Contains(t, err.Error(), "walk failed")
}
