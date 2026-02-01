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
	"encoding/json"
	"path"
	"strconv"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/objstore/storeapi"
)

const (
	baseManifestName     = "manifest.json"
	changedRegionsName   = "changed_regions.json"
	baseManifestDirName  = "base"
	deltaManifestDirName = "delta"
)

// BaseManifest records the base version metadata stored on external storage.
type BaseManifest struct {
	TableID    int64            `json:"table_id"`
	BaseID     string           `json:"base_id"`
	BaseURI    string           `json:"base_uri"`
	CreateTime time.Time        `json:"create_time"`
	RowCount   uint64           `json:"row_count"`
	DataBytes  uint64           `json:"data_bytes"`
	IndexBytes uint64           `json:"index_bytes"`
	Regions    []BaseRegionMeta `json:"regions"`
}

// BaseRegionMeta describes the sorted KV files for a single region range.
type BaseRegionMeta struct {
	StartKey       string   `json:"start_key"`
	EndKey         string   `json:"end_key"`
	DataFiles      []string `json:"data_files"`
	StatFiles      []string `json:"stat_files"`
	IndexFiles     []string `json:"index_files"`
	IndexStatFiles []string `json:"index_stat_files,omitempty"`
	Checksum       string   `json:"checksum,omitempty"`
	KVBytes        uint64   `json:"kv_bytes,omitempty"`
}

// ChangedRegionsManifest records the changed regions after a delta job.
type ChangedRegionsManifest struct {
	BaseID  string              `json:"base_id"`
	JobID   int64               `json:"job_id"`
	Regions []ChangedRegionMeta `json:"regions"`
}

// ChangedRegionMeta describes a changed region range for ingest.
type ChangedRegionMeta struct {
	StartKey    string `json:"start_key"`
	EndKey      string `json:"end_key"`
	ChangedRows uint64 `json:"changed_rows,omitempty"`
	KVBytes     uint64 `json:"kv_bytes,omitempty"`
}

// BaseManifestPath returns the default external storage path for a base manifest.
func BaseManifestPath(baseID string) string {
	return path.Join(baseManifestDirName, baseID, baseManifestName)
}

// ChangedRegionsPath returns the default external storage path for a delta changed regions manifest.
func ChangedRegionsPath(jobID int64) string {
	return path.Join(deltaManifestDirName, strconv.FormatInt(jobID, 10), changedRegionsName)
}

// WriteBaseManifest writes base manifest JSON to external storage.
func WriteBaseManifest(ctx context.Context, store storeapi.Storage, manifestPath string, m *BaseManifest) error {
	if manifestPath == "" {
		return errors.New("base manifest path is empty")
	}
	data, err := json.Marshal(m)
	if err != nil {
		return errors.Trace(err)
	}
	return store.WriteFile(ctx, manifestPath, data)
}

// ReadBaseManifest reads base manifest JSON from external storage.
func ReadBaseManifest(ctx context.Context, store storeapi.Storage, manifestPath string) (*BaseManifest, error) {
	if manifestPath == "" {
		return nil, errors.New("base manifest path is empty")
	}
	data, err := store.ReadFile(ctx, manifestPath)
	if err != nil {
		return nil, errors.Trace(err)
	}
	var m BaseManifest
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, errors.Trace(err)
	}
	return &m, nil
}

// WriteChangedRegionsManifest writes changed regions manifest JSON to external storage.
func WriteChangedRegionsManifest(ctx context.Context, store storeapi.Storage, manifestPath string, m *ChangedRegionsManifest) error {
	if manifestPath == "" {
		return errors.New("changed regions manifest path is empty")
	}
	data, err := json.Marshal(m)
	if err != nil {
		return errors.Trace(err)
	}
	return store.WriteFile(ctx, manifestPath, data)
}

// ReadChangedRegionsManifest reads changed regions manifest JSON from external storage.
func ReadChangedRegionsManifest(ctx context.Context, store storeapi.Storage, manifestPath string) (*ChangedRegionsManifest, error) {
	if manifestPath == "" {
		return nil, errors.New("changed regions manifest path is empty")
	}
	data, err := store.ReadFile(ctx, manifestPath)
	if err != nil {
		return nil, errors.Trace(err)
	}
	var m ChangedRegionsManifest
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, errors.Trace(err)
	}
	return &m, nil
}
