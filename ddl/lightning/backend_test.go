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
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAdjustMemory(t *testing.T) {
	type TestCase struct {
		name   string
		quota  int64
		lsize  int64
		ensize int64
	}
	tests := []TestCase{
		{"Mem1", 4 * _kb, 256 * _kb, 1 * _mb},
		{"Mem2", 8 * _mb, 256 * _kb, 1 * _mb},
		{"Mem3", 256 * _mb, 8 * _mb, 32 * _mb},
		{"Mem4", 1 * _gb, 32 * _mb, 128 * _mb},
		{"Mem5", 4 * _gb, 128 * _mb, 512 * _mb},
	}
	InitGlobalLightningBackendEnv()
	for _, test := range tests {
		BackCtxMgr.MemRoot.SetMaxMemoryQuota(test.quota)
		cfg, err := generateLightningConfig(BackCtxMgr.MemRoot, 1, false)
		require.NoError(t, err)
		require.Equal(t, test.lsize, int64(cfg.TikvImporter.LocalWriterMemCacheSize))
		require.Equal(t, test.ensize, int64(cfg.TikvImporter.EngineMemCacheSize))
	}
}

func TestLightningBackend(t *testing.T) {
	GlobalEnv.SetMinQuota()
	InitGlobalLightningBackendEnv()
	require.Equal(t, GlobalEnv.IsInited, true)
	ctx := context.Background()
	require.Equal(t, BackCtxMgr.MemRoot.CurrentUsage(), int64(0))
	// Init important variables
	sysVars := obtainImportantVariables()
	jobID := int64(1)
	cfg, err := generateLightningConfig(BackCtxMgr.MemRoot, jobID, false)
	require.NoError(t, err)
	BackCtxMgr.Store(jobID, newBackendContext(ctx, jobID, nil, cfg, sysVars, BackCtxMgr.MemRoot))
	require.NoError(t, err)

	// Memory allocate failed
	BackCtxMgr.MemRoot.SetMaxMemoryQuota(BackCtxMgr.MemRoot.CurrentUsage())
	err = BackCtxMgr.MemRoot.TryConsume(1 * _gb)
	require.Error(t, err)

	// variable test
	bc, isEnable := BackCtxMgr.Load(100)
	require.Equal(t, false, isEnable)

	jobID = 2
	BackCtxMgr.Store(jobID, newBackendContext(ctx, jobID, nil, cfg, sysVars, BackCtxMgr.MemRoot))
	bc, isEnable = BackCtxMgr.Load(jobID)
	needRestore := bc.NeedRestore()
	require.Equal(t, true, isEnable)
	require.Equal(t, false, needRestore)

	bc.SetNeedRestore(true)
	bc, isEnable = BackCtxMgr.Load(jobID)
	needRestore = bc.NeedRestore()
	require.Equal(t, true, isEnable)
	require.Equal(t, true, needRestore)
}
