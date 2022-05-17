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

const (
	// Error message
	LERR_ALLOC_MEM_FAILED      string = "Lightning: Alloc memory failed."
	LERR_OUT_OF_MAX_MEM        string = "Lightning: Memory is used up for Lightning add index."
	LERR_NO_MEM_TYPE           string = "Lightning: Unking structure for Lightning add index."
	LERR_CREATE_BACKEND_FAILED string = "Lightning: Build lightning backend failed, will use kernel index reorg method to backfill the index."
	LERR_GET_BACKEND_FAILED    string = "Lightning: Get lightning backend failed"
	LERR_CREATE_ENGINE_FAILED  string = "Lightning: Build lightning engine failed, will use kernel index reorg method to backfill the index."
	LERR_CREATE_CONTEX_FAILED  string = "Lightning: Build lightning worker context failed, will use kernel index reorg method to backfill the index."
	LERR_GET_ENGINE_FAILED     string = "Lightning: Use key get engininfo failed."
	LERR_GET_STORAGE_QUOTA     string = "Lightning: Get storage quota err,"
	LERR_GET_SYS_LIMIT_ERR     string = "Lightning: GetSystemRLimit err,"
	LERR_CLOSE_ENGINE_ERR      string = "Lightning: Close engine err,"
	LERR_INGEST_DATA_ERR       string = "Lightning: Ingest data into TiKV err,"
	LERR_LOCAL_DUP_CHECK_ERR   string = "Lightning: Locale duplicate check err,"
	LERR_LOCAL_DUP_EXIST_ERR   string = "Lightning: Locale duplicate index key exist, "
	LERR_REMOTE_DUP_CHECK_ERR  string = "Lightning: Remote duplicate check err,"
	LERR_REMOTE_DUP_EXIST_ERR  string = "Lightning: Remote duplicate index key exist,"
	// Warning message    
	LWAR_ENV_INIT_FAILD        string = "Lightning: initialize environment failed"
	LWAR_BACKEND_NOT_EXIST     string = "Lightning: backend not exist"
	LWAR_CONFIG_ERROR          string = "Lightning: build config for backend failed"
	LWAR_GEN_MEM_LIMIT         string = "Lightning: Generate memory max limitation,"
	// Info message
	LInfo_ENV_INIT_SUCC        string = "Lightning: Init global lightning backend environment finished."
	LInfo_SORTED_DIR           string = "Lightning: The lightning sorted dir:."
	LINFO_CREATE_BACKEND       string = "Lightning: Create one backend for job."
	LINFO_CLOSE_BACKEND        string = "Lightning: Close one backend for job."
	LINFO_OPEN_ENGINE          string = "Lightning: Open one engine for index reorg"
	LINFO_CLEANUP_ENGINE       string = "Lightning: CleanUp one engine for index reorg"
	LINFO_CREATE_WRITER        string = "Lightning: Create one local Writer for Index reorg task"
	LINFO_CLOSE_ENGINE         string = "Lightning: Flush all writer and get closed engine"
	LINFO_LOCAL_DUPL_CHECK     string = "Lightning: Start Local duplicate check"
	LINFO_REMOTE_DUPL_CHECK    string = "Lightning: Start remote duplicate check"
	LINFO_START_TO_IMPORT      string = "Lightning: Start to import data"
	LINFO_GEN_MEM_LIMIT        string = "Lightning: Generate memory max limitation,"
	LINFO_CHG_MEM_SETTING      string = "Lightning: Change memory setting for lightning,"
	LINFO_INIT_MEM_SETTING     string = "Lightning: Initial memory setting for lightning,"
	LINFO_ENGINE_DELETE        string = "Lightning: Delete one engine from engine manager cache,"
)