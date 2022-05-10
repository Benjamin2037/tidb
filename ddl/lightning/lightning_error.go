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
	LERR_CREATE_ENGINE_FAILED  string = "Lightning: Build lightning engine failed, will use kernel index reorg method to backfill the index."
	LERR_CREATE_CONTEX_FAILED  string = "Lightning: Build lightning worker context failed, will use kernel index reorg method to backfill the index."
	LERR_GET_ENGINE_FAILED     string = "Lightning: Use key get engininfo failed."
	LERR_GET_STORAGE_QUOTA     string = "Lightning: Get storage quota err:"
	LERR_GET_SYS_LIMIT_ERR     string = "Lightning: GetSystemRLimit err:"
	
	// Warning message    
	LWAR_ENV_INIT_FAILD        string = "Lightning: initialize environment failed"
	LWAR_BACKEND_NOT_EXIST     string = "Lightning: backend not exist"

	// Info message
	LInfo_ENV_INIT_SUCC        string = "Lightning: Init global lightning backend environment finished."
)