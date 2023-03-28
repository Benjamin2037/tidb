// Copyright 2023 PingCAP, Inc.
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

package globaldataservice

import (
	"github.com/pingcap/tidb/disttask/framework/proto"
)

type RedistributeDataService interface {
	// InitRedistributeDataServiceEnv is used by scheduler to initialize the global data service in local TiDB node.
	InitRedistributeDataServiceEnv(DSMeta *proto.DataServiceMeta) error
	// FlushData will send out the data to remote target.
	FlushData()([]string, error)
	// FinishDataService used to mark global data service finished for specific TiDB node. 
	FinishDataService(taskKey string, nodeID string) 
	// CleanupDataServiceEnv 
	CleanupDataServiceEnv()
}

func BuildRoutingTable(jobID, tableID, indexID, TaskType, keyStart, keyEnd, DataServiceType string) (*proto.DataServiceMeta, error) {
	var (
		DSMeta *proto.DataServiceMeta
		err error
	)
	return  DSMeta, err
} 


