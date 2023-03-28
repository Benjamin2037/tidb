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
	"encoding/binary"

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

func BuildRoutingTable(jobID, tableID, indexID, TaskType, DataServiceType string, keyStart, keyEnd []byte) (*proto.DataServiceMeta, error) {
	var (
		DSMeta *proto.DataServiceMeta
		err error
	)
	DSMeta = &proto.DataServiceMeta{
		JobID: jobID,
		TableID: tableID,
		IndexID: indexID,
		TaskType: TaskType,
		KeyStart: keyStart,
		KeyEnd: keyEnd,
	}
	return  DSMeta, err
} 

// SplitRange used to split a range of key by input number of compute node.
func SplitRange(startKey, endKey []byte, numNodes int) [][]byte {
    var result [][]byte
    rangeSize := int(binary.BigEndian.Uint64(endKey)) - int(binary.BigEndian.Uint64(startKey))
    chunkSize := rangeSize / numNodes

    for i := 0; i < numNodes; i++ {
        var chunkStart, chunkEnd []byte

        if i == 0 {
            chunkStart = startKey
        } else {
            chunkStart = make([]byte, len(startKey))
            copy(chunkStart, startKey)
            binary.BigEndian.PutUint64(chunkStart, binary.BigEndian.Uint64(chunkStart)+uint64(chunkSize*i))
        }

        if i == numNodes-1 {
            chunkEnd = endKey
        } else {
            chunkEnd = make([]byte, len(endKey))
            copy(chunkEnd, startKey)
            binary.BigEndian.PutUint64(chunkEnd, binary.BigEndian.Uint64(chunkEnd)+uint64(chunkSize*(i+1)))
        }

        result = append(result, chunkStart)
        result = append(result, chunkEnd)
    }

    return result
}
