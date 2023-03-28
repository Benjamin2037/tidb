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

package proto

import (
	"github.com/pingcap/tidb/domain/infosync"
)

type DataServiceMeta struct {
	DataServiceType 	string `json:"data_service_type"`
	JobID      			string `json:"job_id"`
    TableID    			string `json:"table_id"`
	IndexID    			string `json:"index_id"`
	TaskType   			string `json:"task_type"`
    KeyStart   			[]byte `json:"key_start"`
	KeyEnd     			[]byte `json:"key_end"`
	RTable     			*RoutingTable `json:"routing_table"`
	Servers    			map[string]*infosync.ServerInfo `json:"-"`
	FinishedNodes       []bool `json:"finished_nodes"`
}

type RouteItem struct {
   	KeyStart    string  `json:"key_start"`
    NodeID      string  `json:"node_id"`
}

type RoutingTable struct {
    Items []RouteItem  `json:"route_item_array"`
}