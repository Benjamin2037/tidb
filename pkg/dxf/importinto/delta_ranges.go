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
	"bytes"
	"encoding/hex"

	"github.com/pingcap/errors"
)

func decodeHexKey(hexStr string) ([]byte, error) {
	if hexStr == "" {
		return nil, nil
	}
	b, err := hex.DecodeString(hexStr)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return b, nil
}

// rangesOverlap returns true when [startA, endA) intersects [startB, endB).
// Empty end means unbounded.
func rangesOverlap(startA, endA, startB, endB []byte) bool {
	if len(endA) > 0 && bytes.Compare(endA, startB) <= 0 {
		return false
	}
	if len(endB) > 0 && bytes.Compare(endB, startA) <= 0 {
		return false
	}
	return true
}

// withinRange checks whether key is in [start, end). Empty end means unbounded.
func withinRange(key, start, end []byte) bool {
	if len(start) > 0 && bytes.Compare(key, start) < 0 {
		return false
	}
	if len(end) > 0 && bytes.Compare(key, end) >= 0 {
		return false
	}
	return true
}
