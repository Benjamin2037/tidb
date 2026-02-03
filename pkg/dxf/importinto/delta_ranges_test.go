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
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDecodeHexKey(t *testing.T) {
	key, err := decodeHexKey("00ff")
	require.NoError(t, err)
	require.Equal(t, []byte{0x00, 0xff}, key)

	key, err = decodeHexKey("")
	require.NoError(t, err)
	require.Nil(t, key)

	_, err = decodeHexKey("zz")
	require.Error(t, err)
}

func TestRangesOverlap(t *testing.T) {
	aStart := []byte{0x00}
	aEnd := []byte{0x10}
	bStart := []byte{0x10}
	bEnd := []byte{0x20}
	require.False(t, rangesOverlap(aStart, aEnd, bStart, bEnd)) // touching boundary

	bStart = []byte{0x05}
	require.True(t, rangesOverlap(aStart, aEnd, bStart, bEnd))

	// Unbounded end overlaps.
	require.True(t, rangesOverlap(aStart, nil, bStart, bEnd))
}

func TestWithinRange(t *testing.T) {
	start := []byte{0x10}
	end := []byte{0x20}
	require.True(t, withinRange([]byte{0x10}, start, end))
	require.True(t, withinRange([]byte{0x1f}, start, end))
	require.False(t, withinRange([]byte{0x20}, start, end)) // end is exclusive
	require.False(t, withinRange([]byte{0x0f}, start, end))

	require.True(t, withinRange([]byte{0x10}, start, nil)) // unbounded end
}
