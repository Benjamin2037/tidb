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

package importer

import (
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBuildColumnBitmapAndLookup(t *testing.T) {
	// Indices set: 1, 3, 7, 8 -> 0b10001010, 0b00000001.
	hasValue := []bool{false, true, false, true, false, false, false, true, true}
	bitmap := BuildColumnBitmap(hasValue)
	require.Equal(t, []byte{0x8A, 0x01}, bitmap)

	require.False(t, ColumnInBitmap(bitmap, -1))
	require.True(t, ColumnInBitmap(bitmap, 1))
	require.True(t, ColumnInBitmap(bitmap, 3))
	require.True(t, ColumnInBitmap(bitmap, 7))
	require.True(t, ColumnInBitmap(bitmap, 8))
	require.False(t, ColumnInBitmap(bitmap, 0))
	require.False(t, ColumnInBitmap(bitmap, 2))
	require.False(t, ColumnInBitmap(bitmap, 9))
}

func TestEncodeDecodeDeltaRowValue(t *testing.T) {
	value := []byte("row-value")
	bitmap := []byte{0xAA, 0x55}
	encoded := EncodeDeltaRowValue(value, bitmap)

	gotBitmap, gotValue, err := DecodeDeltaRowValue(encoded)
	require.NoError(t, err)
	require.Equal(t, bitmap, gotBitmap)
	require.Equal(t, value, gotValue)
}

func TestDecodeDeltaRowValueErrors(t *testing.T) {
	_, _, err := DecodeDeltaRowValue(nil)
	require.Error(t, err)

	// Invalid varint: continuation bit set without following bytes.
	_, _, err = DecodeDeltaRowValue([]byte{0x80})
	require.Error(t, err)

	// Declared bitmap length is larger than the available bytes.
	buf := make([]byte, binary.MaxVarintLen64)
	n := binary.PutUvarint(buf, 10)
	encoded := append(buf[:n], []byte{0x01, 0x02}...)
	_, _, err = DecodeDeltaRowValue(encoded)
	require.Error(t, err)
}
