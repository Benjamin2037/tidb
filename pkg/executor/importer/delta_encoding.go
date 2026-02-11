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

	"github.com/pingcap/errors"
)

// BuildColumnBitmap encodes column presence into a bitmap indexed by column offset.
func BuildColumnBitmap(hasValue []bool) []byte {
	if len(hasValue) == 0 {
		return nil
	}
	byteLen := (len(hasValue) + 7) / 8
	buf := make([]byte, byteLen)
	for i, v := range hasValue {
		if !v {
			continue
		}
		buf[i/8] |= 1 << uint(i%8)
	}
	return buf
}

// ColumnInBitmap checks whether column index is marked in bitmap.
func ColumnInBitmap(bitmap []byte, colIdx int) bool {
	if colIdx < 0 {
		return false
	}
	byteIdx := colIdx / 8
	if byteIdx >= len(bitmap) {
		return false
	}
	return bitmap[byteIdx]&(1<<uint(colIdx%8)) != 0
}

// EncodeDeltaRowValue prefixes the row value with a varint bitmap length and bitmap bytes.
func EncodeDeltaRowValue(value []byte, bitmap []byte) []byte {
	varintBuf := make([]byte, binary.MaxVarintLen64)
	n := binary.PutUvarint(varintBuf, uint64(len(bitmap)))
	out := make([]byte, n+len(bitmap)+len(value))
	copy(out, varintBuf[:n])
	copy(out[n:], bitmap)
	copy(out[n+len(bitmap):], value)
	return out
}

// DecodeDeltaRowValue decodes a bitmap (length-prefixed) and the raw row value.
func DecodeDeltaRowValue(value []byte) ([]byte, []byte, error) {
	if len(value) == 0 {
		return nil, nil, errors.New("delta row value is empty")
	}
	bitmapLen, n := binary.Uvarint(value)
	if n <= 0 {
		return nil, nil, errors.New("invalid delta bitmap length")
	}
	total := n + int(bitmapLen)
	if total > len(value) {
		return nil, nil, errors.New("delta bitmap length out of range")
	}
	return value[n:total], value[total:], nil
}
