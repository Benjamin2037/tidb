// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package utils

import (
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/br/pkg/utils/utildb"
	"go.uber.org/zap"
)

// MinInt choice smallest integer from its arguments.
func MinInt(x int, xs ...int) int {
	return utildb.MinInt(x, xs...)
}

// MaxInt choice biggest integer from its arguments.
func MaxInt(x int, xs ...int) int {
	return utildb.MaxInt(x, xs...)
}

// ClampInt restrict a value to a certain interval.
func ClampInt(n, min, max int) int {
	if min > max {
		log.Error("clamping integer with min > max", zap.Int("min", min), zap.Int("max", max))
	}

	return MinInt(max, MaxInt(min, n))
}

// MinInt64 choice smallest integer from its arguments.
func MinInt64(x int64, xs ...int64) int64 {
	min := x
	for _, n := range xs {
		if n < min {
			min = n
		}
	}
	return min
}

// NextPowerOfTwo returns the smallest power of two greater than or equal to `i`
// Caller should guarantee that i > 0 and the return value is not overflow.
func NextPowerOfTwo(i int64) int64 {
	return utildb.NextPowerOfTwo(i)
}
