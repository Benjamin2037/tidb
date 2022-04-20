package utildb

// MaxInt choice biggest integer from its arguments.
func MaxInt(x int, xs ...int) int {
	max := x
	for _, n := range xs {
		if n > max {
			max = n
		}
	}
	return max
}

// NextPowerOfTwo returns the smallest power of two greater than or equal to `i`
// Caller should guarantee that i > 0 and the return value is not overflow.
func NextPowerOfTwo(i int64) int64 {
	if i&(i-1) == 0 {
		return i
	}
	i *= 2
	for i&(i-1) != 0 {
		i &= i - 1
	}
	return i
}

// MinInt choice smallest integer from its arguments.
func MinInt(x int, xs ...int) int {
	min := x
	for _, n := range xs {
		if n < min {
			min = n
		}
	}
	return min
}
