package bsearch

const (
	BINARY_SEARCH_RESULT_HIT int = iota
	BINARY_SEARCH_RESULT_MISS
)

func BinarySearchBy(size int, f func(int) int) (int, int) {
	left := 0
	right := size
	for left < right {
		mid := left + size/2
		cmp := f(mid)
		if cmp < 0 {
			left = mid + 1
		} else if cmp > 0 {
			right = mid
		} else {
			return BINARY_SEARCH_RESULT_HIT, mid
		}
		size = right - left
	}
	return BINARY_SEARCH_RESULT_MISS, left
}
