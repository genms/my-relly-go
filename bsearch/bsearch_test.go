package bsearch

import (
	"testing"
)

func TestSlotted(t *testing.T) {
	test := func(data []int, search_value int, expectResult int, expectIndex int) {
		result, index := BinarySearchBy(len(data), func(idx int) int { return data[idx] - search_value })
		if !(result == expectResult && index == expectIndex) {
			t.Fatalf("BinarySearchBy() = %v %v, want %v %v", result, index, expectResult, expectIndex)
		}
	}

	a := []int{1, 2, 3, 5, 8, 13, 21}
	test(a, 1, BINARY_SEARCH_RESULT_HIT, 0)
	test(a, 0, BINARY_SEARCH_RESULT_MISS, 0)
	test(a, 2, BINARY_SEARCH_RESULT_HIT, 1)
	test(a, 8, BINARY_SEARCH_RESULT_HIT, 4)
	test(a, 6, BINARY_SEARCH_RESULT_MISS, 4)
	test(a, 21, BINARY_SEARCH_RESULT_HIT, 6)
	test(a, 22, BINARY_SEARCH_RESULT_MISS, 7)
}
