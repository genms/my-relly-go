package bsearch

const (
	BINARY_SEARCH_RESULT_HIT int = iota
	BINARY_SEARCH_RESULT_MISS
)

/*
 * This is originated in Rust core library:
 * https://github.com/rust-lang/rust/blob/b01026de465d5a5ef51e32c1012c43927d2a111c/library/core/src/slice/mod.rs#L2186
 *
 * Permission is hereby granted, free of charge, to any
 * person obtaining a copy of this software and associated
 * documentation files (the "Software"), to deal in the
 * Software without restriction, including without
 * limitation the rights to use, copy, modify, merge,
 * publish, distribute, sublicense, and/or sell copies of
 * the Software, and to permit persons to whom the Software
 * is furnished to do so, subject to the following
 * conditions:

 * The above copyright notice and this permission notice
 * shall be included in all copies or substantial portions
 * of the Software.

 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF
 * ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED
 * TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A
 * PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT
 * SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY
 * CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
 * OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR
 * IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
 * DEALINGS IN THE SOFTWARE.
 */
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
