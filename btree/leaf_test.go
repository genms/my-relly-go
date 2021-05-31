package btree

import (
	"bytes"
	"testing"

	"my-relly-go/bsearch"
)

func (l *Leaf) SearchPair(key []byte) *Pair {
	result, slotId := l.SearchSlotId(key)
	if result != bsearch.BINARY_SEARCH_RESULT_HIT {
		panic("miss")
	}
	return l.PairAt(slotId)
}

func TestLeaf(t *testing.T) {
	insert := func(leafPage *Leaf, key []byte, value []byte, expectIndex int) {
		result, id := leafPage.SearchSlotId(key)
		if result == bsearch.BINARY_SEARCH_RESULT_HIT {
			t.Fatalf("leafPage.SearchSlotId() unexpected hit")
		}
		if id != expectIndex {
			t.Fatalf("leafPage.SearchSlotId() = %v, want %v", id, expectIndex)
		}
		err := leafPage.Insert(id, key, value)
		if err != nil {
			t.Fatalf("leafPage.Insert(): %v", err)
		}
	}

	pairAtTest := func(leafPage *Leaf, index int, expectKey []byte, expectValue []byte) {
		pair := leafPage.PairAt(index)
		{
			actual := pair.Key
			if !bytes.Equal(actual, expectKey) {
				t.Fatalf("leafPage.PairAt(%d).Key = %v, want %v", index, actual, expectKey)
			}
		}
		{
			actual := pair.Value
			if !bytes.Equal(actual, expectValue) {
				t.Fatalf("leafPage.PairAt(%d).Value = %v, want %v", index, actual, expectValue)
			}
		}
	}

	searchPairTest := func(leafPage *Leaf, tests [][]string) {
		for _, tt := range tests {
			searchResult := leafPage.SearchPair([]byte(tt[0]))
			actual := searchResult.Value
			expect := []byte(tt[1])
			if !bytes.Equal(actual, expect) {
				t.Fatalf("leafPage.SearchPair().Value = %v, want %v", actual, expect)
			}
		}
	}

	t.Run("Insert", func(t *testing.T) {
		pageData := make([]byte, 100)
		leafPage := NewLeaf(pageData)
		leafPage.Initialize()

		insert(leafPage, []byte("deadbeef"), []byte("world"), 0)
		pairAtTest(leafPage, 0, []byte("deadbeef"), []byte("world"))

		insert(leafPage, []byte("facebook"), []byte("!"), 1)
		pairAtTest(leafPage, 0, []byte("deadbeef"), []byte("world"))
		pairAtTest(leafPage, 1, []byte("facebook"), []byte("!"))

		insert(leafPage, []byte("beefdead"), []byte("hello"), 0)
		pairAtTest(leafPage, 0, []byte("beefdead"), []byte("hello"))
		pairAtTest(leafPage, 1, []byte("deadbeef"), []byte("world"))
		pairAtTest(leafPage, 2, []byte("facebook"), []byte("!"))

		searchResult := leafPage.SearchPair([]byte("beefdead"))
		actual := searchResult.Value
		expect := []byte("hello")
		if !bytes.Equal(actual, expect) {
			t.Fatalf("leafPage.SearchPair().Value = %v, want %v", actual, expect)
		}
	})

	t.Run("SplitInsert: to new leaf", func(t *testing.T) {
		pageData := make([]byte, 88)
		leafPage := NewLeaf(pageData)
		leafPage.Initialize()

		insert(leafPage, []byte("deadbeef"), []byte("world"), 0) // 4 + 13 + 6 = 23 bytes
		insert(leafPage, []byte("facebook"), []byte("!"), 1)     // 4 + 9 + 6 = 19 bytes
		insert(leafPage, []byte("hoge"), []byte("fuga"), 2)      // 4 + 8 + 6 = 18 bytes
		{
			result, id := leafPage.SearchSlotId([]byte("beefdead"))
			if result == bsearch.BINARY_SEARCH_RESULT_HIT {
				t.Fatalf("leafPage.SearchSlotId() unexpected hit")
			}
			if id != 0 {
				t.Fatalf("leafPage.SearchSlotId() = %v, want %v", id, 0)
			}
			err := leafPage.Insert(id, []byte("beefdead"), []byte("hello")) // 4 + 13 + 6 = 23 bytes
			if err == nil {
				t.Fatalf("leafPage.Insert(): unexpected success")
			}
		}

		newPageData := make([]byte, 88)
		newLeafPage := NewLeaf(newPageData)
		leafPage.SplitInsert(newLeafPage, []byte("beefdead"), []byte("hello")) // 4 + 13 + 6 = 23 bytes
		searchPairTest(newLeafPage, [][]string{
			{"beefdead", "hello"},
			{"deadbeef", "world"},
		})
		searchPairTest(leafPage, [][]string{
			{"facebook", "!"},
			{"hoge", "fuga"},
		})
	})

	t.Run("SplitInsert: to old leaf", func(t *testing.T) {
		pageData := make([]byte, 88)
		leafPage := NewLeaf(pageData)
		leafPage.Initialize()

		insert(leafPage, []byte("deadbeef"), []byte("world"), 0) // 4 + 13 + 6 = 23 bytes
		insert(leafPage, []byte("facebook"), []byte("!"), 1)     // 4 + 9 + 6 = 19 bytes
		insert(leafPage, []byte("hoge"), []byte("fuga"), 2)      // 4 + 8 + 6 = 18 bytes

		newPageData := make([]byte, 88)
		newLeafPage := NewLeaf(newPageData)
		leafPage.SplitInsert(newLeafPage, []byte("zzzzzzzz"), []byte("hello")) // 4 + 13 + 6 = 23 bytes
		searchPairTest(newLeafPage, [][]string{
			{"deadbeef", "world"},
			{"facebook", "!"},
		})
		searchPairTest(leafPage, [][]string{
			{"hoge", "fuga"},
			{"zzzzzzzz", "hello"},
		})
	})
}
