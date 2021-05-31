package btree

import (
	"bytes"
	"encoding/binary"
	"testing"

	"my-relly-go/disk"
)

func TestBranch(t *testing.T) {
	uint64ToBytes := func(n uint64) []byte {
		buf := make([]byte, 8)
		binary.BigEndian.PutUint64(buf, n)
		return buf[:]
	}

	t.Run("Insert", func(t *testing.T) {
		var err error

		data := make([]byte, 100)
		branch := NewBranch(data)

		branch.Initialize(uint64ToBytes(5), disk.PageId(1), disk.PageId(2))
		err = branch.Insert(1, uint64ToBytes(8), disk.PageId(3))
		if err != nil {
			panic(err)
		}
		err = branch.Insert(2, uint64ToBytes(11), disk.PageId(4))
		if err != nil {
			panic(err)
		}

		tests := []struct {
			key    uint64
			pageId disk.PageId
		}{
			{1, 1},
			{5, 3},
			{6, 3},
			{8, 4},
			{10, 4},
			{11, 2},
			{12, 2},
		}
		for _, tt := range tests {
			actual := branch.SearchChild(uint64ToBytes(tt.key))
			if actual != tt.pageId {
				t.Fatalf("branch.SearchChild(%v) = %v, want %v", tt.key, actual, tt.pageId)
			}
		}
	})

	t.Run("Split", func(t *testing.T) {
		var err error

		data := make([]byte, 100)
		branch := NewBranch(data)

		branch.Initialize(uint64ToBytes(5), disk.PageId(1), disk.PageId(2))
		err = branch.Insert(1, uint64ToBytes(8), disk.PageId(3))
		if err != nil {
			panic(err)
		}
		err = branch.Insert(2, uint64ToBytes(11), disk.PageId(4))
		if err != nil {
			panic(err)
		}

		data2 := make([]byte, 100)
		branch2 := NewBranch(data2)
		{
			midKey := branch.SplitInsert(branch2, uint64ToBytes(10), disk.PageId(5))
			expect := uint64ToBytes(8)
			if !bytes.Equal(midKey, expect) {
				t.Fatalf("branch.SplitInsert() = %v, want %v", midKey, expect)
			}
		}
		{
			actual := branch.NumPairs()
			expect := 2
			if actual != expect {
				t.Fatalf("branch.NumPairs() = %v, want %v", actual, expect)
			}
		}
		{
			actual := branch2.NumPairs()
			expect := 1
			if actual != expect {
				t.Fatalf("branch2.NumPairs() = %v, want %v", actual, expect)
			}
		}
		{
			tests := []struct {
				key    uint64
				pageId disk.PageId
			}{
				{1, 1},
				{5, 3},
				{6, 3},
			}
			for _, tt := range tests {
				actual := branch2.SearchChild(uint64ToBytes(tt.key))
				if actual != tt.pageId {
					t.Fatalf("branch2.SearchChild(%v) = %v, want %v", tt.key, actual, tt.pageId)
				}
			}
		}
		{
			tests := []struct {
				key    uint64
				pageId disk.PageId
			}{
				{9, 5},
				{10, 4},
				{11, 2},
				{12, 2},
			}
			for _, tt := range tests {
				actual := branch.SearchChild(uint64ToBytes(tt.key))
				if actual != tt.pageId {
					t.Fatalf("branch.SearchChild(%v) = %v, want %v", tt.key, actual, tt.pageId)
				}
			}
		}
	})
}
