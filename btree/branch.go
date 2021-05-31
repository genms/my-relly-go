package btree

import (
	"bytes"
	"unsafe"

	"my-relly-go/bsearch"
	"my-relly-go/disk"

	"golang.org/x/xerrors"
)

type BranchHeader struct {
	rightChild disk.PageId
}

type Branch struct {
	header *BranchHeader
	body   *Slotted
}

func NewBranch(bytes []byte) *Branch {
	branch := Branch{}
	headerSize := int(unsafe.Sizeof(*branch.header))
	if headerSize+1 > len(bytes) {
		panic("branch header must be aligned")
	}

	branch.header = (*BranchHeader)(unsafe.Pointer(&bytes[0]))
	branch.body = NewSlotted(bytes[headerSize:])
	return &branch
}

func (b *Branch) NumPairs() int {
	return b.body.NumSlots()
}

func (b *Branch) SearchSlotId(key []byte) (int, int) {
	return bsearch.BinarySearchBy(b.NumPairs(), func(slotId int) int {
		return bytes.Compare(b.PairAt(slotId).Key, key)
	})
}

func (b *Branch) SearchChild(key []byte) disk.PageId {
	childIdx := b.SearchChildIdx(key)
	return b.ChildAt(childIdx)
}

func (b *Branch) SearchChildIdx(key []byte) int {
	switch result, slotId := b.SearchSlotId(key); result {
	case bsearch.BINARY_SEARCH_RESULT_HIT:
		return slotId + 1
	case bsearch.BINARY_SEARCH_RESULT_MISS:
		return slotId
	default:
		panic("unreachable")
	}
}

func (b *Branch) ChildAt(childIdx int) disk.PageId {
	if childIdx == b.NumPairs() {
		return b.header.rightChild
	} else {
		return disk.BytesToPageId(b.PairAt(childIdx).Value)
	}
}

func (b *Branch) PairAt(slotId int) *Pair {
	data := b.body.ReadData(slotId)
	return NewPairFromBytes(data)
}

func (b *Branch) MaxPairSize() int {
	return b.body.Capacity()/2 - int(unsafe.Sizeof(Pointer{}))
}

func (b *Branch) Initialize(key []byte, leftChild disk.PageId, rightChild disk.PageId) {
	b.body.Initialize()
	err := b.Insert(0, key, leftChild)
	if err != nil {
		panic(xerrors.Errorf("new leaf must have space: %v", err))
	}
	b.header.rightChild = rightChild
}

func (b *Branch) FillRightChild() []byte {
	lastId := b.NumPairs() - 1
	pair := b.PairAt(lastId)
	rightChild := disk.BytesToPageId(pair.Value)
	b.body.Remove(lastId)
	b.header.rightChild = rightChild
	return pair.Key
}

func (b *Branch) Insert(slotId int, key []byte, pageId disk.PageId) error {
	pair := Pair{Key: key, Value: disk.PageIdToBytes(pageId)}
	pairBytes := pair.ToBytes()
	if len(pairBytes) > b.MaxPairSize() {
		return ErrTooLongData
	}
	err := b.body.Insert(slotId, len(pairBytes))
	if err != nil {
		return err
	}
	b.body.WriteData(slotId, pairBytes)
	return nil
}

func (b *Branch) isHalfFull() bool {
	return 2*b.body.FreeSpace() < b.body.Capacity()
}

func (b *Branch) SplitInsert(newBranch *Branch, newKey []byte, newPageId disk.PageId) []byte {
	newBranch.body.Initialize()
	for {
		if newBranch.isHalfFull() {
			result, index := b.SearchSlotId(newKey)
			if result == bsearch.BINARY_SEARCH_RESULT_HIT {
				panic("key must be unique")
			}
			err := b.Insert(index, newKey, newPageId)
			if err != nil {
				panic(xerrors.Errorf("old branch must have space: %v", err))
			}
			break
		}
		if bytes.Compare(b.PairAt(0).Key, newKey) < 0 {
			b.Transfer(newBranch)
		} else {
			err := newBranch.Insert(newBranch.NumPairs(), newKey, newPageId)
			if err != nil {
				panic(xerrors.Errorf("new branch must have space: %v", err))
			}
			for !newBranch.isHalfFull() {
				b.Transfer(newBranch)
			}
			break
		}
	}
	return newBranch.FillRightChild()
}

func (b *Branch) Transfer(dest *Branch) {
	nextIndex := dest.NumPairs()
	srcBody := b.body.ReadData(0)
	err := dest.body.Insert(nextIndex, len(srcBody))
	if err != nil {
		panic(xerrors.Errorf("no space in dest branch: %v", err))
	}
	dest.body.WriteData(nextIndex, srcBody)
	b.body.Remove(0)
}
