package btree

import (
	"bytes"
	"unsafe"

	"my-relly-go/bsearch"
	"my-relly-go/disk"

	"golang.org/x/xerrors"
)

var (
	ErrTooLongData = xerrors.New("too long data")
)

type LeafHeader struct {
	prevPageId disk.PageId
	nextPageId disk.PageId
}

type Leaf struct {
	header *LeafHeader
	body   *Slotted
}

func NewLeaf(bytes []byte) *Leaf {
	leaf := Leaf{}
	headerSize := int(unsafe.Sizeof(*leaf.header))
	if headerSize+1 > len(bytes) {
		panic("leaf header must be aligned")
	}

	leaf.header = (*LeafHeader)(unsafe.Pointer(&bytes[0]))
	leaf.body = NewSlotted(bytes[headerSize:])
	return &leaf
}

func (l *Leaf) PrevPageId() (disk.PageId, error) {
	pageId, err := l.header.prevPageId.Valid()
	if err != nil {
		return disk.INVALID_PAGE_ID, err
	}
	return pageId, nil
}

func (l *Leaf) NextPageId() (disk.PageId, error) {
	pageId, err := l.header.nextPageId.Valid()
	if err != nil {
		return disk.INVALID_PAGE_ID, err
	}
	return pageId, nil
}

func (l *Leaf) NumPairs() int {
	return l.body.NumSlots()
}

func (l *Leaf) SearchSlotId(key []byte) (int, int) {
	return bsearch.BinarySearchBy(l.NumPairs(), func(slotId int) int {
		return bytes.Compare(l.PairAt(slotId).Key, key)
	})
}

func (l *Leaf) PairAt(slotId int) *Pair {
	data := l.body.ReadData(slotId)
	return NewPairFromBytes(data)
}

func (l *Leaf) MaxPairSize() int {
	return l.body.Capacity()/2 - int(unsafe.Sizeof(Pointer{}))
}

func (l *Leaf) Initialize() {
	l.header.prevPageId = disk.INVALID_PAGE_ID
	l.header.nextPageId = disk.INVALID_PAGE_ID
	l.body.Initialize()
}

func (l *Leaf) SetPrevPageId(prevPageId disk.PageId) {
	l.header.prevPageId = prevPageId
}

func (l *Leaf) SetNextPageId(nextPageId disk.PageId) {
	l.header.nextPageId = nextPageId
}

func (l *Leaf) Insert(slotId int, key []byte, value []byte) error {
	pair := Pair{Key: key, Value: value}
	pairBytes := pair.ToBytes()
	if len(pairBytes) > l.MaxPairSize() {
		return ErrTooLongData
	}
	err := l.body.Insert(slotId, len(pairBytes))
	if err != nil {
		return err
	}
	l.body.WriteData(slotId, pairBytes)
	return nil
}

func (l *Leaf) isHalfFull() bool {
	return 2*l.body.FreeSpace() < l.body.Capacity()
}

func (l *Leaf) SplitInsert(newLeaf *Leaf, newKey []byte, newValue []byte) []byte {
	newLeaf.Initialize()
	for {
		if newLeaf.isHalfFull() {
			result, index := l.SearchSlotId(newKey)
			if result == bsearch.BINARY_SEARCH_RESULT_HIT {
				panic("key must be unique")
			}
			err := l.Insert(index, newKey, newValue)
			if err != nil {
				panic(xerrors.Errorf("old leaf must have space: %v", err))
			}
			break
		}
		if bytes.Compare(l.PairAt(0).Key, newKey) < 0 {
			l.Transfer(newLeaf)
		} else {
			err := newLeaf.Insert(newLeaf.NumPairs(), newKey, newValue)
			if err != nil {
				panic(xerrors.Errorf("new leaf must have space: %v", err))
			}
			for !newLeaf.isHalfFull() {
				l.Transfer(newLeaf)
			}
			break
		}
	}
	return l.PairAt(0).Key
}

func (l *Leaf) Transfer(dest *Leaf) {
	nextIndex := dest.NumPairs()
	srcBody := l.body.ReadData(0)
	err := dest.body.Insert(nextIndex, len(srcBody))
	if err != nil {
		panic(err)
	}
	dest.body.WriteData(nextIndex, srcBody)
	l.body.Remove(0)
}
