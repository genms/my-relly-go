package btree

import (
	"my-relly-go/bsearch"
	"my-relly-go/buffer"
	"my-relly-go/disk"

	"golang.org/x/xerrors"
	"google.golang.org/protobuf/proto"
)

var (
	ErrDuplicateKey  = xerrors.New("duplicate key")
	ErrEndOfIterator = xerrors.New("end of iterator")
)

func NewPairFromBytes(buf []byte) *Pair {
	pair := &Pair{}
	if err := proto.Unmarshal(buf, pair); err != nil {
		panic(err)
	}
	return pair
}

func (p *Pair) ToBytes() []byte {
	buf, err := proto.Marshal(p)
	if err != nil {
		panic(err)
	}
	return buf
}

type SearchMode interface {
	childPageId(branch *Branch) disk.PageId
	tupleSlotId(leaf *Leaf) (int, int)
}

type SearchModeStart struct {
}

func (s *SearchModeStart) childPageId(branch *Branch) disk.PageId {
	return branch.ChildAt(0)
}

func (s *SearchModeStart) tupleSlotId(leaf *Leaf) (int, int) {
	return bsearch.BINARY_SEARCH_RESULT_MISS, 0
}

type SearchModeKey struct {
	Key []byte
}

func (s *SearchModeKey) childPageId(branch *Branch) disk.PageId {
	return branch.SearchChild(s.Key)
}

func (s *SearchModeKey) tupleSlotId(leaf *Leaf) (int, int) {
	return leaf.SearchSlotId(s.Key)
}

type BTree struct {
	MetaPageId disk.PageId
}

func CreateBTree(bufmgr *buffer.BufferPoolManager) (*BTree, error) {
	metaBuffer, err := bufmgr.CreatePage()
	if err != nil {
		return nil, err
	}
	defer bufmgr.FinishUsingPage(metaBuffer)
	meta := NewMeta(metaBuffer.Page[:])

	rootBuffer, err := bufmgr.CreatePage()
	if err != nil {
		return nil, err
	}
	defer bufmgr.FinishUsingPage(rootBuffer)
	root := NewNode(rootBuffer.Page[:])
	root.InitializeAsLeaf()

	leaf := NewLeaf(root.body)
	leaf.Initialize()

	meta.header.rootPageId = rootBuffer.PageId
	return NewBTree(metaBuffer.PageId), nil
}

func NewBTree(metaPageId disk.PageId) *BTree {
	return &BTree{metaPageId}
}

func (t *BTree) ReadMetaAppArea(bufmgr *buffer.BufferPoolManager) ([]byte, error) {
	metaBuffer, err := t.fetchMetaPage(bufmgr)
	if err != nil {
		return nil, err
	}
	defer bufmgr.FinishUsingPage(metaBuffer)

	meta := NewMeta(metaBuffer.Page[:])
	data := make([]byte, len(meta.appArea))
	copy(data, meta.appArea)
	return data, nil
}

func (t *BTree) WriteMetaAppArea(bufmgr *buffer.BufferPoolManager, data []byte) error {
	metaBuffer, err := t.fetchMetaPage(bufmgr)
	if err != nil {
		return err
	}
	defer bufmgr.FinishUsingPage(metaBuffer)

	meta := NewMeta(metaBuffer.Page[:])
	if len(meta.appArea) < len(data) {
		return ErrTooLongData
	}
	copy(meta.appArea, data)
	return nil
}

func (t *BTree) fetchMetaPage(bufmgr *buffer.BufferPoolManager) (*buffer.Buffer, error) {
	metaBuffer, err := bufmgr.FetchPage(t.MetaPageId)
	if err != nil {
		return nil, err
	}
	return metaBuffer, nil
}

func (t *BTree) fetchRootPage(bufmgr *buffer.BufferPoolManager) (*buffer.Buffer, error) {
	metaBuffer, err := bufmgr.FetchPage(t.MetaPageId)
	if err != nil {
		return nil, err
	}
	defer bufmgr.FinishUsingPage(metaBuffer)

	meta := NewMeta(metaBuffer.Page[:])
	rootPageId := meta.header.rootPageId
	rootBuffer, err := bufmgr.FetchPage(rootPageId)
	if err != nil {
		return nil, err
	}
	return rootBuffer, nil
}

func (t *BTree) searchInternal(bufmgr *buffer.BufferPoolManager, nodeBuffer *buffer.Buffer, searchMode SearchMode) (*BTreeIter, error) {
	node := NewNode(nodeBuffer.Page[:])
	switch node.header.NodeTypeString() {
	case NODE_TYPE_LEAF:
		leaf := NewLeaf(node.body)
		_, slotId := searchMode.tupleSlotId(leaf)
		node = nil
		return &BTreeIter{nodeBuffer, slotId}, nil
	case NODE_TYPE_BRANCH:
		branch := NewBranch(node.body)
		childPageId := searchMode.childPageId(branch)
		node = nil
		bufmgr.FinishUsingPage(nodeBuffer)
		childNodePage, err := bufmgr.FetchPage(childPageId)
		if err != nil {
			return nil, err
		}
		return t.searchInternal(bufmgr, childNodePage, searchMode)
	default:
		panic("unreachable")
	}
}

func (t *BTree) Search(bufmgr *buffer.BufferPoolManager, searchMode SearchMode) (*BTreeIter, error) {
	rootPage, err := t.fetchRootPage(bufmgr)
	if err != nil {
		return nil, err
	}
	return t.searchInternal(bufmgr, rootPage, searchMode)
}

func (t *BTree) insertInternal(bufmgr *buffer.BufferPoolManager, buffer *buffer.Buffer, key []byte, value []byte) (bool, []byte, disk.PageId, error) {
	node := NewNode(buffer.Page[:])
	switch node.header.NodeTypeString() {
	case NODE_TYPE_LEAF:
		leaf := NewLeaf(node.body)
		result, slotId := leaf.SearchSlotId(key)
		if result == bsearch.BINARY_SEARCH_RESULT_HIT {
			return false, nil, disk.INVALID_PAGE_ID, ErrDuplicateKey
		}
		if err := leaf.Insert(slotId, key, value); err == nil {
			buffer.IsDirty = true
			return false, nil, disk.INVALID_PAGE_ID, nil
		} else {
			// overflowした場合
			// 新しいleafのBufferを作成
			newLeafBuffer, err := bufmgr.CreatePage()
			if err != nil {
				return false, nil, disk.INVALID_PAGE_ID, err
			}
			defer bufmgr.FinishUsingPage(newLeafBuffer)

			// leaf.prevLeafとleafの間に入れる
			prevLeafPageId, err := leaf.PrevPageId()
			if !xerrors.Is(err, disk.ErrInvalidPageId) {
				prevLeafBuffer, err := bufmgr.FetchPage(prevLeafPageId)
				if err != nil {
					return false, nil, disk.INVALID_PAGE_ID, err
				}
				defer bufmgr.FinishUsingPage(prevLeafBuffer)

				node := NewNode(prevLeafBuffer.Page[:])
				prefLeaf := NewLeaf(node.body)
				prefLeaf.SetNextPageId(newLeafBuffer.PageId)
				prevLeafBuffer.IsDirty = true
			}
			leaf.SetPrevPageId(newLeafBuffer.PageId)

			// 新しいleafを初期化
			// leafと新しいleafにSplitInsert
			newLeafNode := NewNode(newLeafBuffer.Page[:])
			newLeafNode.InitializeAsLeaf()
			newLeaf := NewLeaf(newLeafNode.body)
			newLeaf.Initialize()
			overflowKey := leaf.SplitInsert(newLeaf, key, value)
			newLeaf.SetNextPageId(buffer.PageId)
			newLeaf.SetPrevPageId(prevLeafPageId)
			buffer.IsDirty = true
			return true, overflowKey, newLeafBuffer.PageId, nil
		}

	case NODE_TYPE_BRANCH:
		branch := NewBranch(node.body)
		childIdx := branch.SearchChildIdx(key)
		childPageId := branch.ChildAt(childIdx)
		childNodeBuffer, err := bufmgr.FetchPage(childPageId)
		if err != nil {
			return false, nil, disk.INVALID_PAGE_ID, err
		}
		defer bufmgr.FinishUsingPage(childNodeBuffer)

		overflow, overflowKeyFromChild, overflowChildPageId, err := t.insertInternal(bufmgr, childNodeBuffer, key, value)
		if err != nil {
			return false, nil, disk.INVALID_PAGE_ID, err
		}
		if overflow {
			// overflowした場合
			// branchにInsert
			if err := branch.Insert(childIdx, overflowKeyFromChild, overflowChildPageId); err == nil {
				buffer.IsDirty = true
				return false, nil, disk.INVALID_PAGE_ID, nil
			} else {
				// それも入りきらなかった場合
				// 新しいbranchを作成し、SplitInsert
				newBranchBuffer, err := bufmgr.CreatePage()
				if err != nil {
					return false, nil, disk.INVALID_PAGE_ID, err
				}
				defer bufmgr.FinishUsingPage(newBranchBuffer)

				newBranchNode := NewNode(newBranchBuffer.Page[:])
				newBranchNode.InitializeAsBranch()
				newBranch := NewBranch(newBranchNode.body)
				overflowKey := branch.SplitInsert(newBranch, overflowKeyFromChild, overflowChildPageId)
				buffer.IsDirty = true
				newBranchBuffer.IsDirty = true
				return true, overflowKey, newBranchBuffer.PageId, nil
			}
		} else {
			return false, nil, disk.INVALID_PAGE_ID, nil
		}

	default:
		panic("unreachable")
	}
}

func (t *BTree) Insert(bufmgr *buffer.BufferPoolManager, key []byte, value []byte) error {
	metaBuffer, err := bufmgr.FetchPage(t.MetaPageId)
	if err != nil {
		return err
	}
	defer bufmgr.FinishUsingPage(metaBuffer)
	meta := NewMeta(metaBuffer.Page[:])

	rootPageId := meta.header.rootPageId
	rootBuffer, err := bufmgr.FetchPage(rootPageId)
	if err != nil {
		return err
	}
	defer bufmgr.FinishUsingPage(rootBuffer)

	overflow, key, childPageId, err := t.insertInternal(bufmgr, rootBuffer, key, value)
	if err != nil {
		return err
	}
	if overflow {
		// overflowした場合
		// rootの下に新しいbranchを作成
		newRootBuffer, err := bufmgr.CreatePage()
		if err != nil {
			return err
		}
		defer bufmgr.FinishUsingPage(newRootBuffer)

		node := NewNode(newRootBuffer.Page[:])
		node.InitializeAsBranch()
		branch := NewBranch(node.body)
		branch.Initialize(key, childPageId, rootPageId)
		meta.header.rootPageId = newRootBuffer.PageId
		metaBuffer.IsDirty = true
	}
	return nil
}

type BTreeIter struct {
	buffer *buffer.Buffer
	slotId int
}

func (it *BTreeIter) Get() ([]byte, []byte, error) {
	leafNode := NewNode(it.buffer.Page[:])
	leaf := NewLeaf(leafNode.body)
	if it.slotId < leaf.NumPairs() {
		pair := leaf.PairAt(it.slotId)
		return pair.Key, pair.Value, nil
	}
	return nil, nil, ErrEndOfIterator
}

func (it *BTreeIter) Next(bufmgr *buffer.BufferPoolManager) ([]byte, []byte, error) {
	key, value, err := it.Get()
	if err != nil {
		return nil, nil, err
	}

	it.slotId++
	leafNode := NewNode(it.buffer.Page[:])
	leaf := NewLeaf(leafNode.body)
	if it.slotId < leaf.NumPairs() {
		return key, value, nil
	}
	nextPageId, err := leaf.NextPageId()
	if !xerrors.Is(err, disk.ErrInvalidPageId) {
		bufmgr.FinishUsingPage(it.buffer)
		it.buffer, err = bufmgr.FetchPage(nextPageId)
		if err != nil {
			return nil, nil, err
		}
		it.slotId = 0
	}
	return key, value, nil
}

func (it *BTreeIter) Finish(bufmgr *buffer.BufferPoolManager) {
	bufmgr.FinishUsingPage(it.buffer)
}
