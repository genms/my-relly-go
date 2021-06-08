package table

import (
	"my-relly-go/btree"
	"my-relly-go/buffer"
	"my-relly-go/disk"
)

type SimpleTable struct {
	MetaPageId  disk.PageId
	NumKeyElems int
}

func (t *SimpleTable) Create(bufmgr *buffer.BufferPoolManager) error {
	tree, err := btree.CreateBTree(bufmgr)
	if err != nil {
		return err
	}
	t.MetaPageId = tree.MetaPageId
	return nil
}

func (t *SimpleTable) Insert(bufmgr *buffer.BufferPoolManager, record [][]byte) error {
	tree := btree.NewBTree(t.MetaPageId)
	key := EncodeTuple(record[:t.NumKeyElems])
	value := EncodeTuple(record[t.NumKeyElems:])
	if err := tree.Insert(bufmgr, key, value); err != nil {
		return err
	}
	return nil
}
