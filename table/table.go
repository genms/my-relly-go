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

type Table struct {
	MetaPageId    disk.PageId
	NumKeyElems   int
	UniqueIndices []UniqueIndex
}

func (t *Table) Create(bufmgr *buffer.BufferPoolManager) error {
	tree, err := btree.CreateBTree(bufmgr)
	if err != nil {
		return err
	}
	t.MetaPageId = tree.MetaPageId
	for i := range t.UniqueIndices {
		t.UniqueIndices[i].Create(bufmgr)
	}
	return nil
}

func (t *Table) Insert(bufmgr *buffer.BufferPoolManager, record [][]byte) error {
	tree := btree.NewBTree(t.MetaPageId)
	key := EncodeTuple(record[:t.NumKeyElems])
	value := EncodeTuple(record[t.NumKeyElems:])
	if err := tree.Insert(bufmgr, key, value); err != nil {
		return err
	}
	for _, uniqueIndex := range t.UniqueIndices {
		err := uniqueIndex.Insert(bufmgr, key, record)
		if err != nil {
			return err
		}
	}
	return nil
}

type UniqueIndex struct {
	MetaPageId disk.PageId
	SKey       []int
}

func (idx *UniqueIndex) Create(bufmgr *buffer.BufferPoolManager) error {
	tree, err := btree.CreateBTree(bufmgr)
	if err != nil {
		return err
	}
	idx.MetaPageId = tree.MetaPageId
	return nil
}

func (idx *UniqueIndex) Insert(bufmgr *buffer.BufferPoolManager, pkey []byte, record [][]byte) error {
	tree := btree.NewBTree(idx.MetaPageId)

	skeyElems := [][]byte{}
	for _, k := range idx.SKey {
		skeyElems = append(skeyElems, record[k])
	}
	skey := EncodeTuple(skeyElems)

	if err := tree.Insert(bufmgr, skey, pkey); err != nil {
		return err
	}
	return nil
}
