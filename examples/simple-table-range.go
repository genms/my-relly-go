package main

import (
	"my-relly-go/btree"
	"my-relly-go/buffer"
	"my-relly-go/disk"
	"my-relly-go/table"
)

func SimpleTableRange() {
	diskManager, err := disk.OpenDiskManager("simple.rly")
	if err != nil {
		panic(err)
	}
	pool := buffer.NewBufferPool(10)
	bufmgr := buffer.NewBufferPoolManager(diskManager, pool)

	tree := btree.NewBTree(disk.PageId(0))
	searchKey := table.EncodeTuple([][]byte{[]byte("y")})
	iter, err := tree.Search(bufmgr, &btree.SearchModeKey{Key: searchKey})
	if err != nil {
		panic(err)
	}
	defer iter.Finish(bufmgr)

	for {
		key, value, err := iter.Next(bufmgr)
		if err != nil {
			if err == btree.ErrEndOfIterator {
				break
			}
			panic(err)
		}
		record := make([][]byte, 0)
		record = table.DecodeTuple(key, record)
		record = table.DecodeTuple(value, record)
		printRecord(record)
	}
}
