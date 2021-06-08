package examples

import (
	"bytes"
	"my-relly-go/btree"
	"my-relly-go/buffer"
	"my-relly-go/disk"
	"my-relly-go/table"
)

func SimpleTableScan() {
	diskManager, err := disk.OpenDiskManager("simple.rly")
	if err != nil {
		panic(err)
	}
	pool := buffer.NewBufferPool(10)
	bufmgr := buffer.NewBufferPoolManager(diskManager, pool)

	tree := btree.NewBTree(disk.PageId(0))
	iter, err := tree.Search(bufmgr, &btree.SearchModeStart{})
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
		if bytes.Equal(record[2], []byte("Smith")) {
			printRecord(record)
		}
	}
}
