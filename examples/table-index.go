package main

import (
	"bytes"
	"my-relly-go/buffer"
	"my-relly-go/disk"
	"my-relly-go/query"
)

func TableIndex() {
	diskManager, err := disk.OpenDiskManager("table_large.rly")
	if err != nil {
		panic(err)
	}
	pool := buffer.NewBufferPool(10)
	bufmgr := buffer.NewBufferPoolManager(diskManager, pool)

	plan := query.IndexScan{
		TableMetaPageId: disk.PageId(0),
		IndexMetaPageId: disk.PageId(2),
		SearchMode:      &query.TupleSearchModeKey{Key: [][]byte{[]byte("Smith")}},
		WhileCond: func(skey query.Tuple) bool {
			return bytes.Equal(skey[0], []byte("Smith"))
		},
	}
	exec, err := plan.Start(bufmgr)
	if err != nil {
		panic(err)
	}
	defer exec.Finish(bufmgr)

	for {
		record, err := exec.Next(bufmgr)
		if err != nil {
			if err == query.ErrEndOfIterator {
				break
			}
			panic(err)
		}
		printRecord(record)
	}
}
