package main

import (
	"bytes"
	"my-relly-go/buffer"
	"my-relly-go/disk"
	"my-relly-go/query"
)

func SimpleTablePlan() {
	diskManager, err := disk.OpenDiskManager("simple.rly")
	if err != nil {
		panic(err)
	}
	pool := buffer.NewBufferPool(10)
	bufmgr := buffer.NewBufferPoolManager(diskManager, pool)

	plan := query.Filter{
		Cond: func(record query.Tuple) bool {
			return bytes.Compare(record[1], []byte("Dave")) < 0
		},
		InnerPlan: &query.SeqScan{
			TableMetaPageId: disk.PageId(0),
			SearchMode:      &query.TupleSearchModeKey{Key: [][]byte{[]byte("w")}},
			WhileCond: func(pkey query.Tuple) bool {
				return bytes.Compare(pkey[0], []byte("z")) < 0
			},
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
