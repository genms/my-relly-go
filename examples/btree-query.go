package main

import (
	"fmt"
	"my-relly-go/btree"
	"my-relly-go/buffer"
	"my-relly-go/disk"
)

func BTreeQuery() {
	diskManager, err := disk.OpenDiskManager("test.btr")
	if err != nil {
		panic(err)
	}
	pool := buffer.NewBufferPool(10)
	bufmgr := buffer.NewBufferPoolManager(diskManager, pool)

	tree := btree.NewBTree(disk.PageId(0))
	iter, err := tree.Search(bufmgr, &btree.SearchModeKey{Key: []byte("Hyogo")})
	if err != nil {
		panic(err)
	}
	defer iter.Finish(bufmgr)

	key, value, err := iter.Next(bufmgr)
	if err != nil {
		panic(err)
	}
	//fmt.Printf("%02x = %02x\n", key, value)
	fmt.Printf("%s = %s\n", string(key), string(value))
}
