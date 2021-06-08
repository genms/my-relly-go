package examples

import (
	"fmt"
	"my-relly-go/btree"
	"my-relly-go/buffer"
	"my-relly-go/disk"
)

func BTreeLargeQuery() {
	diskManager, err := disk.OpenDiskManager("large.btr")
	if err != nil {
		panic(err)
	}
	pool := buffer.NewBufferPool(10)
	bufmgr := buffer.NewBufferPoolManager(diskManager, pool)

	tree := btree.NewBTree(disk.PageId(0))
	key := []byte{0xec, 0x2c, 0xdd, 0x0e, 0x4d, 0x0c, 0x94, 0x67, 0x30, 0x58, 0xc7, 0xd7, 0xbe, 0x7b, 0x85, 0xd2}
	iter, err := tree.Search(bufmgr, &btree.SearchModeKey{Key: key})
	if err != nil {
		panic(err)
	}
	defer iter.Finish(bufmgr)

	key, value, err := iter.Next(bufmgr)
	if err != nil {
		panic(err)
	}
	fmt.Printf("%02x = %02x\n", key, value)
}
