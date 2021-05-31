package examples

import (
	"fmt"
	"my-relly-go/btree"
	"my-relly-go/buffer"
	"my-relly-go/disk"
	"os"
)

func BTreeAll() {
	file, err := os.Open("test.btr")
	if err != nil {
		panic(err)
	}
	diskManager, err := disk.NewDiskManager(file)
	if err != nil {
		panic(err)
	}
	defer func() {
		if err := file.Close(); err != nil {
			panic(err)
		}
	}()

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
		//fmt.Printf("%02x = %02x\n", key, value)
		fmt.Printf("%s = %s\n", string(key), string(value))
	}
}
