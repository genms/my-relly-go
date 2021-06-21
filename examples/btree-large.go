package main

import (
	"crypto/md5"
	"encoding/binary"
	"fmt"
	"my-relly-go/btree"
	"my-relly-go/buffer"
	"my-relly-go/disk"
	"os"
)

const NUM_PAIRS uint32 = 1_000_000

func BTreeLarge() {
	file, err := os.Create("large.btr")
	if err != nil {
		panic(err)
	}
	defer func() {
		if err := file.Close(); err != nil {
			panic(err)
		}
	}()

	disk, err := disk.NewDiskManager(file)
	if err != nil {
		panic(err)
	}

	pool := buffer.NewBufferPool(100)
	bufmgr := buffer.NewBufferPoolManager(disk, pool)

	btree, err := btree.CreateBTree(bufmgr)
	if err != nil {
		panic(err)
	}

	var i uint32
	for i = 1; i <= NUM_PAIRS; i++ {
		pkey := make([]byte, 4)
		binary.BigEndian.PutUint32(pkey, uint32(i))
		hash := md5.Sum(pkey)
		if err := btree.Insert(bufmgr, hash[:], pkey[:]); err != nil {
			panic(err)
		}
	}
	bufmgr.Flush()
	fmt.Println("Ok")
}
