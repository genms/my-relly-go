package examples

import (
	"fmt"
	"my-relly-go/buffer"
	"my-relly-go/disk"
	"my-relly-go/table"
	"os"
)

func SimpleTableCreate() {
	file, err := os.Create("simple.rly")
	if err != nil {
		panic(err)
	}
	defer func() {
		if err := file.Close(); err != nil {
			panic(err)
		}
	}()

	diskManager, err := disk.NewDiskManager(file)
	if err != nil {
		panic(err)
	}

	pool := buffer.NewBufferPool(10)
	bufmgr := buffer.NewBufferPoolManager(diskManager, pool)

	tbl := table.SimpleTable{
		MetaPageId:  disk.PageId(0),
		NumKeyElems: 1,
	}
	if err := tbl.Create(bufmgr); err != nil {
		panic(err)
	}
	fmt.Println(tbl)

	rows := [][][]byte{
		{[]byte("z"), []byte("Alice"), []byte("Smith")},
		{[]byte("x"), []byte("Bob"), []byte("Johnson")},
		{[]byte("y"), []byte("Charlie"), []byte("Williams")},
		{[]byte("w"), []byte("Dave"), []byte("Miller")},
		{[]byte("v"), []byte("Eve"), []byte("Brown")},
	}
	for _, row := range rows {
		if err := tbl.Insert(bufmgr, row); err != nil {
			panic(err)
		}
	}

	bufmgr.Flush()
}
