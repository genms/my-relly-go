package examples

import (
	"crypto/md5"
	"crypto/sha1"
	"encoding/binary"
	"fmt"
	"my-relly-go/buffer"
	"my-relly-go/disk"
	"my-relly-go/table"
	"os"
)

const NUM_ROWS int = 10_000_000

func TableLarge() {
	file, err := os.Create("table_large.rly")
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

	pool := buffer.NewBufferPool(1_000_000)
	bufmgr := buffer.NewBufferPoolManager(diskManager, pool)

	tbl := table.Table{
		MetaPageId:  disk.INVALID_PAGE_ID,
		NumKeyElems: 1,
		UniqueIndices: []table.UniqueIndex{
			{
				MetaPageId: disk.INVALID_PAGE_ID,
				SKey:       []table.KeyElemType{2},
			},
		},
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

	var i int
	for i = 0; i <= NUM_ROWS; i++ {
		pkey := make([]byte, 4)
		binary.BigEndian.PutUint32(pkey, uint32(i))
		md5Hash := md5.Sum(pkey)
		sha1Hash := sha1.Sum(pkey)
		if err := tbl.Insert(bufmgr, [][]byte{
			pkey[:],
			md5Hash[:],
			sha1Hash[:],
		}); err != nil {
			panic(err)
		}
	}

	bufmgr.Flush()
	fmt.Println("Ok")
}
