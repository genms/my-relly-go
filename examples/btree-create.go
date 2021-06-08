package examples

import (
	"my-relly-go/btree"
	"my-relly-go/buffer"
	"my-relly-go/disk"
	"os"
)

func BTreeCreate() {
	file, err := os.Create("test.btr")
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

	pool := buffer.NewBufferPool(10)
	bufmgr := buffer.NewBufferPoolManager(disk, pool)

	btree, err := btree.CreateBTree(bufmgr)
	if err != nil {
		panic(err)
	}

	dataList := []struct {
		key   string
		value string
	}{
		{"Kanagawa", "Yokohama"},
		{"Osaka", "Osaka"},
		{"Aichi", "Nagoya"},
		{"Hokkaido", "Sapporo"},
		{"Fukuoka", "Fukuoka"},
		{"Hyogo", "Kobe"},
	}
	for _, data := range dataList {
		if err := btree.Insert(bufmgr, []byte(data.key), []byte(data.value)); err != nil {
			panic(err)
		}
	}

	bufmgr.Flush()
}
