package disk

import (
	"bytes"
	"io/ioutil"
	"os"
	"testing"
)

func TestDisk(t *testing.T) {
	var err error

	file, err := ioutil.TempFile("", "TestDisk")
	if err != nil {
		panic(err)
	}
	defer func() {
		/*
			if derr := file.Close(); err != nil {
				panic(derr)
			}
		*/
		if derr := os.Remove(file.Name()); derr != nil {
			panic(derr)
		}
	}()

	disk, err := NewDiskManager(file)
	if err != nil {
		panic(err)
	}

	hello := make([]byte, PAGE_SIZE)
	copy(hello, []byte("hello"))
	helloPageId := disk.AllocatePage()
	err = disk.WritePageData(helloPageId, hello)
	if err != nil {
		panic(err)
	}

	world := make([]byte, PAGE_SIZE)
	copy(world, []byte("world"))
	worldPageId := disk.AllocatePage()
	err = disk.WritePageData(worldPageId, world)
	if err != nil {
		panic(err)
	}

	err = file.Close()
	if err != nil {
		panic(err)
	}

	disk2, err := OpenDiskManager(file.Name())
	if err != nil {
		panic(err)
	}

	buf := make([]byte, PAGE_SIZE)
	disk2.ReadPageData(helloPageId, buf)
	if !bytes.Equal(hello, buf) {
		t.Fatal("bytes.Equal(hello, buf)")
	}
	disk2.ReadPageData(worldPageId, buf)
	if !bytes.Equal(world, buf) {
		t.Fatal("bytes.Equal(world, buf)")
	}
}
