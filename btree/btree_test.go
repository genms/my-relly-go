package btree

import (
	"bytes"
	"encoding/binary"
	"io/ioutil"
	"log"
	"os"
	"sort"
	"testing"

	"my-relly-go/buffer"
	"my-relly-go/disk"
)

// BTreeの中身をlogに出力
func (t *BTree) dump(bufmgr *buffer.BufferPoolManager) {
	metaBuffer, err := bufmgr.FetchPage(t.metaPageId)
	if err != nil {
		panic(err)
	}
	defer bufmgr.FinishUsingPage(metaBuffer)
	meta := NewMeta(metaBuffer.Page[:])

	rootPageId := meta.header.rootPageId
	rootBuffer, err := bufmgr.FetchPage(rootPageId)
	if err != nil {
		panic(err)
	}
	defer bufmgr.FinishUsingPage(rootBuffer)

	t.dumpInternal(bufmgr, rootBuffer)
}

func (t *BTree) dumpInternal(bufmgr *buffer.BufferPoolManager, buffer *buffer.Buffer) {
	node := NewNode(buffer.Page[:])
	switch node.header.NodeTypeString() {
	case NODE_TYPE_LEAF:
		leaf := NewLeaf(node.body)
		log.Println("***** [leaf] ", buffer.PageId)
		log.Print("leaf.PrevPageId() = ")
		log.Println(leaf.PrevPageId())
		log.Print("leaf.NextPageId() = ")
		log.Println(leaf.NextPageId())
		for i := 0; i < leaf.NumPairs(); i++ {
			pair := leaf.PairAt(i)
			log.Printf("leaf.PairAt(%d) = ", i)
			log.Println(pair.Key[:1], pair.Value[:1])
		}
	case NODE_TYPE_BRANCH:
		branch := NewBranch(node.body)
		log.Println("***** [branch] ", buffer.PageId)
		for i := 0; i < branch.NumPairs(); i++ {
			pair := branch.PairAt(i)
			log.Printf("leaf.PairAt(%d) = ", i)
			log.Println(pair.Key[:1], pair.Value[:1])
		}
		log.Print("branch.header.rightChild = ")
		log.Println(branch.header.rightChild)
		for i := 0; i < branch.NumPairs(); i++ {
			func() {
				childPageId := branch.ChildAt(i)
				childNodeBuffer, err := bufmgr.FetchPage(childPageId)
				if err != nil {
					panic(err)
				}
				defer bufmgr.FinishUsingPage(childNodeBuffer)

				t.dumpInternal(bufmgr, childNodeBuffer)
			}()
		}
		childNodeBuffer, _ := bufmgr.FetchPage(branch.header.rightChild)
		defer bufmgr.FinishUsingPage(childNodeBuffer)
		t.dumpInternal(bufmgr, childNodeBuffer)
	default:
		panic("?")
	}
}

func TestBTree(t *testing.T) {
	uint64ToBytes := func(n uint64) []byte {
		buf := make([]byte, 8)
		binary.BigEndian.PutUint64(buf, n)
		return buf[:]
	}

	createDiskManager := func() (*os.File, *disk.DiskManager) {
		file, err := ioutil.TempFile("", "TestBuffer")
		if err != nil {
			panic(err)
		}
		disk, err := disk.NewDiskManager(file)
		if err != nil {
			panic(err)
		}
		return file, disk
	}

	destroyDiskManager := func(file *os.File, _ *disk.DiskManager) {
		if err := file.Close(); err != nil {
			panic(err)
		}
		if err := os.Remove(file.Name()); err != nil {
			panic(err)
		}
	}

	t.Run("Search", func(t *testing.T) {
		file, disk := createDiskManager()
		defer destroyDiskManager(file, disk)

		pool := buffer.NewBufferPool(10)
		bufmgr := buffer.NewBufferPoolManager(disk, pool)

		btree, err := CreateBTree(bufmgr)
		if err != nil {
			panic(err)
		}

		err = btree.Insert(bufmgr, uint64ToBytes(6), []byte("world"))
		if err != nil {
			panic(err)
		}
		err = btree.Insert(bufmgr, uint64ToBytes(3), []byte("hello"))
		if err != nil {
			panic(err)
		}
		err = btree.Insert(bufmgr, uint64ToBytes(8), []byte("!"))
		if err != nil {
			panic(err)
		}
		err = btree.Insert(bufmgr, uint64ToBytes(4), []byte(","))
		if err != nil {
			panic(err)
		}
		{
			iter, err := btree.Search(bufmgr, &SearchModeKey{uint64ToBytes(3)})
			if err != nil {
				panic(err)
			}
			defer iter.Finish(bufmgr)

			_, value, err := iter.Get()
			if err != nil {
				panic(err)
			}
			expect := []byte("hello")
			if !bytes.Equal(expect, value) {
				t.Fatalf("btree.search() = %v, want = %v", value, expect)
			}
		}
		{
			iter, err := btree.Search(bufmgr, &SearchModeKey{uint64ToBytes(8)})
			if err != nil {
				panic(err)
			}
			defer iter.Finish(bufmgr)

			_, value, err := iter.Get()
			if err != nil {
				panic(err)
			}
			expect := []byte("!")
			if !bytes.Equal(expect, value) {
				t.Fatalf("btree.search() = %v, want = %v", value, expect)
			}
		}
	})

	t.Run("Split", func(t *testing.T) {
		arrayRepeat := func(value byte, length int) []byte {
			longData := make([]byte, length)
			for j := 0; j < length; j++ {
				longData[j] = value
			}
			return longData
		}

		file, disk := createDiskManager()
		defer destroyDiskManager(file, disk)

		// pool := buffer.NewBufferPool(10)
		pool := buffer.NewBufferPool(5)
		bufmgr := buffer.NewBufferPoolManager(disk, pool)

		btree, err := CreateBTree(bufmgr)
		if err != nil {
			panic(err)
		}

		longDataList := [][]byte{
			arrayRepeat(0xC0, 1000),
			arrayRepeat(0x01, 1000),
			arrayRepeat(0xCA, 1000),
			arrayRepeat(0xFE, 1000),
			arrayRepeat(0xDE, 1000),
			arrayRepeat(0xAD, 1000),
			arrayRepeat(0xBE, 1000),
			arrayRepeat(0xAE, 1000),
		}

		for _, data := range longDataList {
			//log.Println("=============== ", i)
			err := btree.Insert(bufmgr, data, data)
			if err != nil {
				panic(err)
			}
			//btree.dump(bufmgr)
		}

		// 先頭からすべて検索
		func() {
			sortedLongDataList := longDataList
			sort.SliceStable(sortedLongDataList, func(i, j int) bool {
				return sortedLongDataList[i][0] < sortedLongDataList[j][0]
			})

			iter, err := btree.Search(bufmgr, &SearchModeStart{})
			if err != nil {
				panic(err)
			}
			defer iter.Finish(bufmgr)

			i := 0
			for {
				k, v, err := iter.Next(bufmgr)
				if err != nil {
					if err == ErrEndOfIterator {
						break
					}
					panic(err)
				}

				data := sortedLongDataList[i]
				if !bytes.Equal(data, k) {
					t.Fatalf("bytes.Equal(data, k) = %v, want = %v", k[0], data[0])
				}
				if !bytes.Equal(data, v) {
					t.Fatalf("bytes.Equal(data, v) = %v, want = %v", v[0], data[0])
				}
				i++
			}
		}()

		// 個別に検索
		for _, data := range longDataList {
			func() {
				iter, err := btree.Search(bufmgr, &SearchModeKey{data})
				if err != nil {
					panic(err)
				}
				defer iter.Finish(bufmgr)

				k, v, err := iter.Get()
				if err != nil {
					panic(err)
				}
				if !bytes.Equal(data, k) {
					t.Fatalf("bytes.Equal(data, k) = %v, want = %v", k[0], data[0])
				}
				if !bytes.Equal(data, v) {
					t.Fatalf("bytes.Equal(data, v) = %v, want = %v", v[0], data[0])
				}
			}()
		}
	})

	t.Run("Insert: キーが重複", func(t *testing.T) {
		file, disk := createDiskManager()
		defer destroyDiskManager(file, disk)

		pool := buffer.NewBufferPool(10)
		bufmgr := buffer.NewBufferPoolManager(disk, pool)

		btree, err := CreateBTree(bufmgr)
		if err != nil {
			panic(err)
		}

		if err = btree.Insert(bufmgr, uint64ToBytes(6), []byte("world")); err != nil {
			panic(err)
		}
		if err = btree.Insert(bufmgr, uint64ToBytes(6), []byte("world")); err != ErrDuplicateKey {
			t.Fatalf("btree.Insert() = %v, want ErrDuplicateKey", err)
		}
	})
}
