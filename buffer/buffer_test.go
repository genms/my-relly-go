package buffer

import (
	"bytes"
	"io/ioutil"
	"os"
	"testing"

	"my-relly-go/disk"
)

func TestBuffer(t *testing.T) {
	// 書き込むデータを準備
	hello := make([]byte, disk.PAGE_SIZE)
	copy(hello, []byte("hello"))
	world := make([]byte, disk.PAGE_SIZE)
	copy(world, []byte("world"))

	// ディスクマネージャ作成用
	createDiskManager := func() (*os.File, *disk.DiskManager) {
		file, err := ioutil.TempFile("", "TestBuffer")
		if err != nil {
			panic(err)
		}
		diskManager, err := disk.NewDiskManager(file)
		if err != nil {
			panic(err)
		}
		return file, diskManager
	}

	// ディスクマネージャ破棄用
	destroyDiskManager := func(file *os.File, _ *disk.DiskManager) {
		if err := file.Close(); err != nil {
			panic(err)
		}
		if err := os.Remove(file.Name()); err != nil {
			panic(err)
		}
	}

	t.Run("正常系", func(t *testing.T) {
		tempFile, diskManager := createDiskManager()
		defer destroyDiskManager(tempFile, diskManager)

		// バッファプール初期化 サイズ=1
		pool := NewBufferPool(1)
		bufmgr := NewBufferPoolManager(diskManager, pool)

		var page1Id disk.PageId
		{
			// ページ1を作成し、バッファ貸し出し
			buffer, err := bufmgr.CreatePage()
			if err != nil {
				t.Fatalf("bufmgr.CreatePage() %s", err)
			}
			// ページ1のデータを更新（バッファに格納）
			copy(buffer.Page[:], hello)
			buffer.IsDirty = true
			page1Id = buffer.PageId
			bufmgr.FinishUsingPage(buffer)
		}
		{
			// ページ1を取得（バッファから）
			buffer, err := bufmgr.FetchPage(page1Id)
			if err != nil {
				t.Fatal("bufmgr.FetchPage()")
			}
			if !bytes.Equal(hello, buffer.Page[:]) {
				t.Fatalf("bufmgr.FetchPage() actual = %v, expect = %v", buffer.Page[0:5], hello[0:5])
			}
			bufmgr.FinishUsingPage(buffer)
		}
		var page2Id disk.PageId
		{
			// ページ2を作成し、バッファ貸し出し
			// ここでページ1はフラッシュされ、バッファからも捨てられる
			buffer, err := bufmgr.CreatePage()
			if err != nil {
				t.Fatalf("bufmgr.CreatePage() %s", err)
			}
			// ページ2のデータを更新（バッファに格納）
			copy(buffer.Page[:], world)
			buffer.IsDirty = true
			page2Id = buffer.PageId
			bufmgr.FinishUsingPage(buffer)
		}
		{
			// ページ1を取得（ファイルから読み込み、バッファ貸し出し）
			// ここでページ2はフラッシュされ、バッファからも捨てられる
			buffer, err := bufmgr.FetchPage(page1Id)
			if err != nil {
				t.Fatal("bufmgr.FetchPage()")
			}
			if !bytes.Equal(hello, buffer.Page[:]) {
				t.Fatalf("bufmgr.FetchPage() actual = %v, expect = %v", buffer.Page[0:5], hello[0:5])
			}
			bufmgr.FinishUsingPage(buffer)
		}
		{
			// ページ2を取得（ファイルからから読み込み、バッファ貸し出し）
			// ここでページ1はバッファから捨てられる
			buffer, err := bufmgr.FetchPage(page2Id)
			if err != nil {
				t.Fatal("bufmgr.FetchPage()")
			}
			if !bytes.Equal(world, buffer.Page[:]) {
				t.Fatalf("bufmgr.FetchPage() actual = %v, expect = %v", buffer.Page[0:5], world[0:5])
			}
			bufmgr.FinishUsingPage(buffer)
		}
	})

	t.Run("CreatePage_バッファが足りない", func(t *testing.T) {
		var err error

		tempFile, diskManager := createDiskManager()
		defer destroyDiskManager(tempFile, diskManager)

		// バッファサイズ=1
		pool := NewBufferPool(1)
		bufmgr := NewBufferPoolManager(diskManager, pool)

		_, err = bufmgr.CreatePage()
		if err != nil {
			t.Fatalf("bufmgr.CreatePage() %s", err)
		}

		// 2回目のCreatePageでエラー
		_, err = bufmgr.CreatePage()
		if err == nil {
			t.Fatal("bufmgr.CreatePage() Success: not expected")
		}
	})

	t.Run("FetchPage_バッファが足りない", func(t *testing.T) {
		tempFile, diskManager := createDiskManager()
		defer destroyDiskManager(tempFile, diskManager)

		// バッファサイズ=1
		pool := NewBufferPool(1)
		bufmgr := NewBufferPoolManager(diskManager, pool)

		var page1Id disk.PageId
		{
			buffer, err := bufmgr.CreatePage()
			if err != nil {
				t.Fatalf("bufmgr.CreatePage() %s", err)
			}
			copy(buffer.Page[:], hello)
			buffer.IsDirty = true
			page1Id = buffer.PageId
			bufmgr.FinishUsingPage(buffer)
		}
		var page2Id disk.PageId
		{
			buffer, err := bufmgr.CreatePage()
			if err != nil {
				t.Fatalf("bufmgr.CreatePage() %s", err)
			}
			copy(buffer.Page[:], world)
			buffer.IsDirty = true
			page2Id = buffer.PageId
			bufmgr.FinishUsingPage(buffer)
		}
		bufmgr.Flush()

		{
			buffer, err := bufmgr.FetchPage(page1Id)
			if err != nil {
				t.Fatal("bufmgr.FetchPage()")
			}
			if !bytes.Equal(hello, buffer.Page[:]) {
				t.Fatalf("bufmgr.FetchPage() actual = %v, expect = %v", buffer.Page[0:5], hello[0:5])
			}

			// 2回目のFetchPageでエラー
			_, err = bufmgr.FetchPage(page2Id)
			if err == nil {
				t.Fatal("bufmgr.FetchPage() Success: not expected")
			}
		}
	})
}
