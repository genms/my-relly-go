package disk

import (
	"encoding/binary"
	"math"
	"os"

	"golang.org/x/xerrors"
)

type PageId uint64

var (
	ErrInvalidPageId = xerrors.New("invalid page id")
)

func BytesToPageId(b []byte) PageId {
	return PageId(binary.LittleEndian.Uint64(b))
}

func PageIdToBytes(pageId PageId) []byte {
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, uint64(pageId))
	return buf[:]
}

const INVALID_PAGE_ID = PageId(math.MaxUint64)
const PAGE_SIZE = 4096

func (p *PageId) Valid() (PageId, error) {
	if *p == INVALID_PAGE_ID {
		return INVALID_PAGE_ID, ErrInvalidPageId
	} else {
		return *p, nil
	}
}

func (p *PageId) ToUint64() uint64 {
	return uint64(*p)
}

type DiskManager struct {
	heapFile   *os.File
	nextPageId PageId
}

func NewDiskManager(heapFile *os.File) (*DiskManager, error) {
	stat, err := heapFile.Stat()
	if err != nil {
		return nil, err
	}

	heapFileSize := stat.Size()
	nextPageId := PageId(heapFileSize / PAGE_SIZE)
	return &DiskManager{heapFile, nextPageId}, nil
}

func OpenDiskManager(heapFilePath string) (*DiskManager, error) {
	heapFile, err := os.OpenFile(heapFilePath, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, err
	}
	diskManager, err := NewDiskManager(heapFile)
	if err != nil {
		return nil, err
	}
	return diskManager, nil
}

func (m *DiskManager) ReadPageData(pageId PageId, data []byte) error {
	var err error

	offset := int64(PAGE_SIZE * pageId)
	_, err = m.heapFile.Seek(offset, 0)
	if err != nil {
		return err
	}
	_, err = m.heapFile.Read(data)
	if err != nil {
		return err
	}
	return nil
}

func (m *DiskManager) WritePageData(pageId PageId, data []byte) error {
	var err error

	offset := int64(PAGE_SIZE * pageId)
	_, err = m.heapFile.Seek(offset, 0)
	if err != nil {
		return err
	}
	_, err = m.heapFile.Write(data)
	if err != nil {
		return err
	}
	return nil
}

func (m *DiskManager) AllocatePage() PageId {
	pageId := m.nextPageId
	m.nextPageId++
	return pageId
}

func (m *DiskManager) Sync() error {
	return m.heapFile.Sync()
}
