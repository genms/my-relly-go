package main

import (
	"errors"
	"math"
	"os"
)

type PageId uint64

const INVALID_PAGE_ID = PageId(math.MaxUint64)
const PAGE_SIZE = 4096

func (self *PageId) Valid() (PageId, error) {
	if *self == INVALID_PAGE_ID {
		return INVALID_PAGE_ID, errors.New("Valid")
	} else {
		return *self, nil
	}
}

func (self *PageId) ToUint64() uint64 {
	return uint64(*self)
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

func (self *DiskManager) ReadPageData(pageId PageId, data []byte) error {
	var err error

	offset := int64(PAGE_SIZE * pageId)
	_, err = self.heapFile.Seek(offset, 0)
	if err != nil {
		return err
	}
	_, err = self.heapFile.Read(data)
	if err != nil {
		return err
	}
	return nil
}

func (self *DiskManager) WritePageData(pageId PageId, data []byte) error {
	var err error

	offset := int64(PAGE_SIZE * pageId)
	_, err = self.heapFile.Seek(offset, 0)
	if err != nil {
		return err
	}
	_, err = self.heapFile.Write(data)
	if err != nil {
		return err
	}
	return nil
}

func (self *DiskManager) AllocatePage() PageId {
	pageId := self.nextPageId
	self.nextPageId++
	return pageId
}

func (self *DiskManager) Sync() error {
	return self.heapFile.Sync()
}
