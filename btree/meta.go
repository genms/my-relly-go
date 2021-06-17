package btree

import (
	"my-relly-go/disk"
	"unsafe"
)

type MetaHeader struct {
	rootPageId disk.PageId
}

type Meta struct {
	header  *MetaHeader
	appArea []byte
}

func NewMeta(bytes []byte) *Meta {
	meta := Meta{}
	headerSize := int(unsafe.Sizeof(*meta.header))
	if headerSize+1 > len(bytes) {
		panic("meta header must be aligned")
	}

	meta.header = (*MetaHeader)(unsafe.Pointer(&bytes[0]))
	meta.appArea = bytes[headerSize:]
	return &meta
}
