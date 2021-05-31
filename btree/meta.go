package btree

import (
	"unsafe"

	"my-relly-go/disk"
)

type MetaHeader struct {
	rootPageId disk.PageId
}

type Meta struct {
	header  *MetaHeader
	_unused []byte
}

func NewMeta(bytes []byte) *Meta {
	meta := Meta{}
	headerSize := int(unsafe.Sizeof(*meta.header))
	if headerSize+1 > len(bytes) {
		panic("meta header must be aligned")
	}

	meta.header = (*MetaHeader)(unsafe.Pointer(&bytes[0]))
	meta._unused = bytes[headerSize:]
	return &meta
}
