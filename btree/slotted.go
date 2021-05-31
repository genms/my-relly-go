package btree

import (
	"unsafe"

	"golang.org/x/xerrors"
)

type SlottedHeader struct {
	numSlots        uint16
	freeSpaceOffset uint16
	_pad            uint32
}

type Pointer struct {
	offset uint16
	length uint16
}

func (p *Pointer) getRange() (int, int) {
	start := int(p.offset)
	end := start + int(p.length)
	return start, end
}

var pointerSize int = int(unsafe.Sizeof(Pointer{}))

type Slotted struct {
	header *SlottedHeader
	body   []byte
}

func NewSlotted(bytes []byte) *Slotted {
	slotted := Slotted{}
	headerSize := int(unsafe.Sizeof(*slotted.header))
	if headerSize+1 > len(bytes) {
		panic("slotted header must be aligned")
	}

	slotted.header = (*SlottedHeader)(unsafe.Pointer(&bytes[0]))
	slotted.body = bytes[headerSize:]
	return &slotted
}

func (s *Slotted) Capacity() int {
	return len(s.body)
}

func (s *Slotted) NumSlots() int {
	return int(s.header.numSlots)
}

func (s *Slotted) FreeSpace() int {
	return int(s.header.freeSpaceOffset) - s.pointersSize()
}

func (s *Slotted) pointersSize() int {
	return int(pointerSize * s.NumSlots())
}

func (s *Slotted) pointers() []*Pointer {
	ret := make([]*Pointer, s.NumSlots())
	for i := 0; i < s.NumSlots(); i++ {
		ret[i] = (*Pointer)(unsafe.Pointer(&s.body[i*pointerSize]))
	}
	return ret
}

func (s *Slotted) data(pointer *Pointer) []byte {
	start, end := pointer.getRange()
	return s.body[start:end]
}

func (s *Slotted) Initialize() {
	s.header.numSlots = 0
	s.header.freeSpaceOffset = uint16(len(s.body))
}

func (s *Slotted) Insert(index int, length int) error {
	if s.FreeSpace() < pointerSize+length {
		return xerrors.New("no free space")
	}

	numSlotsOrig := s.NumSlots()
	s.header.freeSpaceOffset -= uint16(length)
	s.header.numSlots++
	freeSpaceOffset := s.header.freeSpaceOffset
	pointers := s.pointers()
	for i := numSlotsOrig - 1; i >= index; i-- {
		*pointers[i+1] = *pointers[i]
	}
	pointer := pointers[index]
	pointer.offset = freeSpaceOffset
	pointer.length = uint16(length)
	return nil
}

func (s *Slotted) Remove(index int) {
	s.Resize(index, 0)
	pointers := s.pointers()
	for i := index + 1; i < s.NumSlots(); i++ {
		*pointers[i-1] = *pointers[i]
	}
	s.header.numSlots--
}

func (s *Slotted) Resize(index int, lenNew int) error {
	pointers := s.pointers()
	lenOrig := int(pointers[index].length)
	lenIncr := lenNew - lenOrig
	if lenIncr == 0 {
		return nil
	}
	if lenIncr > s.FreeSpace() {
		return xerrors.New("no free space")
	}

	freeSpaceOffset := s.header.freeSpaceOffset
	offsetOrig := pointers[index].offset
	shiftStart := int(freeSpaceOffset)
	shiftEnd := int(offsetOrig)
	freeSpaceOffsetNew := int(freeSpaceOffset) - lenIncr
	s.header.freeSpaceOffset = uint16(freeSpaceOffsetNew)

	buf := make([]byte, shiftEnd-shiftStart)
	copy(buf, s.body[shiftStart:shiftEnd])
	copy(s.body[freeSpaceOffsetNew:], buf)

	for _, pointer := range pointers {
		if pointer.offset <= offsetOrig {
			pointer.offset = uint16(int(pointer.offset) - lenIncr)
		}
	}

	pointer := pointers[index]
	pointer.length = uint16(lenNew)
	if lenNew == 0 {
		pointer.offset = uint16(freeSpaceOffsetNew)
	}
	return nil
}

func (s *Slotted) ReadData(index int) []byte {
	return s.data(s.pointers()[index])
}

func (s *Slotted) WriteData(index int, buf []byte) {
	data := s.ReadData(index)
	copy(data, buf)
}
