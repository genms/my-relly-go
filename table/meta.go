package table

import "math"

const (
	META_VERSION_UNIQUE_INDICES VersionType = 1
	META_CURRENT_VERSION        VersionType = 1
	INVALID_SKEY                KeyElemType = math.MaxUint16
)

type VersionType uint16
type NumColsType uint16
type NumKeyElemsType uint16
type KeyElemType uint16

type Meta struct {
	Version       VersionType
	NumCols       NumColsType
	NumKeyElems   NumKeyElemsType
	_             uint16
	UniqueIndices [16][16]KeyElemType
}

func NewMeta() *Meta {
	meta := &Meta{
		Version: META_CURRENT_VERSION,
	}
	for i := 0; i < 16; i++ {
		for j := 0; j < 16; j++ {
			meta.UniqueIndices[i][j] = INVALID_SKEY
		}
	}
	return meta
}

func (m *Meta) SetUniqueIndices(indexNo int, ui []KeyElemType) {
	for i := 0; i < 16; i++ {
		if i < len(ui) {
			m.UniqueIndices[indexNo][i] = KeyElemType(ui[i])
		} else {
			m.UniqueIndices[indexNo][i] = INVALID_SKEY
		}
	}
}

func (m *Meta) GetUniqueIndices() [][]KeyElemType {
	ui := make([][]KeyElemType, 0, 16)
	for i := 0; i < 16; i++ {
		for j := 0; j < 16; j++ {
			if m.UniqueIndices[i][j] != INVALID_SKEY {
				if i >= len(ui) {
					ui = append(ui, []KeyElemType{})
				}
				ui[i] = append(ui[i], KeyElemType(m.UniqueIndices[i][j]))
			}
		}
	}
	return ui
}
