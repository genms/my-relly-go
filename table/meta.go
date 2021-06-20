package table

import (
	"strconv"
	"strings"

	"google.golang.org/protobuf/proto"
)

const (
	META_VERSION_UNIQUE_INDICES = 1
	META_CURRENT_VERSION        = 1
	//INVALID_SKEY                = math.MaxUint16
)

/*
type Meta struct {
	Version       int
	NumCols       int
	NumKeyElems   int
	ColNames      []string
	UniqueIndices []string
}
*/

func NewMeta() *Meta {
	meta := &Meta{
		Version: META_CURRENT_VERSION,
	}
	return meta
}

func (m *Meta) AddUniqueIndices(indices []int) {
	str := ""
	for _, col := range indices {
		str += strconv.Itoa(col) + ","
	}
	str = strings.Trim(str, ",")
	m.UniqueIndicesStr = append(m.UniqueIndicesStr, str)
}

func (m *Meta) GetUniqueIndices() [][]int {
	indices := [][]int{}
	for i, str := range m.UniqueIndicesStr {
		splited := strings.Split(str, ",")
		indices = append(indices, []int{})
		for _, colStr := range splited {
			col, err := strconv.Atoi(colStr)
			if err != nil {
				panic(err)
			}
			indices[i] = append(indices[i], col)
		}
	}
	return indices
}

func NewMetaFromBytes(buf []byte) *Meta {
	meta := &Meta{}
	if err := proto.Unmarshal(buf, meta); err != nil {
		panic(err)
	}
	return meta
}

func (m *Meta) ToBytes() []byte {
	buf, err := proto.Marshal(m)
	if err != nil {
		panic(err)
	}
	return buf
}
