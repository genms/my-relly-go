package table

import "my-relly-go/memcmpable"

func EncodeTuple(elems [][]byte) []byte {
	encSize := 0
	for _, elem := range elems {
		encSize += memcmpable.EncodedSize(len(elem))
	}
	bytes := make([]byte, 0, encSize)
	for _, elem := range elems {
		bytes = memcmpable.Encode(elem, bytes)
	}
	return bytes
}

func DecodeTuple(bytes []byte, elems [][]byte) [][]byte {
	rest := bytes
	for len(rest) > 0 {
		elem := make([]byte, 0, len(bytes))
		rest, elem = memcmpable.Decode(rest, elem)
		elems = append(elems, elem)
	}
	return elems
}
