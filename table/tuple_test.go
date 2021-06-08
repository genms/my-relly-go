package table

import (
	"bytes"
	"testing"
)

func TestTuple(t *testing.T) {
	org := [][]byte{
		[]byte("helloworld!memcmpable"),
		[]byte("foobarbazhogehuga"),
	}

	elems := make([][]byte, 0)
	enc := EncodeTuple(org)
	elems = DecodeTuple(enc, elems)
	for i, r := range elems {
		if !bytes.Equal(r, org[i]) {
			t.Fatalf("Decode() = %v, want %v", r, org)
		}
	}
}
