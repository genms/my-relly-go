package memcmpable

import (
	"bytes"
	"testing"
)

func TestMemCmpAble(t *testing.T) {
	org1 := []byte("helloworld!memcmpable")
	org2 := []byte("foobarbazhogehuga")

	encSize := EncodedSize(len(org1)) + EncodedSize(len(org2))
	//fmt.Println(encSize)
	enc := make([]byte, 0, encSize)
	enc = Encode(org1, enc)
	//fmt.Println(enc)
	enc = Encode(org2, enc)
	//fmt.Println(enc)
	rest := enc

	dec1 := make([]byte, 0, len(rest))
	rest, dec1 = Decode(rest, dec1)
	if !bytes.Equal(org1, dec1) {
		t.Fatalf("Decode() = %v, want %v", dec1, org1)
	}
	dec2 := make([]byte, 0, len(rest))
	_, dec2 = Decode(rest, dec2)
	if !bytes.Equal(org2, dec2) {
		t.Fatalf("Decode() = %v, want %v", dec2, org2)
	}
}
