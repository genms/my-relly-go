package btree

import (
	"bytes"
	"testing"
)

func TestSlotted(t *testing.T) {
	pageData := make([]byte, 128)
	slotted := NewSlotted(pageData)
	insert := func(slotted *Slotted, index int, buf []byte) {
		err := slotted.Insert(index, len(buf))
		if err != nil {
			panic(err)
		}
		slotted.WriteData(index, buf)
	}
	push := func(slotted *Slotted, buf []byte) {
		index := slotted.NumSlots()
		insert(slotted, index, buf)
	}
	readTest := func(slotted *Slotted, tests []string) {
		for index, data := range tests {
			actual := slotted.ReadData(index)
			expect := []byte(data)
			if !bytes.Equal(actual, expect) {
				t.Fatalf("slotted.getData(%d) = %v, want %v", index, actual, expect)
			}
		}
	}

	slotted.Initialize()

	if actual := slotted.Capacity(); actual != 120 {
		t.Fatalf("slotted.Capacity() = %v, want 120", actual)
	}

	push(slotted, []byte("hello"))
	push(slotted, []byte("world"))
	{
		tests := []string{
			"hello",
			"world",
		}
		readTest(slotted, tests)
	}

	insert(slotted, 1, []byte(", "))
	push(slotted, []byte("!"))
	{
		tests := []string{
			"hello",
			", ",
			"world",
			"!",
		}
		readTest(slotted, tests)
	}

	slotted.Remove(1)
	slotted.Resize(0, 2)
	slotted.WriteData(0, []byte("hi"))
	slotted.Resize(1, 8)
	slotted.WriteData(1, []byte("my rdbms"))
	{
		tests := []string{
			"hi",
			"my rdbms",
			"!",
		}
		readTest(slotted, tests)

		if actual := slotted.NumSlots(); actual != 3 {
			t.Fatalf("slotted.NumSlots() = %v, want 3", actual)
		}
	}
}
