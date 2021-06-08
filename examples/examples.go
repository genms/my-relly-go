package examples

import "fmt"

func printRecord(record [][]byte) {
	s := ""
	for _, col := range record {
		s += string(col) + "\t"
	}
	fmt.Println(s)
}
