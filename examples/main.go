package main

import "fmt"

func printRecord(record [][]byte) {
	s := ""
	for _, col := range record {
		s += string(col) + "\t"
	}
	fmt.Println(s)
}

func main() {
	BTreeAll()
	//BTreeLarge()
	//BTreeCreate()
	//BTreeLargeQuery()
	//BTreeQuery()
	//BTreeRange()
	//SimpleTableAll()
	//SimpleTableCreate()
	//SimpleTableExact()
	//SimpleTablePlan()
	//SimpleTableRange()
	//SimpleTableScan()
	//TableCreate()
	//TableIndex()
	//TableLarge()
}
