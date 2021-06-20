package query

import (
	"fmt"
	"log"
	"math/rand"
	"my-relly-go/buffer"
	"my-relly-go/disk"
	"my-relly-go/table"
	"os"
	"strconv"
	"testing"
	"time"
)

func TestMain(m *testing.M) {
	createTestTable()
	m.Run()
}

func createTestTable() {
	rand.Seed(time.Now().UnixNano())

	create := func(numKeyElems int) {
		fileName := fmt.Sprintf("../query_test%d.rly", numKeyElems)
		_, err := os.Stat(fileName)
		if err == nil {
			return
		} else {
			if !os.IsNotExist(err) {
				panic(err)
			}
		}

		file, err := os.Create(fileName)
		if err != nil {
			panic(err)
		}
		defer func() {
			if err := file.Close(); err != nil {
				panic(err)
			}
		}()

		diskManager, err := disk.NewDiskManager(file)
		if err != nil {
			panic(err)
		}

		pool := buffer.NewBufferPool(100)
		bufmgr := buffer.NewBufferPoolManager(diskManager, pool)

		tbl := table.Table{
			MetaPageId:  disk.INVALID_PAGE_ID,
			NumCols:     7,
			NumKeyElems: numKeyElems,
			ColNames: []string{
				"id1",
				"id2",
				"email",
				"name",
				"grade",
				"class",
				"student_no",
			},
			UniqueIndices: []table.UniqueIndex{
				{
					MetaPageId: disk.INVALID_PAGE_ID,
					SKey:       []int{2}, // email
				},
				{
					MetaPageId: disk.INVALID_PAGE_ID,
					SKey:       []int{4, 5, 6}, // grade, class, student_no
				},
			},
		}
		if err := tbl.Create(bufmgr); err != nil {
			panic(err)
		}
		fmt.Println(tbl)

		for i := 0; i < 960; i++ {
			grade := int(i/320) + 1  // 01..03
			class := int(i/40)%8 + 1 // 01..08
			student_no := i%40 + 1   // 01..40

			record := make([][]byte, 7)
			record[0] = []byte(fmt.Sprintf("%04d", i))
			record[1] = []byte(strconv.Itoa(i % 2))
			record[2] = []byte(fmt.Sprintf("%04d@example.com", i))
			record[3] = []byte(fmt.Sprintf("YamadaTaro%02d%02d%02d", grade, class, student_no))
			record[4] = []byte(fmt.Sprintf("%02d", grade))
			record[5] = []byte(fmt.Sprintf("%02d", class))
			record[6] = []byte(fmt.Sprintf("%02d", student_no))
			if err := tbl.Insert(bufmgr, record); err != nil {
				log.Println(record)
				panic(err)
			}
		}

		bufmgr.Flush()
		//fmt.Printf("%s created\n", fileName)
	}
	create(1)
	create(2)
}
