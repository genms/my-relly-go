package query

import (
	"bytes"
	"log"
	"my-relly-go/buffer"
	"my-relly-go/disk"
	"testing"
)

type QueryTestCase struct {
	query       string
	wantExplain []string
	wantPKeys   [][]byte
}

type QueryErrorTestCase struct {
	query   string
	wantErr error
}

func openDb(fileName string) (*buffer.BufferPoolManager, *Parser) {
	diskManager, err := disk.OpenDiskManager(fileName)
	if err != nil {
		panic(err)
	}
	pool := buffer.NewBufferPool(10)
	bufmgr := buffer.NewBufferPoolManager(diskManager, pool)

	parser, err := NewParser(bufmgr)
	if err != nil {
		panic(err)
	}

	return bufmgr, parser
}

func printRecord(record [][]byte) {
	s := ""
	for _, col := range record {
		s += string(col) + "\t"
	}
	log.Println(s)
}

func queryTest(t *testing.T, bufmgr *buffer.BufferPoolManager, parser *Parser, tests []*QueryTestCase) {
	for i, tt := range tests {
		log.Printf("# %s %d", t.Name(), i)

		plan, err := parser.Parse(tt.query)
		if err != nil {
			panic(err)
		}

		{
			got := plan.Explain()
			if len(got) != len(tt.wantExplain) {
				t.Fatalf("%s explain = %v, want = %v", tt.query, got, tt.wantExplain)
			}
			for i := 0; i < len(got); i++ {
				if got[i] != tt.wantExplain[i] {
					t.Fatalf("%s explain = %v, want = %v", tt.query, got, tt.wantExplain)
				}
			}
		}

		exec, err := plan.Start(bufmgr)
		if err != nil {
			panic(err)
		}
		defer exec.Finish(bufmgr)

		i := 0
		for {
			record, err := exec.Next(bufmgr)
			if err != nil {
				if err == ErrEndOfIterator {
					break
				}
				panic(err)
			}
			if len(tt.wantPKeys) <= i {
				t.Fatalf("%s: too many records", tt.query)
			}
			if !bytes.Equal(record[0], tt.wantPKeys[i]) {
				t.Fatalf("%s = %v, want %v", tt.query, record[0], tt.wantPKeys[i])
			}
			printRecord(record)
			i++
		}

		if len(tt.wantPKeys) != i {
			t.Fatalf("%s: too less records", tt.query)
		}
	}
}

func queryErrorTest(t *testing.T, bufmgr *buffer.BufferPoolManager, parser *Parser, tests []*QueryErrorTestCase) {
	for i, tt := range tests {
		log.Printf("# %s %d", t.Name(), i)

		_, err := parser.Parse(tt.query)
		if err != tt.wantErr {
			t.Fatalf("parser.Parse = %v, want %v", err, tt.wantErr)
		}
	}
}

func TestParserSingleKey(t *testing.T) {
	bufmgr, parser := openDb("../query_test1.rly")

	t.Run("単一プライマリキー、完全一致検索", func(t *testing.T) {
		tests := []*QueryTestCase{
			{
				`{"id1": "0010"}`,
				[]string{"SeqScan"},
				[][]byte{[]byte("0010")},
			},
			{
				`{"id1": "aaaa"}`,
				[]string{"SeqScan"},
				[][]byte{},
			},
			{
				`{"id1": ""}`,
				[]string{"SeqScan"},
				[][]byte{},
			},
			{
				`{"0": "0010"}`,
				[]string{"SeqScan"},
				[][]byte{[]byte("0010")},
			},
		}
		queryTest(t, bufmgr, parser, tests)
	})

	t.Run("単一プライマリキー、範囲検索", func(t *testing.T) {
		tests := []*QueryTestCase{
			{
				`{"id1": {"$gte": "0010", "$lte": "0013"}}`,
				[]string{"Filter", "SeqScan"},
				[][]byte{[]byte("0010"), []byte("0011"), []byte("0012"), []byte("0013")},
			},
			{
				`{"id1": {"$gt": "0010", "$lt": "0013"}}`,
				[]string{"Filter", "SeqScan"},
				[][]byte{[]byte("0011"), []byte("0012")},
			},
			{
				`{"id1": {"$lte": "0003"}}`,
				[]string{"Filter", "SeqScan"},
				[][]byte{[]byte("0000"), []byte("0001"), []byte("0002"), []byte("0003")},
			},
			{
				`{"id1": {"$lt": "0003"}}`,
				[]string{"Filter", "SeqScan"},
				[][]byte{[]byte("0000"), []byte("0001"), []byte("0002")},
			},
			{
				`{"id1": {"$gte": "0956"}}`,
				[]string{"Filter", "SeqScan"},
				[][]byte{[]byte("0956"), []byte("0957"), []byte("0958"), []byte("0959")},
			},
			{
				`{"id1": {"$gt": "0956"}}`,
				[]string{"Filter", "SeqScan"},
				[][]byte{[]byte("0957"), []byte("0958"), []byte("0959")},
			},
			{
				`{"id1": {"$gte": "0010", "$lte": "0013", "$ne": "0012"}}`,
				[]string{"Filter", "SeqScan"},
				[][]byte{[]byte("0010"), []byte("0011"), []byte("0013")},
			},
			{
				`{"id1": {"$lt": "0000"}}`,
				[]string{"Filter", "SeqScan"},
				[][]byte{},
			},
			{
				`{"id1": {"$gt": "0959"}}`,
				[]string{"Filter", "SeqScan"},
				[][]byte{},
			},
		}
		queryTest(t, bufmgr, parser, tests)
	})

	t.Run("単一セカンダリキー、完全一致検索", func(t *testing.T) {
		tests := []*QueryTestCase{
			{
				`{"email": "0010@example.com"}`,
				[]string{"IndexScan"},
				[][]byte{[]byte("0010")},
			},
			{
				`{"email": "aaaa@example.com"}`,
				[]string{"IndexScan"},
				[][]byte{},
			},
			{
				`{"email": ""}`,
				[]string{"IndexScan"},
				[][]byte{},
			},
			{
				`{"2": "0010@example.com"}`,
				[]string{"IndexScan"},
				[][]byte{[]byte("0010")},
			},
		}
		queryTest(t, bufmgr, parser, tests)
	})

	t.Run("単一セカンダリキー、範囲検索", func(t *testing.T) {
		tests := []*QueryTestCase{
			{
				`{"email": {"$gte": "0010@example.com", "$lte": "0013@example.com"}}`,
				[]string{"Filter", "IndexScan"},
				[][]byte{[]byte("0010"), []byte("0011"), []byte("0012"), []byte("0013")},
			},
			{
				`{"email": {"$gt": "0010@example.com", "$lt": "0013@example.com"}}`,
				[]string{"Filter", "IndexScan"},
				[][]byte{[]byte("0011"), []byte("0012")},
			},
			{
				`{"email": {"$lte": "0003@example.com"}}`,
				[]string{"Filter", "IndexScan"},
				[][]byte{[]byte("0000"), []byte("0001"), []byte("0002"), []byte("0003")},
			},
			{
				`{"email": {"$lt": "0003@example.com"}}`,
				[]string{"Filter", "IndexScan"},
				[][]byte{[]byte("0000"), []byte("0001"), []byte("0002")},
			},
			{
				`{"email": {"$gte": "0956@example.com"}}`,
				[]string{"Filter", "IndexScan"},
				[][]byte{[]byte("0956"), []byte("0957"), []byte("0958"), []byte("0959")},
			},
			{
				`{"email": {"$gt": "0956@example.com"}}`,
				[]string{"Filter", "IndexScan"},
				[][]byte{[]byte("0957"), []byte("0958"), []byte("0959")},
			},
			{
				`{"email": {"$gte": "0010@example.com", "$lte": "0013@example.com", "$ne": "0012@example.com"}}`,
				[]string{"Filter", "IndexScan"},
				[][]byte{[]byte("0010"), []byte("0011"), []byte("0013")},
			},
			{
				`{"email": {"$lt": "0000@example.com"}}`,
				[]string{"Filter", "IndexScan"},
				[][]byte{},
			},
			{
				`{"email": {"$gt": "0959@example.com"}}`,
				[]string{"Filter", "IndexScan"},
				[][]byte{},
			},
		}
		queryTest(t, bufmgr, parser, tests)
	})
}

func TestParserCompositeKey(t *testing.T) {
	bufmgr, parser := openDb("../query_test2.rly")

	t.Run("複合プライマリキー、完全一致検索", func(t *testing.T) {
		tests := []*QueryTestCase{
			{
				`{"id1": "0010", "id2": "0"}`,
				[]string{"SeqScan"},
				[][]byte{[]byte("0010")},
			},
			{
				`{"id1": "0010", "id2": "1"}`,
				[]string{"SeqScan"},
				[][]byte{},
			},
			{
				`{"id1": "", "id2": ""}`,
				[]string{"SeqScan"},
				[][]byte{},
			},
		}
		queryTest(t, bufmgr, parser, tests)
	})

	t.Run("複合プライマリキー、範囲検索", func(t *testing.T) {
		tests := []*QueryTestCase{
			{
				`{"id1": {"$lt": "0010"}, "id2": "0"}`,
				[]string{"Filter", "SeqScan"},
				[][]byte{[]byte("0000"), []byte("0002"), []byte("0004"), []byte("0006"), []byte("0008")},
			},
		}
		queryTest(t, bufmgr, parser, tests)
	})

	t.Run("複合セカンダリキー、完全一致検索", func(t *testing.T) {
		tests := []*QueryTestCase{
			{
				`{"grade": "01", "class": "01", "student_no": "11"}`,
				[]string{"IndexScan"},
				[][]byte{[]byte("0010")},
			},
			{
				`{"grade": "aa", "class": "01", "student_no": "10"}`,
				[]string{"IndexScan"},
				[][]byte{},
			},
			{
				`{"grade": "", "class": "", "student_no": ""}`,
				[]string{"IndexScan"},
				[][]byte{},
			},
		}
		queryTest(t, bufmgr, parser, tests)
	})

	t.Run("複合セカンダリキー、範囲検索", func(t *testing.T) {
		tests := []*QueryTestCase{
			{
				`{"grade": "01", "class": "01", "student_no": {"$lte": "04"}}`,
				[]string{"Filter", "SeqScan"},
				[][]byte{[]byte("0000"), []byte("0001"), []byte("0002"), []byte("0003")},
			},
		}
		queryTest(t, bufmgr, parser, tests)
	})

	t.Run("複合プライマリキーの一部だけ完全一致検索", func(t *testing.T) {
		tests := []*QueryTestCase{
			{
				`{"id1": "0010"}`,
				[]string{"Filter", "SeqScan"},
				[][]byte{[]byte("0010")},
			},
		}
		queryTest(t, bufmgr, parser, tests)
	})

	t.Run("複合セカンダリキーの一部だけ完全一致検索", func(t *testing.T) {
		tests := []*QueryTestCase{
			{
				`{"class": "01", "student_no": "01"}`,
				[]string{"Filter", "SeqScan"},
				[][]byte{[]byte("0000"), []byte("0320"), []byte("0640")},
			},
		}
		queryTest(t, bufmgr, parser, tests)
	})
}

func TestParserNotKey(t *testing.T) {
	bufmgr, parser := openDb("../query_test1.rly")

	t.Run("完全一致検索", func(t *testing.T) {
		tests := []*QueryTestCase{
			{
				`{"name": "YamadaTaro010111"}`,
				[]string{"Filter", "SeqScan"},
				[][]byte{[]byte("0010")},
			},
			{
				`{"3": "YamadaTaro010111"}`,
				[]string{"Filter", "SeqScan"},
				[][]byte{[]byte("0010")},
			},
		}
		queryTest(t, bufmgr, parser, tests)
	})

	t.Run("範囲検索", func(t *testing.T) {
		tests := []*QueryTestCase{
			{
				`{"name": {"$gte": "YamadaTaro010111", "$lte": "YamadaTaro010114"}}`,
				[]string{"Filter", "SeqScan"},
				[][]byte{[]byte("0010"), []byte("0011"), []byte("0012"), []byte("0013")},
			},
		}
		queryTest(t, bufmgr, parser, tests)
	})
}

func TestParserError(t *testing.T) {
	bufmgr, parser := openDb("../query_test1.rly")
	tests := []*QueryErrorTestCase{
		// JSONパースエラー
		{
			`{"id1": "0010}`,
			ErrJsonParse,
		},
		// クエリが連想配列でない
		{
			`["id1", "0010"]`,
			ErrInvalidCondition,
		},
		// 単一プライマリキーの演算子が不正
		{
			`{"id1": {"aaa": "0010"}}`,
			ErrInvalidCondition,
		},
		// 単一プライマリキーの検索条件が不正
		{
			`{"id1": ["0010", "0011"]}`,
			ErrInvalidCondition,
		},
		// 単一セカンダリキーの演算子が不正
		{
			`{"email": {"aaa": "0010@example.com"}}`,
			ErrInvalidCondition,
		},
		// 単一セカンダリキーの検索条件が不正
		{
			`{"email": ["0010@example.com", "0011@example.com"]}`,
			ErrInvalidCondition,
		},
		// 単一セカンダリキーの検索条件の右辺が不正
		{
			`{"email": {"$lt": {"$gt": "0010@example.com"}}}`,
			ErrInvalidCondition,
		},
		// キーでないカラムの演算子が不正
		{
			`{"name": {"aaa": "YamadaTaro010111"}}`,
			ErrInvalidCondition,
		},
		// キーでないカラムの検索条件が不正
		{
			`{"name": ["YamadaTaro010111", "YamadaTaro010112"]}`,
			ErrInvalidCondition,
		},
		// キーでないカラムの検索条件の右辺が不正
		{
			`{"name": {"$lt": {"$gt": "YamadaTaro010111"}}}`,
			ErrInvalidCondition,
		},
		// 存在しないカラム名
		{
			`{"no_exists": "bbb"}`,
			ErrInvalidCondition,
		},
		// 存在しないカラム番号（負の数）
		{
			`{"-1": "bbb"}`,
			ErrInvalidCondition,
		},
		// 存在しないカラム番号（番号が大きすぎる）
		{
			`{"7": "bbb"}`,
			ErrInvalidCondition,
		},
	}
	queryErrorTest(t, bufmgr, parser, tests)
}
