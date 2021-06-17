package query

import (
	"my-relly-go/btree"
	"my-relly-go/buffer"
	"my-relly-go/disk"
	"my-relly-go/table"

	"golang.org/x/xerrors"
)

var (
	ErrEndOfIterator = xerrors.New("end of iterator")
)

type Tuple [][]byte
type WhileCondFunc func(Tuple) bool

type TupleSearchMode interface {
	encode() btree.SearchMode
}

type TupleSearchModeStart struct {
}

func (ts *TupleSearchModeStart) encode() btree.SearchMode {
	return &btree.SearchModeStart{}
}

type TupleSearchModeKey struct {
	Key [][]byte
}

func (ts *TupleSearchModeKey) encode() btree.SearchMode {
	return &btree.SearchModeKey{Key: table.EncodeTuple(ts.Key)}
}

type Executor interface {
	Next(bufmgr *buffer.BufferPoolManager) (Tuple, error)
	Finish(bufmgr *buffer.BufferPoolManager)
}

type PlanNode interface {
	Start(bufmgr *buffer.BufferPoolManager) (Executor, error)
	Explain() []string
}

type SeqScan struct {
	TableMetaPageId disk.PageId
	SearchMode      TupleSearchMode
	WhileCond       WhileCondFunc
}

func (s *SeqScan) Start(bufmgr *buffer.BufferPoolManager) (Executor, error) {
	tree := btree.NewBTree(s.TableMetaPageId)
	tableIter, err := tree.Search(bufmgr, s.SearchMode.encode())
	if err != nil {
		return nil, err
	}
	return &ExecSeqScan{
		tableIter,
		s.WhileCond,
	}, nil
}

func (s *SeqScan) Explain() []string {
	return []string{"SeqScan"}
}

type ExecSeqScan struct {
	tableIter *btree.BTreeIter
	whileCond WhileCondFunc
}

func (es *ExecSeqScan) Next(bufmgr *buffer.BufferPoolManager) (Tuple, error) {
	pkeyBytes, tupleBytes, err := es.tableIter.Next(bufmgr)
	if err != nil {
		if err == btree.ErrEndOfIterator {
			return nil, ErrEndOfIterator
		}
		return nil, err
	}
	pkey := [][]byte{}
	pkey = table.DecodeTuple(pkeyBytes, pkey)
	if !(es.whileCond)(pkey) {
		return nil, ErrEndOfIterator
	}
	tuple := pkey
	tuple = table.DecodeTuple(tupleBytes, tuple)
	return tuple, nil
}

func (es *ExecSeqScan) Finish(bufmgr *buffer.BufferPoolManager) {
	es.tableIter.Finish(bufmgr)
}

type Filter struct {
	InnerPlan PlanNode
	Cond      WhileCondFunc
}

func (f *Filter) Start(bufmgr *buffer.BufferPoolManager) (Executor, error) {
	innerIter, err := f.InnerPlan.Start(bufmgr)
	if err != nil {
		return nil, err
	}
	return &ExecFilter{
		innerIter,
		f.Cond,
	}, nil
}

func (f *Filter) Explain() (ret []string) {
	ret = []string{"Filter"}
	ret = append(ret, f.InnerPlan.Explain()...)
	return
}

type ExecFilter struct {
	innerIter Executor
	cond      WhileCondFunc
}

func (ef *ExecFilter) Next(bufmgr *buffer.BufferPoolManager) (Tuple, error) {
	//n := 0
	for {
		tuple, err := ef.innerIter.Next(bufmgr)
		if err != nil {
			if err == btree.ErrEndOfIterator {
				return nil, ErrEndOfIterator
			}
			//log.Println(n)
			return nil, err
		}
		//n++
		if (ef.cond)(tuple) {
			//log.Println(n)
			return tuple, nil
		}
	}
}

func (ef *ExecFilter) Finish(bufmgr *buffer.BufferPoolManager) {
	ef.innerIter.Finish(bufmgr)
}

type IndexScan struct {
	TableMetaPageId disk.PageId
	IndexMetaPageId disk.PageId
	SearchMode      TupleSearchMode
	WhileCond       WhileCondFunc
}

func (s *IndexScan) Start(bufmgr *buffer.BufferPoolManager) (Executor, error) {
	tableTree := btree.NewBTree(s.TableMetaPageId)
	indexTree := btree.NewBTree(s.IndexMetaPageId)
	indexIter, err := indexTree.Search(bufmgr, s.SearchMode.encode())
	if err != nil {
		return nil, err
	}
	return &ExecIndexScan{
		tableTree,
		indexIter,
		s.WhileCond,
	}, nil
}

func (s *IndexScan) Explain() []string {
	return []string{"IndexScan"}
}

type ExecIndexScan struct {
	tableTree *btree.BTree
	indexIter *btree.BTreeIter
	whileCond WhileCondFunc
}

func (es *ExecIndexScan) Next(bufmgr *buffer.BufferPoolManager) (Tuple, error) {
	// セカンダリインデックスの検索を進める
	skeyBytes, pkeyBytes, err := es.indexIter.Next(bufmgr)
	if err != nil {
		if err == btree.ErrEndOfIterator {
			return nil, ErrEndOfIterator
		}
		return nil, err
	}
	skey := [][]byte{}
	skey = table.DecodeTuple(skeyBytes, skey)
	if !(es.whileCond)(skey) {
		return nil, ErrEndOfIterator
	}

	// プライマリキーでテーブルを検索
	tableIter, err := es.tableTree.Search(bufmgr, &btree.SearchModeKey{Key: pkeyBytes})
	if err != nil {
		return nil, err
	}
	defer tableIter.Finish(bufmgr)

	pkeyBytes, tupleBytes, err := tableIter.Next(bufmgr)
	if err != nil {
		return nil, err
	}
	tuple := [][]byte{}
	tuple = table.DecodeTuple(pkeyBytes, tuple)
	tuple = table.DecodeTuple(tupleBytes, tuple)
	return tuple, nil
}

func (es *ExecIndexScan) Finish(bufmgr *buffer.BufferPoolManager) {
	es.indexIter.Finish(bufmgr)
}

type IndexOnlyScan struct {
	IndexMetaPageId disk.PageId
	SearchMode      TupleSearchMode
	WhileCond       WhileCondFunc
}

func (s *IndexOnlyScan) Start(bufmgr *buffer.BufferPoolManager) (Executor, error) {
	indexTree := btree.NewBTree(s.IndexMetaPageId)
	indexIter, err := indexTree.Search(bufmgr, s.SearchMode.encode())
	if err != nil {
		return nil, err
	}
	return &ExecIndexOnlyScan{
		indexIter,
		s.WhileCond,
	}, nil
}

func (s *IndexOnlyScan) Explain() []string {
	return []string{"IndexOnlyScan"}
}

type ExecIndexOnlyScan struct {
	indexIter *btree.BTreeIter
	whileCond WhileCondFunc
}

func (es *ExecIndexOnlyScan) Next(bufmgr *buffer.BufferPoolManager) (Tuple, error) {
	skeyBytes, pkeyBytes, err := es.indexIter.Next(bufmgr)
	if err != nil {
		if err == btree.ErrEndOfIterator {
			return nil, ErrEndOfIterator
		}
		return nil, err
	}
	skey := [][]byte{}
	skey = table.DecodeTuple(skeyBytes, skey)
	if !(es.whileCond)(skey) {
		return nil, ErrEndOfIterator
	}

	tuple := [][]byte{}
	tuple = table.DecodeTuple(pkeyBytes, tuple)
	return tuple, nil
}

func (es *ExecIndexOnlyScan) Finish(bufmgr *buffer.BufferPoolManager) {
	es.indexIter.Finish(bufmgr)
}
