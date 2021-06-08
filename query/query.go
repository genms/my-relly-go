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
}

type SeqScan struct {
	TableMetaPageId disk.PageId
	SearchMode      TupleSearchMode
	WhileCond       func(Tuple) bool
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

type ExecSeqScan struct {
	tableIter *btree.BTreeIter
	whileCond func(Tuple) bool
}

func (es *ExecSeqScan) Next(bufmgr *buffer.BufferPoolManager) (Tuple, error) {
	pkeyBytes, tupleBytes, err := es.tableIter.Next(bufmgr)
	if err != nil {
		if err == btree.ErrEndOfIterator {
			return nil, ErrEndOfIterator
		}
		return nil, err
	}
	pkey := make([][]byte, 0)
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
	Cond      func(Tuple) bool
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

type ExecFilter struct {
	innerIter Executor
	cond      func(Tuple) bool
}

func (ef *ExecFilter) Next(bufmgr *buffer.BufferPoolManager) (Tuple, error) {
	for {
		tuple, err := ef.innerIter.Next(bufmgr)
		if err != nil {
			if err == btree.ErrEndOfIterator {
				return nil, ErrEndOfIterator
			}
			return nil, err
		}
		if (ef.cond)(tuple) {
			return tuple, nil
		}
	}
}

func (ef *ExecFilter) Finish(bufmgr *buffer.BufferPoolManager) {
	ef.innerIter.Finish(bufmgr)
}
