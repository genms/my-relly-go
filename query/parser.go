package query

import (
	"bytes"
	"encoding/json"
	"my-relly-go/btree"
	"my-relly-go/buffer"
	"my-relly-go/disk"
	"my-relly-go/table"
	"regexp"
	"strconv"

	"github.com/thoas/go-funk"
	"golang.org/x/xerrors"
)

type Op string

const (
	OP_LT  Op = "$lt"
	OP_LTE Op = "$lte"
	OP_GT  Op = "$gt"
	OP_GTE Op = "$gte"
	OP_NE  Op = "$ne"
)

func (o Op) valid() bool {
	return o == OP_GT || o == OP_GTE || o == OP_LT || o == OP_LTE || o == OP_NE
}

var (
	ErrJsonParse        = xerrors.New("JSON parse error")
	ErrInvalidCondition = xerrors.New("Invalid condition")
	errCannotMakeConds  = xerrors.New("Cannot make conds")
)

type Parser struct {
	meta *table.Meta
}

func NewParser(bufmgr *buffer.BufferPoolManager) (*Parser, error) {
	tree := btree.NewBTree(disk.PageId(0))
	buf, err := tree.ReadMetaAppArea(bufmgr)
	if err != nil {
		return nil, err
	}
	meta := table.NewMetaFromBytes(buf)
	return &Parser{meta: meta}, nil
}

func (p *Parser) Parse(query string) (PlanNode, error) {
	// JSONデコード
	var decodeData interface{}
	if err := json.Unmarshal([]byte(query), &decodeData); err != nil {
		return nil, ErrJsonParse
	}

	where, ok := decodeData.(map[string]interface{})
	if !ok {
		return nil, ErrInvalidCondition
	}
	where = p.revertColName(where)

	// Scanノードを構築
	scan, where, err := p.buildScanNode(query, where)
	if err != nil {
		return nil, err
	}

	// Filterノードを構築
	nodes := []PlanNode{scan}
	nodes, err = p.buildFilters(query, where, nodes)
	if err != nil {
		return nil, err
	}

	// 一番手前のノードを返す
	return nodes[len(nodes)-1], nil
}

func (p *Parser) revertColName(where map[string]interface{}) map[string]interface{} {
	for colStr, cond := range where {
		r := regexp.MustCompile(`^\d+$`)
		if !r.MatchString(colStr) {
			if col := funk.IndexOf(p.meta.ColNames, colStr); col >= 0 {
				delete(where, colStr)
				where[strconv.Itoa(col)] = cond
			}
		}
	}
	return where
}

func (p *Parser) buildScanNode(query string, where map[string]interface{}) (PlanNode, map[string]interface{}, error) {
	var scan PlanNode = nil
	var err error

	// プライマリキーに対する検索条件をもとにScanノードを構築
	if p.meta.NumKeyElems == 1 {
		// プライマリキーが単一キーの場合
		scan, where, err = p.buildSinglePKeyScanNode(query, where)
	} else {
		// プライマリキーが複合キーの場合
		scan, where, err = p.buildCompositePKeyScanNode(query, where)
	}
	if err != nil {
		return nil, nil, err
	}
	if scan != nil {
		return scan, where, nil
	}

	// セカンダリキーに対する検索条件をもとにScanノードを構築
	uniqueIndices := p.meta.GetUniqueIndices()
	for indexNo, uniqueIndex := range uniqueIndices {
		if len(uniqueIndex) == 1 {
			// セカンダリキーが単一キーの場合
			scan, where, err = p.buildSingleSKeyScanNode(query, where, indexNo, uniqueIndex)
		} else {
			// セカンダリキーが複合キーの場合
			scan, where, err = p.buildCompositeSKeyScanNode(query, where, indexNo, uniqueIndex)
		}
		if err != nil {
			return nil, nil, err
		}
		if scan != nil {
			return scan, where, nil
		}
	}

	// ここまでScanノードが決まらなかったら、先頭からのSeqScanを使う
	scan = &SeqScan{
		TableMetaPageId: disk.PageId(0),
		SearchMode:      &TupleSearchModeStart{},
		WhileCond: func(Tuple) bool {
			return true
		},
	}
	return scan, where, nil
}

func (p *Parser) buildSinglePKeyScanNode(query string, where map[string]interface{}) (PlanNode, map[string]interface{}, error) {
	var scan PlanNode = nil

	// プライマリキーの検索条件が指定されているか
	pkeyStr := "0"
	if _, ok := where[pkeyStr]; !ok {
		return nil, where, nil
	}
	switch v := where[pkeyStr].(type) {
	case string: // 完全一致検索
		tupleSearchMode, whileCond, err := p.makeEqualCondWithSingleKey(v)
		if err != nil {
			return nil, nil, err
		}
		scan = &SeqScan{
			TableMetaPageId: disk.PageId(0),
			SearchMode:      tupleSearchMode,
			WhileCond:       whileCond,
		}
		delete(where, pkeyStr)

	case map[string]interface{}: // 演算子による検索
		tupleSearchMode, whileCond, err := p.makeRangeCondWithSingleKey(v)
		if err != nil {
			return nil, nil, err
		}
		scan = &SeqScan{
			TableMetaPageId: disk.PageId(0),
			SearchMode:      tupleSearchMode,
			WhileCond:       whileCond,
		}

	default:
		return nil, nil, ErrInvalidCondition
	}
	return scan, where, nil
}

func (p *Parser) buildCompositePKeyScanNode(query string, where map[string]interface{}) (PlanNode, map[string]interface{}, error) {
	var scan PlanNode = nil

	// プライマリキーの対象カラムすべてで完全一致検索がされているか
	numKeyElems := int(p.meta.NumKeyElems)
	index := make([]int, numKeyElems)
	for pkey := 0; pkey < numKeyElems; pkey++ {
		index[pkey] = pkey
	}
	tupleSearchMode, whileCond, err := p.makeCondWithCompositeKey(index, where)
	if err != nil {
		if err == errCannotMakeConds {
			return nil, where, nil
		}
		return nil, nil, err
	}

	scan = &SeqScan{
		TableMetaPageId: disk.PageId(0),
		SearchMode:      tupleSearchMode,
		WhileCond:       whileCond,
	}
	for pkey := 0; pkey < numKeyElems; pkey++ {
		pkeyStr := strconv.Itoa(pkey)
		delete(where, pkeyStr)
	}
	return scan, where, nil
}

func (p *Parser) buildSingleSKeyScanNode(query string, where map[string]interface{}, indexNo int, uniqueIndex []int) (PlanNode, map[string]interface{}, error) {
	var scan PlanNode = nil

	// セカンダリキーの検索条件が指定されているか
	skey := int(uniqueIndex[0])
	skeyStr := strconv.Itoa(skey)
	if _, ok := where[skeyStr]; !ok {
		return nil, where, nil
	}

	switch v := where[skeyStr].(type) {
	case string: // 完全一致検索
		tupleSearchMode, whileCond, err := p.makeEqualCondWithSingleKey(v)
		if err != nil {
			return nil, nil, err
		}
		scan = &IndexScan{
			TableMetaPageId: disk.PageId(0),
			IndexMetaPageId: disk.PageId((indexNo + 1) * 2),
			SearchMode:      tupleSearchMode,
			WhileCond:       whileCond,
		}
		delete(where, skeyStr)

	case map[string]interface{}: // 演算子による検索
		tupleSearchMode, whileCond, err := p.makeRangeCondWithSingleKey(v)
		if err != nil {
			return nil, nil, err
		}
		scan = &IndexScan{
			TableMetaPageId: disk.PageId(0),
			IndexMetaPageId: disk.PageId((indexNo + 1) * 2),
			SearchMode:      tupleSearchMode,
			WhileCond:       whileCond,
		}

	default:
		return nil, nil, ErrInvalidCondition
	}

	return scan, where, nil
}

func (p *Parser) buildCompositeSKeyScanNode(query string, where map[string]interface{}, indexNo int, uniqueIndex []int) (PlanNode, map[string]interface{}, error) {
	var scan PlanNode = nil

	// セカンダリキーの対象カラムすべてで完全一致検索がされているか
	tupleSearchMode, whileCond, err := p.makeCondWithCompositeKey(uniqueIndex, where)
	if err != nil {
		if err == errCannotMakeConds {
			return nil, where, nil
		}
		return nil, nil, err
	}

	scan = &IndexScan{
		TableMetaPageId: disk.PageId(0),
		IndexMetaPageId: disk.PageId((indexNo + 1) * 2),
		SearchMode:      tupleSearchMode,
		WhileCond:       whileCond,
	}
	for _, skey := range uniqueIndex {
		skeyStr := strconv.Itoa(int(skey))
		delete(where, skeyStr)
	}
	return scan, where, nil
}

func (p *Parser) buildFilters(query string, where map[string]interface{}, nodes []PlanNode) ([]PlanNode, error) {
	whileCondFuncs := []WhileCondFunc{}
	for key, value := range where {
		col, err := strconv.Atoi(key)
		if err != nil {
			return nil, ErrInvalidCondition
		}
		// カラム存在チェック
		if col < 0 || int(p.meta.NumCols) <= col {
			return nil, ErrInvalidCondition
		}

		switch v := value.(type) {
		case string: // 完全一致検索
			whileCondFuncs = append(whileCondFuncs, func(record Tuple) bool {
				return bytes.Equal(record[col], []byte(v))
			})

		case map[string]interface{}: // 演算子による検索
			for opStr, right := range v {
				r, ok := right.(string)
				if !ok {
					return nil, ErrInvalidCondition
				}

				op := Op(opStr)
				switch op {
				case OP_LT:
					whileCondFuncs = append(whileCondFuncs, func(record Tuple) bool {
						return bytes.Compare(record[col], []byte(r)) < 0
					})
				case OP_LTE:
					whileCondFuncs = append(whileCondFuncs, func(record Tuple) bool {
						return bytes.Compare(record[col], []byte(r)) <= 0
					})
				case OP_GT:
					whileCondFuncs = append(whileCondFuncs, func(record Tuple) bool {
						return bytes.Compare(record[col], []byte(r)) > 0
					})
				case OP_GTE:
					whileCondFuncs = append(whileCondFuncs, func(record Tuple) bool {
						return bytes.Compare(record[col], []byte(r)) >= 0
					})
				case OP_NE:
					whileCondFuncs = append(whileCondFuncs, func(record Tuple) bool {
						return !bytes.Equal(record[col], []byte(r))
					})
				default:
					return nil, ErrInvalidCondition
				}
			}

		default:
			return nil, ErrInvalidCondition
		}
	}
	if len(whileCondFuncs) == 0 {
		return nodes, nil
	}

	// Filterを追加
	nodes = append(nodes, &Filter{
		Cond: func(record Tuple) bool {
			for _, f := range whileCondFuncs {
				if !(f)(record) {
					return false
				}
			}
			return true
		},
		InnerPlan: nodes[len(nodes)-1],
	})
	return nodes, nil
}

func (p *Parser) makeEqualCondWithSingleKey(searchValue string) (TupleSearchMode, WhileCondFunc, error) {
	tupleSearchMode := &TupleSearchModeKey{Key: [][]byte{[]byte(searchValue)}}
	whileCond := func(tuple Tuple) bool {
		return bytes.Equal(tuple[0], []byte(searchValue))
	}
	return tupleSearchMode, whileCond, nil
}

func (p *Parser) makeRangeCondWithSingleKey(exprs map[string]interface{}) (TupleSearchMode, WhileCondFunc, error) {
	var searchKeyBegin []byte = nil
	var searchKeyEnd []byte = nil

	for opStr, right := range exprs {
		op := Op(opStr)
		if !op.valid() {
			return nil, nil, ErrInvalidCondition
		}

		switch r := right.(type) {
		case string:
			if op == OP_GT || op == OP_GTE {
				if searchKeyBegin == nil || bytes.Compare(searchKeyBegin, []byte(r)) > 0 {
					searchKeyBegin = []byte(r)
				}
			} else if op == OP_LT || op == OP_LTE {
				if searchKeyEnd == nil || bytes.Compare(searchKeyEnd, []byte(r)) < 0 {
					searchKeyEnd = []byte(r)
				}
			}
		default:
			return nil, nil, ErrInvalidCondition
		}
	}

	var tupleSearchMode TupleSearchMode = nil
	if searchKeyBegin == nil {
		tupleSearchMode = &TupleSearchModeStart{}
	} else {
		tupleSearchMode = &TupleSearchModeKey{Key: [][]byte{searchKeyBegin}}
	}

	var whileCond WhileCondFunc = nil
	if searchKeyEnd == nil {
		whileCond = func(tuple Tuple) bool {
			return true
		}
	} else {
		whileCond = func(tuple Tuple) bool {
			return bytes.Compare(tuple[0], searchKeyEnd) <= 0
		}
	}
	return tupleSearchMode, whileCond, nil
}

func (p *Parser) makeCondWithCompositeKey(index []int, where map[string]interface{}) (TupleSearchMode, WhileCondFunc, error) {
	searchKeys := [][]byte{}
	whileCondFuncs := []WhileCondFunc{}

	// インデックスのキーすべてで完全一致検索がされているか
	for i, skey := range index {
		skeyStr := strconv.Itoa(int(skey))
		if _, ok := where[skeyStr]; !ok {
			break
		}

		if v, ok := where[skeyStr].(string); ok {
			searchKeys = append(searchKeys, []byte(v))
			{
				ii := i
				whileCondFuncs = append(whileCondFuncs, func(skeyTuple Tuple) bool {
					return bytes.Equal(skeyTuple[ii], []byte(v))
				})
			}
		}
	}
	if len(index) != len(searchKeys) {
		return nil, nil, errCannotMakeConds
	}

	var tupleSearchMode TupleSearchMode = &TupleSearchModeKey{Key: searchKeys}
	var whileCond WhileCondFunc = func(tuple Tuple) bool {
		for _, f := range whileCondFuncs {
			if !(f)(tuple) {
				return false
			}
		}
		return true
	}
	return tupleSearchMode, whileCond, nil
}
