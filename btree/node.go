package btree

import (
	"unsafe"
)

const NODE_TYPE_LEAF string = "LEAF    "
const NODE_TYPE_BRANCH string = "BRANCH  "

type NodeHeader struct {
	nodeType [8]byte
}

func (h *NodeHeader) NodeTypeString() string {
	return string(h.nodeType[:])
}

type Node struct {
	header *NodeHeader
	body   []byte
}

func NewNode(bytes []byte) *Node {
	node := Node{}
	headerSize := int(unsafe.Sizeof(*node.header))
	if headerSize+1 > len(bytes) {
		panic("node header must be aligned")
	}

	node.header = (*NodeHeader)(unsafe.Pointer(&bytes[0]))
	node.body = bytes[headerSize:]
	return &node
}

func (n *Node) InitializeAsLeaf() {
	copy(n.header.nodeType[:], []byte(NODE_TYPE_LEAF))
}

func (n *Node) InitializeAsBranch() {
	copy(n.header.nodeType[:], []byte(NODE_TYPE_BRANCH))
}
