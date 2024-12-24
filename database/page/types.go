package page

import (
	"github.com/Aran404/KV-Database/database/btree"
	"github.com/bwmarrin/snowflake"
)

func init() {
	snowflake.Epoch = 1732926762493 // Sat Nov 30 2024 00:32:42 GMT+0000

	node, err := snowflake.NewNode(1)
	if err != nil {
		panic(err)
	}

	SnowflakeNode = node
}

const (
	PageSize btree.Memory = btree.Memory(btree.KiB * 4)

	InternalNode byte = iota
	LeafNode
	RootNode
	DeletedNode
)

var (
	SnowflakeNode *snowflake.Node
)

type Node struct {
	NodeID        int64
	IsRoot        byte
	NodeType      byte
	ParentOffset  int64
	Keys          [][]byte
	ChildOffsets  []int64
	KeyValuePairs []*btree.KVDump
}
