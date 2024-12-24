package page

import (
	"bytes"
	"encoding/gob"

	"github.com/Aran404/KV-Database/database/btree"
	gbtree "github.com/Aran404/KV-Database/database/gbtree"
)

func GetChildOffset(node *gbtree.Node[gbtree.Item]) int64 {
	if node == nil {
		return -1
	}

	// Check if the node is already persisted

}

func NewNode(node *gbtree.Node[gbtree.Item]) *Node {
	nodeID := SnowflakeNode.Generate().Int64()
	n := &Node{NodeID: nodeID}

	if len(node.Children) == 0 {
		n.ChildOffsets = make([]int64, 0)
	} else {
		//
	}

	//

}

func GetAllKVs(node *gbtree.Node[gbtree.Item]) (k [][]byte, v [][]byte) {
	var keys, values [][]byte
	for _, item := range node.BItems {
		i := item.(*btree.KVDump)
		keys = append(keys, i.Key)
		values = append(values, i.Value)
	}

	for _, child := range node.Children {
		ks, vs := GetAllKVs(child)
		keys = append(keys, ks...)
		values = append(values, vs...)
	}
	return keys, values
}

func SerializeNode(node *gbtree.Node[gbtree.Item]) ([][]byte, error) {
	var (
		isLeaf     bool
		serialized [][]byte = make([][]byte, 0)
	)

	if len(node.Children) == 0 {
		isLeaf = true
	} else {
		isLeaf = false
	}

	keys, values := GetAllKVs(node)
	nodeID := SnowflakeNode.Generate().Int64()

	var childIDs []int64
	for _, child := range node.Children {
		childNodeID := SnowflakeNode.Generate().Int64()
		childData, err := SerializeNode(child)
		if err != nil {
			return nil, err
		}

		serialized = append(serialized, childData...)
		childIDs = append(childIDs, childNodeID)
	}

	dn := &DiskNode{
		NodeID:   nodeID,
		IsLeaf:   isLeaf,
		Keys:     keys,
		Values:   values,
		Children: childIDs,
	}

	var r bytes.Buffer
	if err := gob.NewEncoder(&r).Encode(dn); err != nil {
		return nil, err
	}

	return append(serialized, r.Bytes()), nil
}
