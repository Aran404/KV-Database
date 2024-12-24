package btree

import (
	"bytes"

	btree "github.com/Aran404/KV-Database/database/gbtree"
)

func NewCursor(tree *BTree) *Cursor {
	c := &Cursor{
		tree: tree,
	}
	c.initialize()
	return c
}

func (c *Cursor) initialize() {
	c.node = c.tree.Tree.Root
	c.stack = nil
	c.childIdx = nil
	c.index = 0
	c.current = nil

	// Start from the left most node
	for c.node != nil && len(c.node.Children) > 0 {
		c.stack = append(c.stack, c.node)
		c.childIdx = append(c.childIdx, 0)
		c.node = c.node.Children[0]
	}

	if c.node != nil && len(c.node.BItems) > 0 {
		c.current = &c.node.BItems[c.index]
	}
}

// Next advances the cursor to the next key-value pair. Returns False for stop iteration.
func (c *Cursor) Next() bool {
	if c.node == nil {
		return false
	}

	if c.index+1 < len(c.node.BItems) {
		c.index++
		c.current = &c.node.BItems[c.index]
		return true
	}

	return c.advanceToNextNode()
}

// Prev advances the cursor to the previous key-value pair. Returns False for stop iteration.
func (c *Cursor) Prev() bool {
	if c.node == nil {
		return false
	}

	if c.index-1 >= 0 {
		c.index--
		c.current = &c.node.BItems[c.index]
		return true
	}

	return c.advanceToPrevNode()
}

// Current retrieves the current key-value pair.
func (c *Cursor) Current() (key []byte, value []byte) {
	if c.current == nil {
		return nil, nil
	}
	item := (*c.current).(*KVDump)
	return item.Key, item.Value
}

// First moves the cursor to the first key-value pair.
// In this case the first key-value pair is the left most leaf node.
func (c *Cursor) First() {
	c.node = c.tree.Tree.Root

	for len(c.node.Children) > 0 {
		c.stack = append(c.stack, c.node)
		c.childIdx = append(c.childIdx, 0)
		c.node = c.node.Children[0]
	}

	if len(c.node.BItems) > 0 {
		c.index = 0
		c.current = &c.node.BItems[c.index]
	}
}

// Last moves the cursor to the last key-value pair.
// In this case the last key-value pair is the right most leaf node.
func (c *Cursor) Last() {
	c.node = c.tree.Tree.Root

	for len(c.node.Children) > 0 {
		c.stack = append(c.stack, c.node)
		c.childIdx = append(c.childIdx, len(c.node.Children)-1)
		c.node = c.node.Children[len(c.node.Children)-1]
	}

	if len(c.node.BItems) > 0 {
		c.index = len(c.node.BItems) - 1
		c.current = &c.node.BItems[c.index]
	}
}

// SeekTo moves the cursor to the specified key.
func (c *Cursor) SeekTo(start []byte) {
	c.node = c.tree.Tree.Root
	c.stack = nil
	c.childIdx = nil

	for len(c.node.Children) > 0 {
		i := binarySearch(c.node.BItems, start)
		if i >= len(c.node.BItems) || bytes.Compare(c.node.BItems[i].(*KVDump).Key, start) > 0 {
			c.stack = append(c.stack, c.node)
			c.childIdx = append(c.childIdx, i)
			c.node = c.node.Children[i]
		} else {
			c.index = i
			c.current = &c.node.BItems[c.index]
			return
		}
	}

	i := binarySearch(c.node.BItems, start)
	if i < len(c.node.BItems) && bytes.Compare(c.node.BItems[i].(*KVDump).Key, start) >= 0 {
		c.index = i
		c.current = &c.node.BItems[c.index]
	} else {
		if i < len(c.node.BItems) {
			c.index = i
			c.current = &c.node.BItems[c.index]
		}
	}
}

func (c *Cursor) Delete() *KVDump {
	if c.current == nil {
		return nil
	}

	key := (*c.current).(*KVDump).Key
	// Delete will offset the current index by 1 by convention.
	return c.tree.Delete(key)
}

// Close closes the cursor.
func (c *Cursor) Close() {
	// Delete all references
	c.tree = nil
	c.node = nil
	c.stack = nil
	c.childIdx = nil
	c.current = nil
}

func binarySearch(items btree.Items[btree.Item], key []byte) int {
	low, high := 0, len(items)-1
	for low <= high {
		mid := low + (high-low)/2
		comp := bytes.Compare(items[mid].(*KVDump).Key, key)
		if comp == 0 {
			return mid
		} else if comp < 0 {
			low = mid + 1
		} else {
			high = mid - 1
		}
	}
	return low
}

func (c *Cursor) advanceToPrevNode() bool {
	for {
		// If there are children, move to the previous child.
		if len(c.node.Children) > 0 {
			childIdx := c.index - 1
			if childIdx >= 0 {
				c.stack = append(c.stack, c.node)
				c.childIdx = append(c.childIdx, childIdx)
				c.node = c.node.Children[childIdx]

				// Right Extreme
				for len(c.node.Children) > 0 {
					c.stack = append(c.stack, c.node)
					c.childIdx = append(c.childIdx, len(c.node.Children)-1)
					c.node = c.node.Children[len(c.node.Children)-1]
				}
				c.index = len(c.node.BItems) - 1
				if len(c.node.BItems) > 0 {
					c.current = &c.node.BItems[c.index]
					return true
				}
			}
		}

		// Backtrack
		if len(c.stack) == 0 {
			c.node = nil
			c.current = nil
			return false // end of iteration
		}

		c.node = c.stack[len(c.stack)-1]
		c.stack = c.stack[:len(c.stack)-1]
		c.index = c.childIdx[len(c.childIdx)-1]
		c.childIdx = c.childIdx[:len(c.childIdx)-1]
	}
}

func (c *Cursor) advanceToNextNode() bool {
	for {
		if len(c.node.Children) > 0 {
			childIdx := c.index + 1
			if childIdx < len(c.node.Children) {
				c.stack = append(c.stack, c.node)
				c.childIdx = append(c.childIdx, childIdx)
				c.node = c.node.Children[childIdx]

				// Left Extreme
				for len(c.node.Children) > 0 {
					c.stack = append(c.stack, c.node)
					c.childIdx = append(c.childIdx, 0)
					c.node = c.node.Children[0]
				}
				c.index = 0
				if len(c.node.BItems) > 0 {
					c.current = &c.node.BItems[c.index]
					return true
				}
			}
		}

		if len(c.stack) == 0 {
			c.node = nil
			c.current = nil
			return false
		}

		c.node = c.stack[len(c.stack)-1]
		c.stack = c.stack[:len(c.stack)-1]
		c.index = c.childIdx[len(c.childIdx)-1]
		c.childIdx = c.childIdx[:len(c.childIdx)-1]
	}
}
