// Copyright 2014-2022 Google Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//go:build go1.18
// +build go1.18

// In Go 1.18 and beyond, a BTreeG generic is created, and BTree is a specific
// instantiation of that generic for the Item interface, with a backwards-
// compatible API.  Before go1.18, generics are not supported,
// and BTree is just an implementation based around the Item interface.

// Package btree implements in-memory B-Trees of arbitrary degree.
//
// btree implements an in-memory B-Tree for use as an ordered data structure.
// It is not meant for persistent storage solutions.
//
// It has a flatter structure than an equivalent red-black or other binary tree,
// which in some cases yields better memory usage and/or performance.
// See some discussion on the matter here:
//
//	http://google-opensource.blogspot.com/2013/01/c-containers-that-save-memory-and-time.html
//
// Note, though, that this project is in no way related to the C++ B-Tree
// implementation written about there.
//
// Within this tree, each node contains a slice of items and a (possibly nil)
// slice of children.  For basic numeric values or raw structs, this can cause
// efficiency differences when compared to equivalent C++ template code that
// stores values in arrays within the node:
//   - Due to the overhead of storing values as interfaces (each
//     value needs to be stored as the value itself, then 2 words for the
//     interface pointing to that value and its type), resulting in higher
//     memory use.
//   - Since interfaces can point to values anywhere in memory, values are
//     most likely not stored in contiguous blocks, resulting in a higher
//     number of cache misses.
//
// These issues don't tend to matter, though, when working with strings or other
// heap-allocated structures, since C++-equivalent structures also must store
// pointers and also distribute their values across the heap.
//
// This implementation is designed to be a drop-in replacement to gollrb.LLRB
// trees, (http://github.com/petar/gollrb), an excellent and probably the most
// widely used ordered tree implementation in the Go ecosystem currently.
// Its functions, therefore, exactly mirror those of
// llrb.LLRB where possible.  Unlike gollrb, though, we currently don't
// support storing multiple equivalent values.
//
// There are two implementations; those suffixed with 'G' are generics, usable
// for any type, and require a passed-in "less" function to define their ordering.
// Those without this prefix are specific to the 'Item' interface, and use
// its 'Less' function for ordering.
package btree

import (
	"fmt"
	"io"
	"sort"
	"strings"
	"sync"
)

// Item represents a single object in the tree.
type Item interface {
	// Less tests whether the current item is less than the given argument.
	//
	// This must provide a strict weak ordering.
	// If !a.Less(b) && !b.Less(a), we treat this to mean a == b (i.e. we can only
	// hold one of either a or b in the tree).
	Less(than Item) bool
}

const (
	DefaultFreeListSize = 32
)

// FreeListG represents a free list of btree nodes. By default each
// BTree has its own FreeList, but multiple BTrees can share the same
// FreeList, in particular when they're created with Clone.
// Two Btrees using the same freelist are safe for concurrent write access.
type FreeListG[T any] struct {
	mu       sync.Mutex
	freelist []*Node[T]
}

// NewFreeListG creates a new free list.
// size is the maximum size of the returned free list.
func NewFreeListG[T any](size int) *FreeListG[T] {
	return &FreeListG[T]{freelist: make([]*Node[T], 0, size)}
}

func (f *FreeListG[T]) newNode() (n *Node[T]) {
	f.mu.Lock()
	index := len(f.freelist) - 1
	if index < 0 {
		f.mu.Unlock()
		return new(Node[T])
	}
	n = f.freelist[index]
	f.freelist[index] = nil
	f.freelist = f.freelist[:index]
	f.mu.Unlock()
	return
}

func (f *FreeListG[T]) freeNode(n *Node[T]) (out bool) {
	f.mu.Lock()
	if len(f.freelist) < cap(f.freelist) {
		f.freelist = append(f.freelist, n)
		out = true
	}
	f.mu.Unlock()
	return
}

// ItemIteratorG allows callers of {A/De}scend* to iterate in-order over portions of
// the tree.  When this function returns false, iteration will stop and the
// associated Ascend* function will immediately return.
type ItemIteratorG[T any] func(item T) bool

const (
	NextItem int8 = iota
	PrevItem
	Finish
)

type ItemIteratorHandleG[T any] func(item T) int8

// Ordered represents the set of types for which the '<' operator work.
type Ordered interface {
	~int | ~int8 | ~int16 | ~int32 | ~int64 | ~uint | ~uint8 | ~uint16 | ~uint32 | ~uint64 | ~float32 | ~float64 | ~string
}

// Less[T] returns a default LessFunc that uses the '<' operator for types that support it.
func Less[T Ordered]() LessFunc[T] {
	return func(a, b T) bool { return a < b }
}

// NewOrderedG creates a new B-Tree for ordered types.
func NewOrderedG[T Ordered](degree int) *BTreeG[T] {
	return NewG[T](degree, Less[T]())
}

// NewG creates a new B-Tree with the given degree.
//
// NewG(2), for example, will create a 2-3-4 tree (each node contains 1-3 items
// and 2-4 children).
//
// The passed-in LessFunc determines how objects of type T are ordered.
func NewG[T any](degree int, less LessFunc[T]) *BTreeG[T] {
	return NewWithFreeListG(degree, less, NewFreeListG[T](DefaultFreeListSize))
}

// NewWithFreeListG creates a new B-Tree that uses the given node free list.
func NewWithFreeListG[T any](degree int, less LessFunc[T], f *FreeListG[T]) *BTreeG[T] {
	if degree <= 1 {
		panic("bad degree")
	}
	return &BTreeG[T]{
		Degree: degree,
		Cow:    &CopyOnWriteContext[T]{freelist: f, less: less},
	}
}

// Items stores Items in a node.
type Items[T any] []T

// insertAt inserts a value into the given index, pushing all subsequent values
// forward.
func (s *Items[T]) insertAt(index int, item T) {
	var zero T
	*s = append(*s, zero)
	if index < len(*s) {
		copy((*s)[index+1:], (*s)[index:])
	}
	(*s)[index] = item
}

// removeAt removes a value at a given index, pulling all subsequent values
// back.
func (s *Items[T]) removeAt(index int) T {
	item := (*s)[index]
	copy((*s)[index:], (*s)[index+1:])
	var zero T
	(*s)[len(*s)-1] = zero
	*s = (*s)[:len(*s)-1]
	return item
}

// pop removes and returns the last element in the list.
func (s *Items[T]) pop() (out T) {
	index := len(*s) - 1
	out = (*s)[index]
	var zero T
	(*s)[index] = zero
	*s = (*s)[:index]
	return
}

// truncate truncates this instance at index so that it contains only the
// first index items. index must be less than or equal to length.
func (s *Items[T]) truncate(index int) {
	var toClear Items[T]
	*s, toClear = (*s)[:index], (*s)[index:]
	var zero T
	for i := 0; i < len(toClear); i++ {
		toClear[i] = zero
	}
}

// find returns the index where the given item should be inserted into this
// list.  'found' is true if the item already exists in the list at the given
// index.
func (s Items[T]) find(item T, less func(T, T) bool) (index int, found bool) {
	i := sort.Search(len(s), func(i int) bool {
		return less(item, s[i])
	})
	if i > 0 && !less(s[i-1], item) {
		return i - 1, true
	}
	return i, false
}

// Node is an internal Node in a tree.
//
// It must at all times maintain the invariant that either
//   - len(children) == 0, len(items) unconstrained
//   - len(children) == len(items) + 1
type Node[T any] struct {
	BItems   Items[T]
	Children Items[*Node[T]]
	Cow      *CopyOnWriteContext[T]
}

func (n *Node[T]) mutableFor(cow *CopyOnWriteContext[T]) *Node[T] {
	if n.Cow == cow {
		return n
	}
	out := cow.newNode()
	if cap(out.BItems) >= len(n.BItems) {
		out.BItems = out.BItems[:len(n.BItems)]
	} else {
		out.BItems = make(Items[T], len(n.BItems), cap(n.BItems))
	}
	copy(out.BItems, n.BItems)
	// Copy children
	if cap(out.Children) >= len(n.Children) {
		out.Children = out.Children[:len(n.Children)]
	} else {
		out.Children = make(Items[*Node[T]], len(n.Children), cap(n.Children))
	}
	copy(out.Children, n.Children)
	return out
}

func (n *Node[T]) mutableChild(i int) *Node[T] {
	c := n.Children[i].mutableFor(n.Cow)
	n.Children[i] = c
	return c
}

// split splits the given node at the given index.  The current node shrinks,
// and this function returns the item that existed at that index and a new node
// containing all items/children after it.
func (n *Node[T]) split(i int) (T, *Node[T]) {
	item := n.BItems[i]
	next := n.Cow.newNode()
	next.BItems = append(next.BItems, n.BItems[i+1:]...)
	n.BItems.truncate(i)
	if len(n.Children) > 0 {
		next.Children = append(next.Children, n.Children[i+1:]...)
		n.Children.truncate(i + 1)
	}
	return item, next
}

// maybeSplitChild checks if a child should be split, and if so splits it.
// Returns whether or not a split occurred.
func (n *Node[T]) maybeSplitChild(i, maxItems int) bool {
	if len(n.Children[i].BItems) < maxItems {
		return false
	}
	first := n.mutableChild(i)
	item, second := first.split(maxItems / 2)
	n.BItems.insertAt(i, item)
	n.Children.insertAt(i+1, second)
	return true
}

// insert inserts an item into the subtree rooted at this node, making sure
// no nodes in the subtree exceed maxItems items.  Should an equivalent item be
// be found/replaced by insert, it will be returned.
func (n *Node[T]) insert(item T, maxItems int) (_ T, _ bool) {
	i, found := n.BItems.find(item, n.Cow.less)
	if found {
		out := n.BItems[i]
		n.BItems[i] = item
		return out, true
	}
	if len(n.Children) == 0 {
		n.BItems.insertAt(i, item)
		return
	}
	if n.maybeSplitChild(i, maxItems) {
		inTree := n.BItems[i]
		switch {
		case n.Cow.less(item, inTree):
			// no change, we want first split node
		case n.Cow.less(inTree, item):
			i++ // we want second split node
		default:
			out := n.BItems[i]
			n.BItems[i] = item
			return out, true
		}
	}
	return n.mutableChild(i).insert(item, maxItems)
}

// get finds the given key in the subtree and returns it.
func (n *Node[T]) get(key T) (_ T, _ bool) {
	i, found := n.BItems.find(key, n.Cow.less)
	if found {
		return n.BItems[i], true
	} else if len(n.Children) > 0 {
		return n.Children[i].get(key)
	}
	return
}

// min returns the first item in the subtree.
func min[T any](n *Node[T]) (_ T, found bool) {
	if n == nil {
		return
	}
	for len(n.Children) > 0 {
		n = n.Children[0]
	}
	if len(n.BItems) == 0 {
		return
	}
	return n.BItems[0], true
}

// max returns the last item in the subtree.
func max[T any](n *Node[T]) (_ T, found bool) {
	if n == nil {
		return
	}
	for len(n.Children) > 0 {
		n = n.Children[len(n.Children)-1]
	}
	if len(n.BItems) == 0 {
		return
	}
	return n.BItems[len(n.BItems)-1], true
}

// toRemove details what item to remove in a node.remove call.
type toRemove int

const (
	removeItem toRemove = iota // removes the given item
	removeMin                  // removes smallest item in the subtree
	removeMax                  // removes largest item in the subtree
)

// remove removes an item from the subtree rooted at this node.
func (n *Node[T]) remove(item T, minItems int, typ toRemove) (_ T, _ bool) {
	var i int
	var found bool
	switch typ {
	case removeMax:
		if len(n.Children) == 0 {
			return n.BItems.pop(), true
		}
		i = len(n.BItems)
	case removeMin:
		if len(n.Children) == 0 {
			return n.BItems.removeAt(0), true
		}
		i = 0
	case removeItem:
		i, found = n.BItems.find(item, n.Cow.less)
		if len(n.Children) == 0 {
			if found {
				return n.BItems.removeAt(i), true
			}
			return
		}
	default:
		panic("invalid type")
	}
	// If we get to here, we have children.
	if len(n.Children[i].BItems) <= minItems {
		return n.growChildAndRemove(i, item, minItems, typ)
	}
	child := n.mutableChild(i)
	// Either we had enough items to begin with, or we've done some
	// merging/stealing, because we've got enough now and we're ready to return
	// stuff.
	if found {
		// The item exists at index 'i', and the child we've selected can give us a
		// predecessor, since if we've gotten here it's got > minItems items in it.
		out := n.BItems[i]
		// We use our special-case 'remove' call with typ=maxItem to pull the
		// predecessor of item i (the rightmost leaf of our immediate left child)
		// and set it into where we pulled the item from.
		var zero T
		n.BItems[i], _ = child.remove(zero, minItems, removeMax)
		return out, true
	}
	// Final recursive call.  Once we're here, we know that the item isn't in this
	// node and that the child is big enough to remove from.
	return child.remove(item, minItems, typ)
}

// growChildAndRemove grows child 'i' to make sure it's possible to remove an
// item from it while keeping it at minItems, then calls remove to actually
// remove it.
//
// Most documentation says we have to do two sets of special casing:
//  1. item is in this node
//  2. item is in child
//
// In both cases, we need to handle the two subcases:
//
//	A) node has enough values that it can spare one
//	B) node doesn't have enough values
//
// For the latter, we have to check:
//
//	a) left sibling has node to spare
//	b) right sibling has node to spare
//	c) we must merge
//
// To simplify our code here, we handle cases #1 and #2 the same:
// If a node doesn't have enough items, we make sure it does (using a,b,c).
// We then simply redo our remove call, and the second time (regardless of
// whether we're in case 1 or 2), we'll have enough items and can guarantee
// that we hit case A.
func (n *Node[T]) growChildAndRemove(i int, item T, minItems int, typ toRemove) (T, bool) {
	if i > 0 && len(n.Children[i-1].BItems) > minItems {
		// Steal from left child
		child := n.mutableChild(i)
		stealFrom := n.mutableChild(i - 1)
		stolenItem := stealFrom.BItems.pop()
		child.BItems.insertAt(0, n.BItems[i-1])
		n.BItems[i-1] = stolenItem
		if len(stealFrom.Children) > 0 {
			child.Children.insertAt(0, stealFrom.Children.pop())
		}
	} else if i < len(n.BItems) && len(n.Children[i+1].BItems) > minItems {
		// steal from right child
		child := n.mutableChild(i)
		stealFrom := n.mutableChild(i + 1)
		stolenItem := stealFrom.BItems.removeAt(0)
		child.BItems = append(child.BItems, n.BItems[i])
		n.BItems[i] = stolenItem
		if len(stealFrom.Children) > 0 {
			child.Children = append(child.Children, stealFrom.Children.removeAt(0))
		}
	} else {
		if i >= len(n.BItems) {
			i--
		}
		child := n.mutableChild(i)
		// merge with right child
		mergeItem := n.BItems.removeAt(i)
		mergeChild := n.Children.removeAt(i + 1)
		child.BItems = append(child.BItems, mergeItem)
		child.BItems = append(child.BItems, mergeChild.BItems...)
		child.Children = append(child.Children, mergeChild.Children...)
		n.Cow.freeNode(mergeChild)
	}
	return n.remove(item, minItems, typ)
}

type direction int

const (
	descend = direction(-1)
	ascend  = direction(+1)
)

type OptionalItem[T any] struct {
	item  T
	valid bool
}

func optional[T any](item T) OptionalItem[T] {
	return OptionalItem[T]{item: item, valid: true}
}
func empty[T any]() OptionalItem[T] {
	return OptionalItem[T]{}
}

// iterate provides a simple method for iterating over elements in the tree.
//
// When ascending, the 'start' should be less than 'stop' and when descending,
// the 'start' should be greater than 'stop'. Setting 'includeStart' to true
// will force the iterator to include the first item when it equals 'start',
// thus creating a "greaterOrEqual" or "lessThanEqual" rather than just a
// "greaterThan" or "lessThan" queries.
func (n *Node[T]) iterate(dir direction, start, stop OptionalItem[T], includeStart bool, hit bool, iter ItemIteratorG[T]) (bool, bool) {
	var ok, found bool
	var index int
	switch dir {
	case ascend:
		if start.valid {
			index, _ = n.BItems.find(start.item, n.Cow.less)
		}
		for i := index; i < len(n.BItems); i++ {
			if len(n.Children) > 0 {
				if hit, ok = n.Children[i].iterate(dir, start, stop, includeStart, hit, iter); !ok {
					return hit, false
				}
			}
			if !includeStart && !hit && start.valid && !n.Cow.less(start.item, n.BItems[i]) {
				hit = true
				continue
			}
			hit = true
			if stop.valid && !n.Cow.less(n.BItems[i], stop.item) {
				return hit, false
			}
			if !iter(n.BItems[i]) {
				return hit, false
			}
		}
		if len(n.Children) > 0 {
			if hit, ok = n.Children[len(n.Children)-1].iterate(dir, start, stop, includeStart, hit, iter); !ok {
				return hit, false
			}
		}
	case descend:
		if start.valid {
			index, found = n.BItems.find(start.item, n.Cow.less)
			if !found {
				index = index - 1
			}
		} else {
			index = len(n.BItems) - 1
		}
		for i := index; i >= 0; i-- {
			if start.valid && !n.Cow.less(n.BItems[i], start.item) {
				if !includeStart || hit || n.Cow.less(start.item, n.BItems[i]) {
					continue
				}
			}
			if len(n.Children) > 0 {
				if hit, ok = n.Children[i+1].iterate(dir, start, stop, includeStart, hit, iter); !ok {
					return hit, false
				}
			}
			if stop.valid && !n.Cow.less(stop.item, n.BItems[i]) {
				return hit, false //	continue
			}
			hit = true
			if !iter(n.BItems[i]) {
				return hit, false
			}
		}
		if len(n.Children) > 0 {
			if hit, ok = n.Children[0].iterate(dir, start, stop, includeStart, hit, iter); !ok {
				return hit, false
			}
		}
	}
	return hit, true
}

// print is used for testing/debugging purposes.
func (n *Node[T]) print(w io.Writer, level int) {
	fmt.Fprintf(w, "%sNODE:%v\n", strings.Repeat("  ", level), n.BItems)
	for _, c := range n.Children {
		c.print(w, level+1)
	}
}

// BTreeG is a generic implementation of a B-Tree.
//
// BTreeG stores items of type T in an ordered structure, allowing easy insertion,
// removal, and iteration.
//
// Write operations are not safe for concurrent mutation by multiple
// goroutines, but Read operations are.
type BTreeG[T any] struct {
	Degree int
	Length int
	Root   *Node[T]
	Cow    *CopyOnWriteContext[T]
}

// LessFunc[T] determines how to order a type 'T'.  It should implement a strict
// ordering, and should return true if within that ordering, 'a' < 'b'.
type LessFunc[T any] func(a, b T) bool

// CopyOnWriteContext pointers determine node ownership... a tree with a write
// context equivalent to a node's write context is allowed to modify that node.
// A tree whose write context does not match a node's is not allowed to modify
// it, and must create a new, writable copy (IE: it's a Clone).
//
// When doing any write operation, we maintain the invariant that the current
// node's context is equal to the context of the tree that requested the write.
// We do this by, before we descend into any node, creating a copy with the
// correct context if the contexts don't match.
//
// Since the node we're currently visiting on any write has the requesting
// tree's context, that node is modifiable in place.  Children of that node may
// not share context, but before we descend into them, we'll make a mutable
// copy.
type CopyOnWriteContext[T any] struct {
	freelist *FreeListG[T]
	less     LessFunc[T]
}

// Clone clones the btree, lazily.  Clone should not be called concurrently,
// but the original tree (t) and the new tree (t2) can be used concurrently
// once the Clone call completes.
//
// The internal tree structure of b is marked read-only and shared between t and
// t2.  Writes to both t and t2 use copy-on-write logic, creating new nodes
// whenever one of b's original nodes would have been modified.  Read operations
// should have no performance degredation.  Write operations for both t and t2
// will initially experience minor slow-downs caused by additional allocs and
// copies due to the aforementioned copy-on-write logic, but should converge to
// the original performance characteristics of the original tree.
func (t *BTreeG[T]) Clone() (t2 *BTreeG[T]) {
	// Create two entirely new copy-on-write contexts.
	// This operation effectively creates three trees:
	//   the original, shared nodes (old b.cow)
	//   the new b.cow nodes
	//   the new out.cow nodes
	cow1, cow2 := *t.Cow, *t.Cow
	out := *t
	t.Cow = &cow1
	out.Cow = &cow2
	return &out
}

// maxItems returns the max number of items to allow per node.
func (t *BTreeG[T]) maxItems() int {
	return t.Degree*2 - 1
}

// minItems returns the min number of items to allow per node (ignored for the
// root node).
func (t *BTreeG[T]) minItems() int {
	return t.Degree - 1
}

func (c *CopyOnWriteContext[T]) newNode() (n *Node[T]) {
	n = c.freelist.newNode()
	n.Cow = c
	return
}

type freeType int

const (
	ftFreelistFull freeType = iota // node was freed (available for GC, not stored in freelist)
	ftStored                       // node was stored in the freelist for later use
	ftNotOwned                     // node was ignored by COW, since it's owned by another one
)

// freeNode frees a node within a given COW context, if it's owned by that
// context.  It returns what happened to the node (see freeType const
// documentation).
func (c *CopyOnWriteContext[T]) freeNode(n *Node[T]) freeType {
	if n.Cow == c {
		// clear to allow GC
		n.BItems.truncate(0)
		n.Children.truncate(0)
		n.Cow = nil
		if c.freelist.freeNode(n) {
			return ftStored
		} else {
			return ftFreelistFull
		}
	} else {
		return ftNotOwned
	}
}

// ReplaceOrInsert adds the given item to the tree.  If an item in the tree
// already equals the given one, it is removed from the tree and returned,
// and the second return value is true.  Otherwise, (zeroValue, false)
//
// nil cannot be added to the tree (will panic).
func (t *BTreeG[T]) ReplaceOrInsert(item T) (_ T, _ bool) {
	if t.Root == nil {
		t.Root = t.Cow.newNode()
		t.Root.BItems = append(t.Root.BItems, item)
		t.Length++
		return
	} else {
		t.Root = t.Root.mutableFor(t.Cow)
		if len(t.Root.BItems) >= t.maxItems() {
			item2, second := t.Root.split(t.maxItems() / 2)
			oldroot := t.Root
			t.Root = t.Cow.newNode()
			t.Root.BItems = append(t.Root.BItems, item2)
			t.Root.Children = append(t.Root.Children, oldroot, second)
		}
	}
	out, outb := t.Root.insert(item, t.maxItems())
	if !outb {
		t.Length++
	}
	return out, outb
}

// Delete removes an item equal to the passed in item from the tree, returning
// it.  If no such item exists, returns (zeroValue, false).
func (t *BTreeG[T]) Delete(item T) (T, bool) {
	return t.deleteItem(item, removeItem)
}

// DeleteMin removes the smallest item in the tree and returns it.
// If no such item exists, returns (zeroValue, false).
func (t *BTreeG[T]) DeleteMin() (T, bool) {
	var zero T
	return t.deleteItem(zero, removeMin)
}

// DeleteMax removes the largest item in the tree and returns it.
// If no such item exists, returns (zeroValue, false).
func (t *BTreeG[T]) DeleteMax() (T, bool) {
	var zero T
	return t.deleteItem(zero, removeMax)
}

func (t *BTreeG[T]) deleteItem(item T, typ toRemove) (_ T, _ bool) {
	if t.Root == nil || len(t.Root.BItems) == 0 {
		return
	}
	t.Root = t.Root.mutableFor(t.Cow)
	out, outb := t.Root.remove(item, t.minItems(), typ)
	if len(t.Root.BItems) == 0 && len(t.Root.Children) > 0 {
		oldroot := t.Root
		t.Root = t.Root.Children[0]
		t.Cow.freeNode(oldroot)
	}
	if outb {
		t.Length--
	}
	return out, outb
}

// AscendRange calls the iterator for every value in the tree within the range
// [greaterOrEqual, lessThan), until iterator returns false.
func (t *BTreeG[T]) AscendRange(greaterOrEqual, lessThan T, iterator ItemIteratorG[T]) {
	if t.Root == nil {
		return
	}
	t.Root.iterate(ascend, optional[T](greaterOrEqual), optional[T](lessThan), true, false, iterator)
}

// AscendLessThan calls the iterator for every value in the tree within the range
// [first, pivot), until iterator returns false.
func (t *BTreeG[T]) AscendLessThan(pivot T, iterator ItemIteratorG[T]) {
	if t.Root == nil {
		return
	}
	t.Root.iterate(ascend, empty[T](), optional(pivot), false, false, iterator)
}

// AscendGreaterOrEqual calls the iterator for every value in the tree within
// the range [pivot, last], until iterator returns false.
func (t *BTreeG[T]) AscendGreaterOrEqual(pivot T, iterator ItemIteratorG[T]) {
	if t.Root == nil {
		return
	}
	t.Root.iterate(ascend, optional[T](pivot), empty[T](), true, false, iterator)
}

// Ascend calls the iterator for every value in the tree within the range
// [first, last], until iterator returns false.
func (t *BTreeG[T]) Ascend(iterator ItemIteratorG[T]) {
	if t.Root == nil {
		return
	}
	t.Root.iterate(ascend, empty[T](), empty[T](), false, false, iterator)
}

// DescendRange calls the iterator for every value in the tree within the range
// [lessOrEqual, greaterThan), until iterator returns false.
func (t *BTreeG[T]) DescendRange(lessOrEqual, greaterThan T, iterator ItemIteratorG[T]) {
	if t.Root == nil {
		return
	}
	t.Root.iterate(descend, optional[T](lessOrEqual), optional[T](greaterThan), true, false, iterator)
}

// DescendLessOrEqual calls the iterator for every value in the tree within the range
// [pivot, first], until iterator returns false.
func (t *BTreeG[T]) DescendLessOrEqual(pivot T, iterator ItemIteratorG[T]) {
	if t.Root == nil {
		return
	}
	t.Root.iterate(descend, optional[T](pivot), empty[T](), true, false, iterator)
}

// DescendGreaterThan calls the iterator for every value in the tree within
// the range [last, pivot), until iterator returns false.
func (t *BTreeG[T]) DescendGreaterThan(pivot T, iterator ItemIteratorG[T]) {
	if t.Root == nil {
		return
	}
	t.Root.iterate(descend, empty[T](), optional[T](pivot), false, false, iterator)
}

// Descend calls the iterator for every value in the tree within the range
// [last, first], until iterator returns false.
func (t *BTreeG[T]) Descend(iterator ItemIteratorG[T]) {
	if t.Root == nil {
		return
	}
	t.Root.iterate(descend, empty[T](), empty[T](), false, false, iterator)
}

// Get looks for the key item in the tree, returning it.  It returns
// (zeroValue, false) if unable to find that item.
func (t *BTreeG[T]) Get(key T) (_ T, _ bool) {
	if t.Root == nil {
		return
	}
	return t.Root.get(key)
}

// Min returns the smallest item in the tree, or (zeroValue, false) if the tree is empty.
func (t *BTreeG[T]) Min() (_ T, _ bool) {
	return min(t.Root)
}

// Max returns the largest item in the tree, or (zeroValue, false) if the tree is empty.
func (t *BTreeG[T]) Max() (_ T, _ bool) {
	return max(t.Root)
}

// Has returns true if the given key is in the tree.
func (t *BTreeG[T]) Has(key T) bool {
	_, ok := t.Get(key)
	return ok
}

// Len returns the number of items currently in the tree.
func (t *BTreeG[T]) Len() int {
	return t.Length
}

// Clear removes all items from the btree.  If addNodesToFreelist is true,
// t's nodes are added to its freelist as part of this call, until the freelist
// is full.  Otherwise, the root node is simply dereferenced and the subtree
// left to Go's normal GC processes.
//
// This can be much faster
// than calling Delete on all elements, because that requires finding/removing
// each element in the tree and updating the tree accordingly.  It also is
// somewhat faster than creating a new tree to replace the old one, because
// nodes from the old tree are reclaimed into the freelist for use by the new
// one, instead of being lost to the garbage collector.
//
// This call takes:
//
//	O(1): when addNodesToFreelist is false, this is a single operation.
//	O(1): when the freelist is already full, it breaks out immediately
//	O(freelist size):  when the freelist is empty and the nodes are all owned
//	    by this tree, nodes are added to the freelist until full.
//	O(tree size):  when all nodes are owned by another tree, all nodes are
//	    iterated over looking for nodes to add to the freelist, and due to
//	    ownership, none are.
func (t *BTreeG[T]) Clear(addNodesToFreelist bool) {
	if t.Root != nil && addNodesToFreelist {
		t.Root.reset(t.Cow)
	}
	t.Root, t.Length = nil, 0
}

// reset returns a subtree to the freelist.  It breaks out immediately if the
// freelist is full, since the only benefit of iterating is to fill that
// freelist up.  Returns true if parent reset call should continue.
func (n *Node[T]) reset(c *CopyOnWriteContext[T]) bool {
	for _, child := range n.Children {
		if !child.reset(c) {
			return false
		}
	}
	return c.freeNode(n) != ftFreelistFull
}

// Int implements the Item interface for integers.
type Int int

// Less returns true if int(a) < int(b).
func (a Int) Less(b Item) bool {
	return a < b.(Int)
}

// BTree is an implementation of a B-Tree.
//
// BTree stores Item instances in an ordered structure, allowing easy insertion,
// removal, and iteration.
//
// Write operations are not safe for concurrent mutation by multiple
// goroutines, but Read operations are.
type BTree BTreeG[Item]

var itemLess LessFunc[Item] = func(a, b Item) bool {
	return a.Less(b)
}

// New creates a new B-Tree with the given degree.
//
// New(2), for example, will create a 2-3-4 tree (each node contains 1-3 items
// and 2-4 children).
func New(degree int) *BTree {
	return (*BTree)(NewG[Item](degree, itemLess))
}

// FreeList represents a free list of btree nodes. By default each
// BTree has its own FreeList, but multiple BTrees can share the same
// FreeList.
// Two Btrees using the same freelist are safe for concurrent write access.
type FreeList FreeListG[Item]

// NewFreeList creates a new free list.
// size is the maximum size of the returned free list.
func NewFreeList(size int) *FreeList {
	return (*FreeList)(NewFreeListG[Item](size))
}

// NewWithFreeList creates a new B-Tree that uses the given node free list.
func NewWithFreeList(degree int, f *FreeList) *BTree {
	return (*BTree)(NewWithFreeListG[Item](degree, itemLess, (*FreeListG[Item])(f)))
}

// ItemIterator allows callers of Ascend* to iterate in-order over portions of
// the tree.  When this function returns false, iteration will stop and the
// associated Ascend* function will immediately return.
type ItemIterator ItemIteratorG[Item]

// Clone clones the btree, lazily.  Clone should not be called concurrently,
// but the original tree (t) and the new tree (t2) can be used concurrently
// once the Clone call completes.
//
// The internal tree structure of b is marked read-only and shared between t and
// t2.  Writes to both t and t2 use copy-on-write logic, creating new nodes
// whenever one of b's original nodes would have been modified.  Read operations
// should have no performance degredation.  Write operations for both t and t2
// will initially experience minor slow-downs caused by additional allocs and
// copies due to the aforementioned copy-on-write logic, but should converge to
// the original performance characteristics of the original tree.
func (t *BTree) Clone() (t2 *BTree) {
	return (*BTree)((*BTreeG[Item])(t).Clone())
}

// Delete removes an item equal to the passed in item from the tree, returning
// it.  If no such item exists, returns nil.
func (t *BTree) Delete(item Item) Item {
	i, _ := (*BTreeG[Item])(t).Delete(item)
	return i
}

// DeleteMax removes the largest item in the tree and returns it.
// If no such item exists, returns nil.
func (t *BTree) DeleteMax() Item {
	i, _ := (*BTreeG[Item])(t).DeleteMax()
	return i
}

// DeleteMin removes the smallest item in the tree and returns it.
// If no such item exists, returns nil.
func (t *BTree) DeleteMin() Item {
	i, _ := (*BTreeG[Item])(t).DeleteMin()
	return i
}

// Get looks for the key item in the tree, returning it.  It returns nil if
// unable to find that item.
func (t *BTree) Get(key Item) Item {
	i, _ := (*BTreeG[Item])(t).Get(key)
	return i
}

// Max returns the largest item in the tree, or nil if the tree is empty.
func (t *BTree) Max() Item {
	i, _ := (*BTreeG[Item])(t).Max()
	return i
}

// Min returns the smallest item in the tree, or nil if the tree is empty.
func (t *BTree) Min() Item {
	i, _ := (*BTreeG[Item])(t).Min()
	return i
}

// Has returns true if the given key is in the tree.
func (t *BTree) Has(key Item) bool {
	return (*BTreeG[Item])(t).Has(key)
}

// ReplaceOrInsert adds the given item to the tree.  If an item in the tree
// already equals the given one, it is removed from the tree and returned.
// Otherwise, nil is returned.
//
// nil cannot be added to the tree (will panic).
func (t *BTree) ReplaceOrInsert(item Item) Item {
	i, _ := (*BTreeG[Item])(t).ReplaceOrInsert(item)
	return i
}

// AscendRange calls the iterator for every value in the tree within the range
// [greaterOrEqual, lessThan), until iterator returns false.
func (t *BTree) AscendRange(greaterOrEqual, lessThan Item, iterator ItemIterator) {
	(*BTreeG[Item])(t).AscendRange(greaterOrEqual, lessThan, (ItemIteratorG[Item])(iterator))
}

// AscendLessThan calls the iterator for every value in the tree within the range
// [first, pivot), until iterator returns false.
func (t *BTree) AscendLessThan(pivot Item, iterator ItemIterator) {
	(*BTreeG[Item])(t).AscendLessThan(pivot, (ItemIteratorG[Item])(iterator))
}

// AscendGreaterOrEqual calls the iterator for every value in the tree within
// the range [pivot, last], until iterator returns false.
func (t *BTree) AscendGreaterOrEqual(pivot Item, iterator ItemIterator) {
	(*BTreeG[Item])(t).AscendGreaterOrEqual(pivot, (ItemIteratorG[Item])(iterator))
}

// Ascend calls the iterator for every value in the tree within the range
// [first, last], until iterator returns false.
func (t *BTree) Ascend(iterator ItemIterator) {
	(*BTreeG[Item])(t).Ascend((ItemIteratorG[Item])(iterator))
}

// DescendRange calls the iterator for every value in the tree within the range
// [lessOrEqual, greaterThan), until iterator returns false.
func (t *BTree) DescendRange(lessOrEqual, greaterThan Item, iterator ItemIterator) {
	(*BTreeG[Item])(t).DescendRange(lessOrEqual, greaterThan, (ItemIteratorG[Item])(iterator))
}

// DescendLessOrEqual calls the iterator for every value in the tree within the range
// [pivot, first], until iterator returns false.
func (t *BTree) DescendLessOrEqual(pivot Item, iterator ItemIterator) {
	(*BTreeG[Item])(t).DescendLessOrEqual(pivot, (ItemIteratorG[Item])(iterator))
}

// DescendGreaterThan calls the iterator for every value in the tree within
// the range [last, pivot), until iterator returns false.
func (t *BTree) DescendGreaterThan(pivot Item, iterator ItemIterator) {
	(*BTreeG[Item])(t).DescendGreaterThan(pivot, (ItemIteratorG[Item])(iterator))
}

// Descend calls the iterator for every value in the tree within the range
// [last, first], until iterator returns false.
func (t *BTree) Descend(iterator ItemIterator) {
	(*BTreeG[Item])(t).Descend((ItemIteratorG[Item])(iterator))
}

// Len returns the number of items currently in the tree.
func (t *BTree) Len() int {
	return (*BTreeG[Item])(t).Len()
}

// Clear removes all items from the btree.  If addNodesToFreelist is true,
// t's nodes are added to its freelist as part of this call, until the freelist
// is full.  Otherwise, the root node is simply dereferenced and the subtree
// left to Go's normal GC processes.
//
// This can be much faster
// than calling Delete on all elements, because that requires finding/removing
// each element in the tree and updating the tree accordingly.  It also is
// somewhat faster than creating a new tree to replace the old one, because
// nodes from the old tree are reclaimed into the freelist for use by the new
// one, instead of being lost to the garbage collector.
//
// This call takes:
//
//	O(1): when addNodesToFreelist is false, this is a single operation.
//	O(1): when the freelist is already full, it breaks out immediately
//	O(freelist size):  when the freelist is empty and the nodes are all owned
//	    by this tree, nodes are added to the freelist until full.
//	O(tree size):  when all nodes are owned by another tree, all nodes are
//	    iterated over looking for nodes to add to the freelist, and due to
//	    ownership, none are.
func (t *BTree) Clear(addNodesToFreelist bool) {
	(*BTreeG[Item])(t).Clear(addNodesToFreelist)
}
