/*
 * Copyright 2017 Dgraph Labs, Inc. and Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
从RocksDB inline skiplist改编而来。

主要区别：

未针对顺序插入进行优化（没有“prev”）。
没有自定义比较器。
支持覆盖。当插入时遇到相同的键时，这需要特别注意。
对于RocksDB或LevelDB，覆盖是通过键中的新序列号实现的，因此不需要值。
我们不打算支持版本控制。值的原地更新将更有效。
我们丢弃所有非并发代码。
我们不支持Splices。这简化了代码很多。
没有AllocateNode或其他指针算术。
我们将findLessThan，findGreaterOrEqual等组合成一个函数。
*/

package utils

import (
	"fmt"
	"github.com/pkg/errors"
	"log"
	"math"
	"strings"
	"sync/atomic"
	_ "unsafe"
)

const (
	maxHeight      = 20
	heightIncrease = math.MaxUint32 / 3
)

// node 结构体定义了中的一个节点，用于存储键值对。
// 它的设计是为了支持高效的数据检索和更新，同时保持树的平衡。
type node struct {

	// keyOffset 表示节点键在存储介质中的偏移量。
	keyOffset uint32

	// valueIndex 字段用于存储节点的值。它被编码为 uint64 类型，以便可以原子化加载和存储。
	// 具体编码方式为：值偏移量使用 UInt32 表示（位 0-31），值大小使用 UInt16 表示（位 32-63）。
	valueIndex uint64

	// keySize 表示节点键的大小。
	keySize uint16

	// height 表示节点位于哪一层。
	height uint16

	// levels 数组用于存储指向其他节点的指针，最大高度为 maxHeight。
	// 这个设计使得节点能够快速访问其子节点，从而提高树操作的效率。
	levels [maxHeight]uint32
}

// SkipList 表示一个跳表数据结构。
// 它提供了高效的搜索、插入和删除操作，平均时间复杂度为 O(log n)。
// 跳表使用概率方法来平衡节点的高度，以达到平均 O(log n) 的时间复杂度。
type SkipList struct {
	arena *Arena // arena 指向存储节点数据的内存空间，类似于内存池。

	headOffset uint32 // headOffset 是头节点在 arena 内存空间中的偏移量，用于快速访问头节点。
	height     int32  // height 表示当前跳表的高度，决定了最高节点中的指针数量。
	ref        int32  // ref 计数跳表的引用次数，用于引用计数垃圾回收机制，引用计数是一种内存管理方式。在leveldb中，memtable/version等组件，可能被多个线程共享，因此不能直接删除，需要等到没有线程占用时，才能删除。
	Onclose    func() // Onclose 是一个回调函数，在跳表关闭或销毁时调用，用于执行清理操作。
}

/*
SkipListIterator 结构体定义了跳表迭代器的状态。
它主要包含两个成员：跳表本身和当前迭代所处的节点。
*/

type SkipListIterator struct {
	// list 指向跳表本身，用于在迭代过程中访问跳表的结构和数据。
	list *SkipList
	// n 指向当前迭代所处的节点，初始时通常指向跳表的头节点。
	n *node
}

type UniIterator struct {
	iter     *Iterator
	reversed bool
}

//-----------------------------------------跳表的几个重要方法-----------------------------------------

// findNear 在跳表中查找最接近给定键的节点。
// key: 要查找的键。
// less: 如果为true，表示查找小于key的最大键；如果为false，表示查找大于key的最小键。
// allowEqual: 如果为true，当查找的键正好存在于跳表中时，允许返回该键。
// 返回最接近key的节点和一个布尔值，表示是否找到了相等的键。
func (s *SkipList) findSpliceForLevel(key []byte) (*node, []*node, bool) {

	level := int(s.getHeight() - 1)    // 从跳表的最高层开始查找。
	prev := make([]*node, maxHeight+1) // 用于存储每一层小于 key 的最大节点。

	for i := level; i >= 0; i-- {
		preNode := s.getHead() // 从跳表的头节点开始。
		preNode, _ = s.findSpliceForLevel(key, preNode, i)
		prev[i] = preNode
	}

	// 从最底层开始处理

	next := s.getNext(prev[0], 0)
	if next == nil {
		return prev[0], prev, false
	}

	cmp := CompareKeys(key, next.key(s.arena))
	if cmp == 0 {
		return prev[0], prev, true
	}

	return prev[0], prev, false
}

// findSpliceForLevel 在指定层级上查找键 key 的前驱和后继节点偏移量。
// 参数 key 是要查找的键，before 是当前节点在节点池中的偏移量，level 是当前操作的层级。
// 返回值是前驱节点偏移量和后继节点偏移量。
func (s *SkipList) findSpliceForLevel(key []byte, preNode *node, level int) (*node, bool) {
	for {
		// Assume before.key < key.
		nextNode := s.getNext(preNode, level)
		if nextNode == nil {
			return preNode, false
		}
		nextKey := nextNode.key(s.arena)
		cmp := CompareKeys(key, nextKey)
		isEqual := false

		if cmp > 0 {
			preNode = nextNode // Keep moving right on this level.
		} else {
			if cmp == 0 {
				isEqual = true
			}
			return preNode, isEqual
		}

	}
}

// Add 向跳表中添加一个条目。
// 如果键已存在，则更新对应的值而不增加跳表的高度。
// 这里的设计允许覆盖，因此我们可能不需要创建新节点或增加高度。
func (s *SkipList) Add(e *Entry) {
	key, v := e.Key, ValueStruct{
		Meta:      e.Meta,
		Value:     e.Value,
		ExpiresAt: e.ExpiresAt,
		Version:   e.Version,
	}

	foundNode, preNodes, isEqual := s.findNear(key)
	if isEqual && foundNode != nil {
		// 更新已有节点的值
		vo := s.arena.putVal(v)
		encValue := encodeValueLocation(vo, v.EncodedSize())
		s.getNext(preNodes[0], 0).setValue(encValue)
		return
	}

	// 创建新节点
	height := s.randomHeight()
	newNode := newNode(s.arena, key, v, height)

	// 如果新节点的高度超过当前高度，需要增加跳表的高度
	listHeight := s.getHeight()
	for height > int(listHeight) {
		if atomic.CompareAndSwapInt32(&s.height, listHeight, int32(height)) {
			break
		}
		listHeight = s.getHeight()
	}

	// 插入新节点，并更新前链
	for i := 0; i < height; i++ {
		for {
			if preNodes[i] == nil {
				preNodes[i] = s.getHead()
				preNodes[i], isEqual = s.findSpliceForLevel(key, preNodes[i], i)
			}
			nextLocation := s.arena.getNodeOffset(s.getNext(preNodes[i], i))
			newNode.levels[i] = nextLocation
			if preNodes[i].casNextOffset(i, nextLocation, s.arena.getNodeOffset(newNode)) {
				break
			}
			// CAS 失败，重新获取 preNodes 和 nextLocation
			preNodes[i], isEqual = s.findSpliceForLevel(key, preNodes[i], i)
			if isEqual {
				// 更新已有节点的值
				vo := s.arena.putVal(v)
				encValue := encodeValueLocation(vo, v.EncodedSize())
				s.getNext(preNodes[i], i).setValue(encValue)
				return
			}
		}

	}
}

// findLast 在跳表中查找最后一个节点。
// 它通过从跳表的最高层级开始，逐层向下查找最后一个节点。
// 如果跳表为空，则返回nil。
func (s *SkipList) findLast() *node {
	// 获取跳表的头节点
	n := s.getHead()
	// 获取跳表的最大层级，减1是因为层级从0开始
	level := int(s.getHeight()) - 1
	for {
		// 获取当前节点在当前层级的下一个节点
		next := s.getNext(n, level)
		if next != nil {
			// 如果下一个节点存在，移动到下一个节点
			n = next
			continue
		}
		// 当前层级没有下一个节点时，检查是否已经到达最低层级
		if level == 0 {
			// 如果当前节点是头节点，说明跳表为空，返回nil
			if n == s.getHead() {
				return nil
			}
			// 返回当前节点，作为最后一个节点
			return n
		}
		// 当前层级没有下一个节点，下降一层继续查找
		level--
	}
}

// Search 在跳表中查找与给定键相同的值。
// 它首先找到给定键的最接近的节点，如果存在，则比较节点的键。
// 如果节点的键与给定键相同，则返回该节点的值；否则返回空的 ValueStruct。
func (s *SkipList) Search(key []byte) ValueStruct {

	n, _, _ := s.findNear(key)
	x := n.key(s.arena)
	fmt.Println(string(x))
	// 如果找到的节点为空，直接返回空的 ValueStruct。
	if n == nil {
		return ValueStruct{}
	}
	next := s.getNext(n, 0)
	// 获取节点的键。
	nextKey := s.arena.getKey(next.keyOffset, next.keySize)

	// 比较节点的键和给定键。
	if !SameKey(key, nextKey) {
		return ValueStruct{}
	}

	// 获取节点的值偏移和值大小。
	valOffset, valSize := next.getValueOffset()

	// 从数据结构中获取节点的值。
	vs := s.arena.getVal(valOffset, valSize)

	// 返回节点的值。
	return vs
}

// Draw 根据跳表的结构绘制其层次结构图。
// align 参数决定是否进行对齐处理。
func (s *SkipList) Draw(align bool) {
	// 初始化一个二维切片，用于存储每层节点的字符串表示。
	reverseTree := make([][]string, s.getHeight())
	head := s.getHead()

	// 从跳表的最高层开始，逐层遍历。
	for level := int(s.getHeight()) - 1; level >= 0; level-- {
		next := head
		for {
			var nodeStr string
			// 获取下一个节点。
			next = s.getNext(next, level)
			if next != nil {
				// 获取节点的键和值结构，并格式化为字符串。
				key := next.key(s.arena)
				vs := next.getValueStruct(s.arena)
				nodeStr = fmt.Sprintf("%s(%s)", key, vs.Value)
			} else {
				break
			}
			// 将节点字符串添加到当前层的切片中。
			reverseTree[level] = append(reverseTree[level], nodeStr)
		}
	}

	// 对齐处理，使各层节点位置对齐。
	if align && s.getHeight() > 1 {
		baseFloor := reverseTree[0]
		for level := 1; level < int(s.getHeight()); level++ {
			pos := 0
			for _, ele := range baseFloor {
				if pos == len(reverseTree[level]) {
					break
				}
				if ele != reverseTree[level][pos] {
					// 生成与基础层节点等长的横线字符串，用于对齐。
					newStr := fmt.Sprintf(strings.Repeat("-", len(ele)))
					reverseTree[level] = append(reverseTree[level][:pos+1], reverseTree[level][pos:]...)
					reverseTree[level][pos] = newStr
				}
				pos++
			}
		}
	}

	// 打印跳表的层次结构图。
	for level := int(s.getHeight()) - 1; level >= 0; level-- {
		fmt.Printf("%d: ", level)
		for pos, ele := range reverseTree[level] {
			if pos == len(reverseTree[level])-1 {
				fmt.Printf("%s  ", ele)
			} else {
				fmt.Printf("%s->", ele)
			}
		}
		fmt.Println()
	}
}

//-----------------------------------------跳表的几个重要方法 end-----------------------------------------

/*
IncrRef 增加引用计数。
该方法使用原子操作来增加跳表实例的引用计数，确保并发安全。

主要用于对象共享的场景，以避免过早的垃圾回收。
*/
func (s *SkipList) IncrRef() {
	//atomic.AddInt32: 这是一个来自sync/atomic包的函数，用于原子性地对32位整型变量进行加法操作。
	//&s.ref: 取s中的ref字段的地址，即指向ref的指针。
	//1: 要加到s.ref上的数值

	atomic.AddInt32(&s.ref, 1)
}

// DecrRef 函数用于减少跳表的引用计数。当引用计数为0时，表示没有引用者，此时可以安全地调用关闭操作。
// 该函数主要用途是管理跳表的生命周期，确保在没有引用时能够正确地释放资源。
func (s *SkipList) DecrRef() {
	// 原子操作减少引用计数，不需额外注释。
	currentRef := atomic.AddInt32(&s.ref, -1)
	// 如果引用计数大于0，表示还有引用者，直接返回不进行关闭操作。
	if currentRef > 0 {
		return
	}
	// 如果设置了关闭操作函数Onclose，则调用它。这通常用于清理资源或执行其他必要的关闭操作。
	if s.Onclose != nil {
		s.Onclose()
	}
	// 将对象池置为空，防止已经释放的对象被错误地引用或操作。
	s.arena = nil
}

// newNode 在给定的 arena 中创建一个新的节点。
// 该函数负责在 arena 中存储键值对，并维护节点的高度信息，以支持不同高度的节点在层次结构中的插入。

func newNode(arena *Arena, key []byte, v ValueStruct, height int) *node {
	// 从 arena 中获取一个新的节点，并指定其高度。
	node := arena.getNode(arena.memoryAligned(height))
	// 设置节点的键大小。
	node.keySize = uint16(len(key))
	// 将键存储到 arena 中，并记录键的偏移量。
	node.keyOffset = arena.putKey(key)
	// 编码值，并将其存储到 arena 中。
	node.valueIndex = encodeValueLocation(arena.putVal(v), v.EncodedSize())
	// 设置节点的高度。
	node.height = uint16(height)
	// 返回新创建的节点。
	return node
}

// encodeValueLocation 将一个值的偏移量和大小编码为一个64位的整数。
// 高32位存储偏移量，低32位存储大小。

func encodeValueLocation(valOffset uint32, valSize uint32) uint64 {
	return uint64(valOffset)<<32 | uint64(valSize)
}

func decodeValueLocation(value uint64) (valOffset uint32, valSize uint32) {
	valSize = uint32(value)
	valOffset = uint32(value >> 32)
	return
}

// NewSkipList 创建并返回一个新的跳表实例。
// arenaSize 参数指定跳表底层存储空间的大小。
func newSkipList(arenaSize int64) *SkipList {
	// 初始化存储空间，arenaSize为存储空间大小。
	area := newArena(arenaSize)
	// 创建一个新的节点，作为跳表的头节点。
	head := newNode(area, nil, ValueStruct{}, maxHeight)
	// 构造并返回跳表实例。
	return &SkipList{
		arena:      area,                     // 设置跳表的存储空间。
		headOffset: area.getNodeOffset(head), // 记录头节点在存储空间中的偏移。
		height:     1,                        // 初始化跳表的高度为1。
		ref:        1,                        // 初始化引用计数为1。
	}
}

// key方法的改进版本，包括了空指针检查和边界条件检查。
// 它现在返回一个额外的布尔值来指示操作是否成功。
func (n *node) key(arena *Arena) []byte {
	if n == nil || arena == nil {
		return nil
	}

	if n.keyOffset < 0 || n.keySize <= 0 { // 假设arena.data是Arena中用于存储数据的切片
		return nil
	}

	return arena.getKey(n.keyOffset, n.keySize)
}

// getValueOffset 获取节点值及其偏移量
// 该函数通过原子操作加载节点的值，然后解码该值以获取其实际值和偏移量
// 返回值:
// - uint32: 节点值的偏移量
// - uint32: 节点值的低32位
func (n *node) getValueOffset() (uint32, uint32) {
	// 使用原子操作加载节点的64位值
	value := atomic.LoadUint64(&n.valueIndex)
	// 调用decodeValue函数解码节点值，返回值的低32位和偏移量
	return decodeValueLocation(value)
}

// getVs 从节点中获取存储的ValueStruct。
// 此函数通过获取节点存储值的偏移量和大小，从arena中重构ValueStruct。

func (n *node) getValueStruct(arena *Arena) ValueStruct {
	valOffset, valSize := n.getValueOffset() // 获取节点中存储值的偏移量和大小。
	return arena.getVal(valOffset, valSize)  // 从Arena中根据偏移量和大小获取值，并返回对应的ValueStruct。
}

// getNext 在跳表中获取下一个节点。
//
//	此函数通过获取当前节点对应高度的下一个节点地址，并返回该地址对应的节点指针。
//	主要用于跳表中的节点遍历或查找操作。
func (s *SkipList) getNext(nd *node, height int) *node {
	return s.arena.getNode(nd.getNextOffset(height))
}

func (s *SkipList) getHead() *node {
	return s.arena.getNode(s.headOffset)
}

func (s *SkipList) getHeight() int32 {
	return atomic.LoadInt32(&s.height)
}

// setValue 是一个方法，用于在并发环境下安全地更新节点的值。
// 它使用了原子操作来保证在更新操作中的线程安全。 主要用于在并发环境下，避免竞态条件的发生。
func (n *node) setValue(vo uint64) {
	atomic.StoreUint64(&n.valueIndex, vo)
}

func (n *node) getNextOffset(h int) uint32 {
	return atomic.LoadUint32(&n.levels[h])
}

// casNextOffset 尝试在指定的级别上，对 nextOffset 进行比较并交换操作。
// 这个方法用于在并发环境下安全地更新 node 的 nextOffset 值。
// 参数 h 表示操作的级别，old 是预期的当前值，val 是欲更新的目标值。
// 返回值表示本次比较并交换操作是否成功。
func (n *node) casNextOffset(h int, old, val uint32) bool {
	return atomic.CompareAndSwapUint32(&n.levels[h], old, val)
}

func (s *SkipList) MemSize() int64 { return s.arena.size() }

// randomHeight 生成一个随机高度，用于新插入的节点。
// 它解决了为什么需要随机高度的问题：为了保持跳表的平衡，减少搜索时间。
// 随机高度的选取是基于一个概率，这样可以保证跳表的层次不会过于频繁地增加，
// 从而保持较高的搜索效率。
// 返回值是新节点的高度，用于在插入节点时确定其在跳表中的位置。
func (s *SkipList) randomHeight() int {
	h := 1
	for h < maxHeight && FastRand()%4 == 0 {
		h++
	}
	return h
}

// ---------------------------------跳表迭代器相关方法----------------------------------------

func (s *SkipList) newSkipListIterator() Iterator {
	s.IncrRef()
	return &SkipListIterator{list: s}
}

func (s *SkipListIterator) Next() {
	AssertTrue(s.Valid())
	s.n = s.list.getNext(s.n, 0)
}

func (s *SkipListIterator) Valid() bool { return s.n != nil }

func (s *SkipListIterator) Rewind() {
	s.SeekToFirst()
}

func (s *SkipListIterator) Item() Item {
	return &Entry{
		Key:       s.Key(),
		Value:     s.Value().Value,
		ExpiresAt: s.Value().ExpiresAt,
		Meta:      s.Value().Meta,
		Version:   s.Value().Version,
	}
}

func (s *SkipListIterator) Close() error {
	s.list.DecrRef()
	return nil
}

func (s *SkipListIterator) Key() []byte {
	return s.list.arena.getKey(s.n.keyOffset, s.n.keySize)
}

// Value returns valueIndex.
func (s *SkipListIterator) Value() ValueStruct {
	valOffset, valSize := s.n.getValueOffset()
	return s.list.arena.getVal(valOffset, valSize)
}

// ValueUint64 returns the uint64 valueIndex of the current node.
func (s *SkipListIterator) ValueUint64() uint64 {
	return s.n.valueIndex
}

func (s *SkipListIterator) Seek(target []byte) {
	x, _, _ := s.list.findNear(target)
	s.n = x

}

// Prev advances to the previous position.
func (s *SkipListIterator) Prev() {
	AssertTrue(s.Valid())
	x, _, _ := s.list.findNear(s.Key()) // find <. No equality allowed.
	s.n = x
}
func (s *SkipListIterator) SeekForPrev(target []byte) {
	x, _, _ := s.list.findNear(target) // find <=.
	s.n = x
}

// SeekToFirst seeks position at the first entry in list.
// Final state of iterator is Valid() iff list is not empty.
func (s *SkipListIterator) SeekToFirst() {
	s.n = s.list.getNext(s.list.getHead(), 0)
}

// SeekToLast seeks position at the last entry in list.
// Final state of iterator is Valid() iff list is not empty.
func (s *SkipListIterator) SeekToLast() {
	s.n = s.list.findLast()
}

/*
go:linkname 指令: 这个特殊的注释告诉编译器将当前函数与另一个具有指定名称的函数进行链接。在这个例子中, FastRand 被链接到 runtime.fastrand。
runtime.fastrand: 这是Go标准库中的一个内部函数，用于生成快速伪随机数。
返回类型: 函数 FastRand() 返回一个 uint32 类型的值，即一个无符号32位整数。
*/
//go:linkname FastRand runtime.fastrand
func FastRand() uint32

// AssertTruef 是 AssertTrue 的扩展版本，提供了额外的信息。
// 如果第一个参数的布尔值为 false，则根据后续参数格式化并输出错误信息，并终止程序。
// 此函数用于断言在程序执行期间某个条件必须为真，并在断言失败时提供详细的错误信息。
func AssertTrueF(b bool, format string, args ...interface{}) {
	if !b {
		// 当断言条件未满足时，根据 format 和 args 格式化并输出错误信息，然后终止程序。
		log.Fatalf("%+v", errors.Errorf(format, args...))
	}
}

// ---------------------------------跳表迭代器相关方法 end----------------------------------------
