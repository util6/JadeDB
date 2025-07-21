package utils

import (
	"sync/atomic"
	"unsafe"
)

// Splice 记录跳表中每一层的前驱和后继节点
type Splice struct {
	prev [maxHeight]uint32 // 使用offset而不是指针，与现有代码保持一致
	next [maxHeight]uint32
}

// SpliceManager 管理Splice的分配和重用
type SpliceManager struct {
	arena *Arena
}

// NewSpliceManager 创建一个新的SpliceManager
func NewSpliceManager(arena *Arena) *SpliceManager {
	return &SpliceManager{
		arena: arena,
	}
}

// NewSplice 创建一个新的Splice
func (sm *SpliceManager) NewSplice() *Splice {
	// 在arena中分配Splice空间
	offset := sm.arena.allocate(uint32(unsafe.Sizeof(Splice{})))
	splice := (*Splice)(unsafe.Pointer(&sm.arena.buf[offset]))
	return splice
}

// Reset 重置Splice
func (s *Splice) Reset() {
	for i := 0; i < maxHeight; i++ {
		atomic.StoreUint32(&s.prev[i], 0)
		atomic.StoreUint32(&s.next[i], 0)
	}
}

// GetPrev 获取指定层的前驱节点
func (s *Splice) GetPrev(level int) uint32 {
	return atomic.LoadUint32(&s.prev[level])
}

// SetPrev 设置指定层的前驱节点
func (s *Splice) SetPrev(level int, offset uint32) {
	atomic.StoreUint32(&s.prev[level], offset)
}

// GetNext 获取指定层的后继节点
func (s *Splice) GetNext(level int) uint32 {
	return atomic.LoadUint32(&s.next[level])
}

// SetNext 设置指定层的后继节点
func (s *Splice) SetNext(level int, offset uint32) {
	atomic.StoreUint32(&s.next[level], offset)
}

// SpliceIterator 使用Splice的迭代器
type SpliceIterator struct {
	list   *Skiplist
	splice *Splice
	level  int
}

// NewSpliceIterator 创建一个新的SpliceIterator
func NewSpliceIterator(list *Skiplist, splice *Splice) *SpliceIterator {
	return &SpliceIterator{
		list:   list,
		splice: splice,
		level:  0,
	}
}

// Next 移动到下一个节点
func (si *SpliceIterator) Next() bool {
	nextOffset := si.splice.GetNext(si.level)
	if nextOffset == 0 {
		return false
	}
	si.splice.SetPrev(si.level, nextOffset)
	nextNode := si.list.arena.getNode(nextOffset)
	if nextNode == nil {
		return false
	}
	si.splice.SetNext(si.level, nextNode.getNextOffset(si.level))
	return true
}

// Prev 移动到前一个节点
func (si *SpliceIterator) Prev() bool {
	prevOffset := si.splice.GetPrev(si.level)
	if prevOffset == 0 {
		return false
	}
	si.splice.SetNext(si.level, prevOffset)
	prevNode := si.list.arena.getNode(prevOffset)
	if prevNode == nil {
		return false
	}
	si.splice.SetPrev(si.level, prevNode.getNextOffset(si.level))
	return true
}

// GetCurrentNode 获取当前节点
func (si *SpliceIterator) GetCurrentNode() *node {
	prevOffset := si.splice.GetPrev(si.level)
	if prevOffset == 0 {
		return nil
	}
	return si.list.arena.getNode(prevOffset)
}

// SetLevel 设置当前层
func (si *SpliceIterator) SetLevel(level int) {
	si.level = level
}

// GetLevel 获取当前层
func (si *SpliceIterator) GetLevel() int {
	return si.level
}
