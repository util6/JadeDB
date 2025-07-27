/*
JadeDB B+树迭代器模块

本模块实现B+树的迭代器功能，支持顺序遍历和范围查询。
基于B+树的叶子节点链表结构，提供高效的有序遍历能力。

核心功能：
1. 顺序遍历：从最小键到最大键的有序遍历
2. 范围查询：指定起始和结束键的范围遍历
3. 反向遍历：从最大键到最小键的逆序遍历
4. 定位查找：快速定位到指定键的位置
5. 资源管理：自动管理页面引用和释放

设计原理：
- 叶子链表：利用B+树叶子节点的双向链表结构
- 页面缓存：通过缓冲池管理页面访问
- 懒加载：按需加载页面，减少内存占用
- 异常安全：确保在异常情况下正确释放资源

性能特性：
- 顺序访问：O(1)移动到下一个/上一个记录
- 随机定位：O(log n)定位到指定键
- 内存效率：只缓存当前访问的页面
- 并发安全：支持多个迭代器并发访问
*/

package bplustree

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

// BTreeIterator B+树迭代器
type BTreeIterator struct {
	// 核心组件
	btree      *BPlusTree  // B+树实例
	bufferPool *BufferPool // 缓冲池

	// 当前状态
	currentNode   *Node   // 当前节点
	currentIndex  int     // 当前记录索引
	currentRecord *Record // 当前记录

	// 遍历配置
	startKey []byte // 起始键（包含）
	endKey   []byte // 结束键（包含）
	reverse  bool   // 是否反向遍历
	limit    int    // 最大返回数量

	// 状态标志
	valid  bool // 迭代器是否有效
	closed bool // 迭代器是否已关闭

	// 统计信息
	itemsRead int64 // 已读取的记录数
}

// BTreeIteratorOptions 迭代器选项
type BTreeIteratorOptions struct {
	StartKey []byte // 起始键（nil表示从最小键开始）
	EndKey   []byte // 结束键（nil表示到最大键结束）
	Reverse  bool   // 是否反向遍历
	Limit    int    // 最大返回数量（0表示无限制）
}

// NewIterator 创建新的B+树迭代器
func (bt *BPlusTree) NewIterator(options *BTreeIteratorOptions) (*BTreeIterator, error) {
	if options == nil {
		options = &BTreeIteratorOptions{}
	}

	// 验证参数
	if options.StartKey != nil && options.EndKey != nil {
		if bytes.Compare(options.StartKey, options.EndKey) > 0 {
			return nil, fmt.Errorf("start key must be less than or equal to end key")
		}
	}

	iter := &BTreeIterator{
		btree:      bt,
		bufferPool: bt.bufferPool,
		startKey:   options.StartKey,
		endKey:     options.EndKey,
		reverse:    options.Reverse,
		limit:      options.Limit,
		valid:      false,
		closed:     false,
	}

	// 定位到起始位置
	if err := iter.seekToStart(); err != nil {
		return nil, err
	}

	return iter, nil
}

// seekToStart 定位到起始位置
func (iter *BTreeIterator) seekToStart() error {
	// 获取根页面ID
	rootPageID := iter.btree.rootPageID.Load()
	if rootPageID == 0 {
		// 空树
		iter.valid = false
		return nil
	}

	var targetKey []byte
	if iter.reverse {
		// 反向遍历：从endKey开始（如果没有endKey则从最大键开始）
		targetKey = iter.endKey
	} else {
		// 正向遍历：从startKey开始（如果没有startKey则从最小键开始）
		targetKey = iter.startKey
	}

	// 查找起始叶子节点
	leafNode, err := iter.findLeafNode(targetKey)
	if err != nil {
		return err
	}

	iter.currentNode = leafNode

	// 在叶子节点中定位起始记录
	if err := iter.positionInNode(targetKey); err != nil {
		iter.releaseCurrentNode()
		return err
	}

	// 检查当前记录是否在范围内
	if !iter.isCurrentRecordInRange() {
		iter.moveToNextValidRecord()
	}

	return nil
}

// findLeafNode 查找包含指定键的叶子节点
func (iter *BTreeIterator) findLeafNode(key []byte) (*Node, error) {
	// 获取根页面
	rootPageID := iter.btree.rootPageID.Load()
	rootPage, err := iter.bufferPool.GetPage(rootPageID)
	if err != nil {
		return nil, err
	}

	currentNode, err := LoadNode(rootPage)
	if err != nil {
		iter.bufferPool.PutPage(rootPage)
		return nil, err
	}

	// 从根节点向下查找到叶子节点
	for !currentNode.IsLeaf() {
		// 在内部节点中查找子节点
		_, childPageID, err := iter.findChildNode(currentNode, key)
		if err != nil {
			iter.bufferPool.PutPage(currentNode.GetPage())
			return nil, err
		}

		// 获取子节点页面
		childPage, err := iter.bufferPool.GetPage(childPageID)
		if err != nil {
			iter.bufferPool.PutPage(currentNode.GetPage())
			return nil, err
		}

		childNode, err := LoadNode(childPage)
		if err != nil {
			iter.bufferPool.PutPage(childPage)
			iter.bufferPool.PutPage(currentNode.GetPage())
			return nil, err
		}

		// 释放父节点，移动到子节点
		iter.bufferPool.PutPage(currentNode.GetPage())
		currentNode = childNode
	}

	return currentNode, nil
}

// findChildNode 在内部节点中查找子节点
func (iter *BTreeIterator) findChildNode(node *Node, key []byte) (int, uint64, error) {
	if node.IsLeaf() {
		return 0, 0, fmt.Errorf("not an internal node")
	}

	recordCount := node.GetRecordCount()
	if recordCount == 0 {
		return 0, 0, fmt.Errorf("empty internal node")
	}

	// 如果没有指定键，返回第一个子节点（用于遍历开始）
	if key == nil {
		record, err := node.getRecord(0)
		if err != nil {
			return 0, 0, err
		}
		childPageID := binary.LittleEndian.Uint64(record.Value)
		return 0, childPageID, nil
	}

	// 在内部节点中查找合适的子节点
	for i := uint16(0); i < recordCount; i++ {
		record, err := node.getRecord(i)
		if err != nil {
			return 0, 0, err
		}

		// 如果是第一个记录（空键），检查是否应该使用这个子节点
		if i == 0 && len(record.Key) == 0 {
			if recordCount == 1 {
				childPageID := binary.LittleEndian.Uint64(record.Value)
				return int(i), childPageID, nil
			}

			// 检查下一个记录
			nextRecord, err := node.getRecord(1)
			if err != nil {
				return 0, 0, err
			}

			if bytes.Compare(key, nextRecord.Key) < 0 {
				childPageID := binary.LittleEndian.Uint64(record.Value)
				return int(i), childPageID, nil
			}
			continue
		}

		// 比较键
		if len(record.Key) > 0 && bytes.Compare(key, record.Key) <= 0 {
			childPageID := binary.LittleEndian.Uint64(record.Value)
			return int(i), childPageID, nil
		}
	}

	// 如果没找到，使用最后一个子节点
	lastRecord, err := node.getRecord(recordCount - 1)
	if err != nil {
		return 0, 0, err
	}

	childPageID := binary.LittleEndian.Uint64(lastRecord.Value)
	return int(recordCount - 1), childPageID, nil
}

// positionInNode 在节点中定位到指定键的位置
func (iter *BTreeIterator) positionInNode(key []byte) error {
	if iter.currentNode == nil {
		return fmt.Errorf("current node is nil")
	}

	records, err := iter.currentNode.getAllRecords()
	if err != nil {
		return err
	}

	if len(records) == 0 {
		iter.valid = false
		return nil
	}

	// 如果没有指定键，定位到第一个或最后一个记录
	if key == nil {
		if iter.reverse {
			iter.currentIndex = len(records) - 1
		} else {
			iter.currentIndex = 0
		}
		iter.currentRecord = records[iter.currentIndex]
		iter.valid = true
		return nil
	}

	// 查找指定键的位置
	for i, record := range records {
		cmp := bytes.Compare(record.Key, key)
		if cmp >= 0 {
			iter.currentIndex = i
			iter.currentRecord = record
			iter.valid = true
			return nil
		}
	}

	// 如果没找到，定位到最后一个记录的下一个位置
	iter.currentIndex = len(records)
	iter.currentRecord = nil
	iter.valid = false
	return nil
}

// isCurrentRecordInRange 检查当前记录是否在指定范围内
func (iter *BTreeIterator) isCurrentRecordInRange() bool {
	if iter.currentRecord == nil {
		return false
	}

	// 检查起始键
	if iter.startKey != nil {
		if bytes.Compare(iter.currentRecord.Key, iter.startKey) < 0 {
			return false
		}
	}

	// 检查结束键
	if iter.endKey != nil {
		if bytes.Compare(iter.currentRecord.Key, iter.endKey) > 0 {
			return false
		}
	}

	// 检查数量限制
	if iter.limit > 0 && iter.itemsRead >= int64(iter.limit) {
		return false
	}

	return true
}

// moveToNextValidRecord 移动到下一个有效记录
func (iter *BTreeIterator) moveToNextValidRecord() {
	for iter.valid {
		if iter.reverse {
			iter.moveToPrevious()
		} else {
			iter.moveToNext()
		}

		if iter.valid && iter.isCurrentRecordInRange() {
			break
		}
	}
}

// moveToNext 移动到下一个记录
func (iter *BTreeIterator) moveToNext() {
	if !iter.valid || iter.currentNode == nil {
		iter.valid = false
		return
	}

	// 尝试在当前节点中移动到下一个记录
	records, err := iter.currentNode.getAllRecords()
	if err != nil {
		iter.valid = false
		return
	}

	iter.currentIndex++
	if iter.currentIndex < len(records) {
		// 在当前节点中找到下一个记录
		iter.currentRecord = records[iter.currentIndex]
		iter.itemsRead++
		return
	}

	// 当前节点已遍历完，移动到右兄弟节点
	if err := iter.moveToRightSibling(); err != nil {
		iter.valid = false
		return
	}

	// 在新节点中定位到第一个记录
	if iter.currentNode != nil {
		records, err := iter.currentNode.getAllRecords()
		if err != nil || len(records) == 0 {
			iter.valid = false
			return
		}

		iter.currentIndex = 0
		iter.currentRecord = records[0]
		iter.itemsRead++
	} else {
		iter.valid = false
	}
}

// moveToPrevious 移动到上一个记录
func (iter *BTreeIterator) moveToPrevious() {
	if !iter.valid || iter.currentNode == nil {
		iter.valid = false
		return
	}

	// 尝试在当前节点中移动到上一个记录
	if iter.currentIndex > 0 {
		iter.currentIndex--
		records, err := iter.currentNode.getAllRecords()
		if err != nil {
			iter.valid = false
			return
		}
		iter.currentRecord = records[iter.currentIndex]
		iter.itemsRead++
		return
	}

	// 当前节点已遍历完，移动到左兄弟节点
	if err := iter.moveToLeftSibling(); err != nil {
		iter.valid = false
		return
	}

	// 在新节点中定位到最后一个记录
	if iter.currentNode != nil {
		records, err := iter.currentNode.getAllRecords()
		if err != nil || len(records) == 0 {
			iter.valid = false
			return
		}

		iter.currentIndex = len(records) - 1
		iter.currentRecord = records[iter.currentIndex]
		iter.itemsRead++
	} else {
		iter.valid = false
	}
}

// moveToRightSibling 移动到右兄弟节点
func (iter *BTreeIterator) moveToRightSibling() error {
	if iter.currentNode == nil {
		return fmt.Errorf("current node is nil")
	}

	// 获取右兄弟节点ID
	rightSiblingID := iter.currentNode.header.RightSibling
	if rightSiblingID == 0 {
		// 没有右兄弟节点
		iter.releaseCurrentNode()
		iter.currentNode = nil
		return nil
	}

	// 获取右兄弟节点页面
	rightPage, err := iter.bufferPool.GetPage(rightSiblingID)
	if err != nil {
		iter.releaseCurrentNode()
		return err
	}

	rightNode, err := LoadNode(rightPage)
	if err != nil {
		iter.bufferPool.PutPage(rightPage)
		iter.releaseCurrentNode()
		return err
	}

	// 释放当前节点，切换到右兄弟节点
	iter.releaseCurrentNode()
	iter.currentNode = rightNode
	return nil
}

// moveToLeftSibling 移动到左兄弟节点
func (iter *BTreeIterator) moveToLeftSibling() error {
	if iter.currentNode == nil {
		return fmt.Errorf("current node is nil")
	}

	// 获取左兄弟节点ID
	leftSiblingID := iter.currentNode.header.LeftSibling
	if leftSiblingID == 0 {
		// 没有左兄弟节点
		iter.releaseCurrentNode()
		iter.currentNode = nil
		return nil
	}

	// 获取左兄弟节点页面
	leftPage, err := iter.bufferPool.GetPage(leftSiblingID)
	if err != nil {
		iter.releaseCurrentNode()
		return err
	}

	leftNode, err := LoadNode(leftPage)
	if err != nil {
		iter.bufferPool.PutPage(leftPage)
		iter.releaseCurrentNode()
		return err
	}

	// 释放当前节点，切换到左兄弟节点
	iter.releaseCurrentNode()
	iter.currentNode = leftNode
	return nil
}

// releaseCurrentNode 释放当前节点
func (iter *BTreeIterator) releaseCurrentNode() {
	if iter.currentNode != nil {
		iter.bufferPool.PutPage(iter.currentNode.GetPage())
		iter.currentNode = nil
	}
}

// Next 移动到下一个记录
func (iter *BTreeIterator) Next() {
	if iter.closed {
		iter.valid = false
		return
	}

	if iter.reverse {
		iter.moveToPrevious()
	} else {
		iter.moveToNext()
	}

	// 检查是否仍在范围内
	if iter.valid && !iter.isCurrentRecordInRange() {
		iter.valid = false
	}
}

// Valid 检查迭代器是否有效
func (iter *BTreeIterator) Valid() bool {
	return iter.valid && !iter.closed
}

// Key 获取当前记录的键
func (iter *BTreeIterator) Key() []byte {
	if !iter.Valid() || iter.currentRecord == nil {
		return nil
	}
	return iter.currentRecord.Key
}

// Value 获取当前记录的值
func (iter *BTreeIterator) Value() []byte {
	if !iter.Valid() || iter.currentRecord == nil {
		return nil
	}
	return iter.currentRecord.Value
}

// Record 获取当前记录
func (iter *BTreeIterator) Record() *Record {
	if !iter.Valid() {
		return nil
	}
	return iter.currentRecord
}

// Seek 定位到指定键
func (iter *BTreeIterator) Seek(key []byte) error {
	if iter.closed {
		return fmt.Errorf("iterator is closed")
	}

	// 释放当前节点
	iter.releaseCurrentNode()

	// 重新定位
	leafNode, err := iter.findLeafNode(key)
	if err != nil {
		iter.valid = false
		return err
	}

	iter.currentNode = leafNode

	// 在叶子节点中定位
	if err := iter.positionInNode(key); err != nil {
		iter.releaseCurrentNode()
		iter.valid = false
		return err
	}

	// 检查是否在范围内
	if !iter.isCurrentRecordInRange() {
		iter.moveToNextValidRecord()
	}

	return nil
}

// Rewind 重置到起始位置
func (iter *BTreeIterator) Rewind() error {
	if iter.closed {
		return fmt.Errorf("iterator is closed")
	}

	// 释放当前节点
	iter.releaseCurrentNode()

	// 重置状态
	iter.currentIndex = 0
	iter.currentRecord = nil
	iter.valid = false
	iter.itemsRead = 0

	// 重新定位到起始位置
	return iter.seekToStart()
}

// Close 关闭迭代器
func (iter *BTreeIterator) Close() error {
	if iter.closed {
		return nil
	}

	// 释放当前节点
	iter.releaseCurrentNode()

	// 标记为已关闭
	iter.closed = true
	iter.valid = false

	return nil
}

// GetStats 获取迭代器统计信息
func (iter *BTreeIterator) GetStats() map[string]interface{} {
	return map[string]interface{}{
		"items_read": iter.itemsRead,
		"valid":      iter.valid,
		"closed":     iter.closed,
		"reverse":    iter.reverse,
		"has_limit":  iter.limit > 0,
		"limit":      iter.limit,
	}
}

// findLastLeafNode 查找最后一个叶子节点
func (iter *BTreeIterator) findLastLeafNode() (*Node, error) {
	// 获取根页面
	rootPageID := iter.btree.rootPageID.Load()
	rootPage, err := iter.bufferPool.GetPage(rootPageID)
	if err != nil {
		return nil, err
	}

	currentNode, err := LoadNode(rootPage)
	if err != nil {
		iter.bufferPool.PutPage(rootPage)
		return nil, err
	}

	// 沿着最右边的路径向下
	for !currentNode.IsLeaf() {
		// 获取最后一个子节点
		recordCount := currentNode.GetRecordCount()
		if recordCount == 0 {
			iter.bufferPool.PutPage(currentNode.GetPage())
			return nil, fmt.Errorf("empty internal node")
		}

		lastRecord, err := currentNode.getRecord(recordCount - 1)
		if err != nil {
			iter.bufferPool.PutPage(currentNode.GetPage())
			return nil, err
		}

		childPageID := binary.LittleEndian.Uint64(lastRecord.Value)
		childPage, err := iter.bufferPool.GetPage(childPageID)
		if err != nil {
			iter.bufferPool.PutPage(currentNode.GetPage())
			return nil, err
		}

		childNode, err := LoadNode(childPage)
		if err != nil {
			iter.bufferPool.PutPage(childPage)
			iter.bufferPool.PutPage(currentNode.GetPage())
			return nil, err
		}

		// 释放当前节点，移动到子节点
		iter.bufferPool.PutPage(currentNode.GetPage())
		currentNode = childNode
	}

	return currentNode, nil
}

// First 移动到第一个记录
func (iter *BTreeIterator) First() error {
	if iter.closed {
		return fmt.Errorf("iterator is closed")
	}

	// 重置到起始位置
	return iter.Rewind()
}

// Last 移动到最后一个记录
func (iter *BTreeIterator) Last() error {
	if iter.closed {
		return fmt.Errorf("iterator is closed")
	}

	// 释放当前节点
	iter.releaseCurrentNode()

	// 找到最后一个叶子节点
	lastNode, err := iter.findLastLeafNode()
	if err != nil {
		return err
	}

	iter.currentNode = lastNode

	// 定位到最后一个记录
	records, err := iter.currentNode.getAllRecords()
	if err != nil {
		return err
	}

	if len(records) == 0 {
		iter.valid = false
		return nil
	}

	iter.currentIndex = len(records) - 1
	iter.currentRecord = records[iter.currentIndex]
	iter.valid = true
	iter.itemsRead = 1

	// 检查是否在范围内
	if !iter.isCurrentRecordInRange() {
		iter.valid = false
	}

	return nil
}

// Prev 移动到上一个记录（独立的Prev方法）
func (iter *BTreeIterator) Prev() {
	if iter.closed {
		iter.valid = false
		return
	}

	// 直接调用moveToPrevious
	iter.moveToPrevious()

	// 检查是否仍在范围内
	if iter.valid && !iter.isCurrentRecordInRange() {
		iter.valid = false
	}
}
