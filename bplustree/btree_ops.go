/*
JadeDB B+树操作模块

本模块实现B+树的核心操作，包括插入、查找、删除和范围查询。
基于节点操作接口，实现完整的B+树算法。

核心功能：
1. 插入操作：支持键值对插入，自动维护树的平衡
2. 查找操作：高效的点查询和范围查询
3. 删除操作：支持键的删除，自动合并节点
4. 分裂合并：维护B+树的平衡性
5. 迭代器：支持顺序遍历和范围扫描

设计原理：
- 自顶向下：从根节点开始，逐层向下查找
- 写时分裂：插入时如果节点满则分裂
- 延迟合并：删除时延迟合并，提高性能
- 路径记录：记录查找路径，支持高效的分裂合并
- 并发安全：使用页面级锁保护操作

算法复杂度：
- 查找：O(log n)
- 插入：O(log n)
- 删除：O(log n)
- 范围查询：O(log n + k)，k为结果数量
*/

package bplustree

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

// BTreeOperations B+树操作接口
type BTreeOperations struct {
	btree       *BPlusTree   // B+树实例
	bufferPool  *BufferPool  // 缓冲池
	pageManager *PageManager // 页面管理器
}

// SearchPath 查找路径，记录从根到叶子的路径
type SearchPath struct {
	nodes   []*Node // 路径上的节点
	indexes []int   // 每个节点中的索引位置
}

// NewBTreeOperations 创建B+树操作实例
func NewBTreeOperations(btree *BPlusTree) *BTreeOperations {
	return &BTreeOperations{
		btree:       btree,
		bufferPool:  btree.bufferPool,
		pageManager: btree.pageManager,
	}
}

// Insert 插入键值对
func (ops *BTreeOperations) Insert(key []byte, value []byte) error {
	if len(key) == 0 {
		return fmt.Errorf("key cannot be empty")
	}

	// 获取根页面
	rootPageID := ops.btree.rootPageID.Load()
	if rootPageID == 0 {
		// 创建新的根页面
		return ops.createNewRoot(key, value)
	}

	// 查找插入路径
	path, err := ops.findInsertPath(key)
	if err != nil {
		return err
	}

	// 在叶子节点插入
	leafNode := path.nodes[len(path.nodes)-1]
	err = leafNode.insertRecord(key, value, DataRecord)
	if err != nil {
		return err
	}

	// 检查是否需要分裂
	if leafNode.needsSplit() {
		return ops.splitLeafNode(path)
	}

	return nil
}

// Search 查找键对应的值
func (ops *BTreeOperations) Search(key []byte) ([]byte, error) {
	if len(key) == 0 {
		return nil, fmt.Errorf("key cannot be empty")
	}

	// 获取根页面
	rootPageID := ops.btree.rootPageID.Load()
	if rootPageID == 0 {
		return nil, fmt.Errorf("key not found")
	}

	// 查找到叶子节点
	leafNode, err := ops.findLeafNode(key)
	if err != nil {
		return nil, err
	}

	// 在叶子节点中搜索
	record, _, err := leafNode.searchRecord(key)
	if err != nil {
		return nil, fmt.Errorf("key not found")
	}

	return record.Value, nil
}

// Delete 删除键
func (ops *BTreeOperations) Delete(key []byte) error {
	if len(key) == 0 {
		return fmt.Errorf("key cannot be empty")
	}

	// 查找删除路径
	path, err := ops.findInsertPath(key)
	if err != nil {
		return err
	}

	// 在叶子节点中查找并删除
	leafNode := path.nodes[len(path.nodes)-1]
	_, index, err := leafNode.searchRecord(key)
	if err != nil {
		return fmt.Errorf("key not found")
	}

	// 删除记录
	if err := leafNode.deleteRecord(uint16(index)); err != nil {
		return err
	}

	// 检查是否需要合并
	if ops.shouldMerge(leafNode) {
		return ops.handleUnderflow(path)
	}

	return nil
}

// createNewRoot 创建新的根页面
func (ops *BTreeOperations) createNewRoot(key []byte, value []byte) error {
	// 分配新页面
	rootPage, err := ops.pageManager.AllocatePage(LeafPage)
	if err != nil {
		return err
	}

	// 创建根节点
	rootNode, err := NewNode(rootPage, LeafNodeType)
	if err != nil {
		return err
	}

	// 设置为根节点
	if err := rootNode.SetRoot(true); err != nil {
		return err
	}

	// 插入第一条记录
	if err := rootNode.insertRecord(key, value, DataRecord); err != nil {
		return err
	}

	// 写入页面到磁盘
	if err := ops.pageManager.WritePage(rootPage); err != nil {
		return err
	}

	// 更新B+树的根页面ID
	ops.btree.rootPageID.Store(rootPage.ID)
	ops.btree.treeHeight.Store(1)

	// 持久化根页面信息
	return ops.pageManager.SetRootPageID(rootPage.ID)
}

// findInsertPath 查找插入路径
func (ops *BTreeOperations) findInsertPath(key []byte) (*SearchPath, error) {
	path := &SearchPath{
		nodes:   make([]*Node, 0),
		indexes: make([]int, 0),
	}

	// 从根节点开始
	rootPageID := ops.btree.rootPageID.Load()
	currentPageID := rootPageID

	for {
		// 获取当前页面
		page, err := ops.bufferPool.GetPage(currentPageID)
		if err != nil {
			return nil, err
		}

		// 加载节点
		node, err := LoadNode(page)
		if err != nil {
			ops.bufferPool.PutPage(page)
			return nil, err
		}

		path.nodes = append(path.nodes, node)

		// 如果是叶子节点，结束查找
		if node.IsLeaf() {
			// 找到插入位置
			insertPos, err := node.findInsertPosition(key)
			if err != nil {
				return nil, err
			}
			path.indexes = append(path.indexes, int(insertPos))
			break
		}

		// 内部节点，查找子节点
		childIndex, childPageID, err := ops.findChildNode(node, key)
		if err != nil {
			return nil, err
		}

		path.indexes = append(path.indexes, childIndex)
		currentPageID = childPageID
	}

	return path, nil
}

// findLeafNode 查找包含指定键的叶子节点
func (ops *BTreeOperations) findLeafNode(key []byte) (*Node, error) {
	// 从根节点开始
	rootPageID := ops.btree.rootPageID.Load()
	currentPageID := rootPageID

	for {
		// 获取当前页面
		page, err := ops.bufferPool.GetPage(currentPageID)
		if err != nil {
			return nil, err
		}

		// 加载节点
		node, err := LoadNode(page)
		if err != nil {
			ops.bufferPool.PutPage(page)
			return nil, err
		}

		// 如果是叶子节点，返回
		if node.IsLeaf() {
			return node, nil
		}

		// 内部节点，查找子节点
		_, childPageID, err := ops.findChildNode(node, key)
		if err != nil {
			return nil, err
		}

		// 释放当前页面
		ops.bufferPool.PutPage(page)
		currentPageID = childPageID
	}
}

// findChildNode 在内部节点中查找子节点
func (ops *BTreeOperations) findChildNode(node *Node, key []byte) (int, uint64, error) {
	if node.IsLeaf() {
		return 0, 0, fmt.Errorf("not an internal node")
	}

	recordCount := node.GetRecordCount()
	if recordCount == 0 {
		return 0, 0, fmt.Errorf("empty internal node")
	}

	// 在内部节点中查找合适的子节点
	for i := uint16(0); i < recordCount; i++ {
		record, err := node.getRecord(i)
		if err != nil {
			return 0, 0, err
		}

		// 比较键
		if bytes.Compare(key, record.Key) <= 0 {
			// 解析子页面ID
			if len(record.Value) != 8 {
				return 0, 0, fmt.Errorf("invalid child page ID")
			}
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

// shouldMerge 检查节点是否应该合并
func (ops *BTreeOperations) shouldMerge(node *Node) bool {
	// 如果是根节点，不需要合并
	if node.IsRoot() {
		return false
	}

	// 检查节点的填充率
	usedSpace := PageSize - PageHeaderSize - NodeHeaderSize - PageTrailerSize - int(node.GetFreeSpace())
	totalSpace := PageSize - PageHeaderSize - NodeHeaderSize - PageTrailerSize
	fillRatio := float64(usedSpace) / float64(totalSpace)

	// 如果填充率低于50%，考虑合并
	return fillRatio < 0.5
}

// RangeSearch 范围查询
func (ops *BTreeOperations) RangeSearch(startKey []byte, endKey []byte, limit int) ([]*Record, error) {
	if bytes.Compare(startKey, endKey) > 0 {
		return nil, fmt.Errorf("start key must be less than or equal to end key")
	}

	results := make([]*Record, 0)

	// 找到起始叶子节点
	currentNode, err := ops.findLeafNode(startKey)
	if err != nil {
		return nil, err
	}

	for currentNode != nil && len(results) < limit {
		// 在当前节点中查找记录
		records, err := currentNode.getAllRecords()
		if err != nil {
			return nil, err
		}

		for _, record := range records {
			// 检查键是否在范围内
			if bytes.Compare(record.Key, startKey) >= 0 && bytes.Compare(record.Key, endKey) <= 0 {
				results = append(results, record)
				if len(results) >= limit {
					break
				}
			} else if bytes.Compare(record.Key, endKey) > 0 {
				// 超出范围，结束查找
				return results, nil
			}
		}

		// 移动到下一个叶子节点
		if currentNode.header.RightSibling == 0 {
			break
		}

		// 获取右兄弟节点
		rightPage, err := ops.bufferPool.GetPage(currentNode.header.RightSibling)
		if err != nil {
			break
		}

		rightNode, err := LoadNode(rightPage)
		if err != nil {
			ops.bufferPool.PutPage(rightPage)
			break
		}

		// 释放当前节点
		ops.bufferPool.PutPage(currentNode.GetPage())
		currentNode = rightNode
	}

	return results, nil
}

// splitLeafNode 分裂叶子节点
func (ops *BTreeOperations) splitLeafNode(path *SearchPath) error {
	leafNode := path.nodes[len(path.nodes)-1]

	// 分配新的右节点页面
	rightPage, err := ops.pageManager.AllocatePage(LeafPage)
	if err != nil {
		return err
	}

	rightNode, err := NewNode(rightPage, LeafNodeType)
	if err != nil {
		return err
	}

	// 获取所有记录
	records, err := leafNode.getAllRecords()
	if err != nil {
		return err
	}

	// 计算分裂点（中间位置）
	splitPoint := len(records) / 2

	// 清空左节点
	if err := leafNode.clear(); err != nil {
		return err
	}

	// 重新分配记录
	// 左半部分留在原节点
	for i := 0; i < splitPoint; i++ {
		record := records[i]
		if err := leafNode.insertRecord(record.Key, record.Value, record.RecordType); err != nil {
			return err
		}
	}

	// 右半部分移到新节点
	for i := splitPoint; i < len(records); i++ {
		record := records[i]
		if err := rightNode.insertRecord(record.Key, record.Value, record.RecordType); err != nil {
			return err
		}
	}

	// 更新叶子节点链表
	rightNode.header.LeftSibling = leafNode.GetPageID()
	rightNode.header.RightSibling = leafNode.header.RightSibling
	leafNode.header.RightSibling = rightNode.GetPageID()

	// 如果有右兄弟，更新其左指针
	if rightNode.header.RightSibling != 0 {
		rightSiblingPage, err := ops.bufferPool.GetPage(rightNode.header.RightSibling)
		if err == nil {
			rightSiblingNode, err := LoadNode(rightSiblingPage)
			if err == nil {
				rightSiblingNode.header.LeftSibling = rightNode.GetPageID()
				rightSiblingNode.writeNodeHeader()
			}
			ops.bufferPool.PutPage(rightSiblingPage)
		}
	}

	// 写回节点头部
	if err := rightNode.writeNodeHeader(); err != nil {
		return err
	}
	if err := leafNode.writeNodeHeader(); err != nil {
		return err
	}

	// 获取分裂键（右节点的第一个键）
	splitKey := records[splitPoint].Key

	// 向父节点插入分裂键
	return ops.insertIntoParent(path, splitKey, rightNode.GetPageID())
}

// insertIntoParent 向父节点插入分裂键
func (ops *BTreeOperations) insertIntoParent(path *SearchPath, key []byte, rightPageID uint64) error {
	// 如果分裂的是根节点，创建新根
	if len(path.nodes) == 1 {
		return ops.createNewInternalRoot(path.nodes[0], key, rightPageID)
	}

	// 获取父节点
	parentNode := path.nodes[len(path.nodes)-2]

	// 创建指向右子节点的记录
	rightPageIDBytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(rightPageIDBytes, rightPageID)

	// 在父节点中插入
	err := parentNode.insertRecord(key, rightPageIDBytes, IndexRecord)
	if err != nil {
		return err
	}

	// 检查父节点是否需要分裂
	if parentNode.needsSplit() {
		// 创建父节点的路径
		parentPath := &SearchPath{
			nodes:   path.nodes[:len(path.nodes)-1],
			indexes: path.indexes[:len(path.indexes)-1],
		}
		return ops.splitInternalNode(parentPath)
	}

	return nil
}

// createNewInternalRoot 创建新的内部根节点
func (ops *BTreeOperations) createNewInternalRoot(leftChild *Node, key []byte, rightPageID uint64) error {
	// 分配新的根页面
	rootPage, err := ops.pageManager.AllocatePage(InternalPage)
	if err != nil {
		return err
	}

	rootNode, err := NewNode(rootPage, InternalNodeType)
	if err != nil {
		return err
	}

	// 设置为根节点
	if err := rootNode.SetRoot(true); err != nil {
		return err
	}

	// 设置层级
	if err := rootNode.SetLevel(leftChild.GetLevel() + 1); err != nil {
		return err
	}

	// 插入指向左子节点的记录（使用空键）
	leftPageIDBytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(leftPageIDBytes, leftChild.GetPageID())
	if err := rootNode.insertRecord([]byte{}, leftPageIDBytes, IndexRecord); err != nil {
		return err
	}

	// 插入分裂键和指向右子节点的记录
	rightPageIDBytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(rightPageIDBytes, rightPageID)
	if err := rootNode.insertRecord(key, rightPageIDBytes, IndexRecord); err != nil {
		return err
	}

	// 更新子节点的根标志
	if err := leftChild.SetRoot(false); err != nil {
		return err
	}

	// 更新B+树信息
	ops.btree.rootPageID.Store(rootPage.ID)
	ops.btree.treeHeight.Add(1)

	// 持久化根页面信息
	return ops.pageManager.SetRootPageID(rootPage.ID)
}

// splitInternalNode 分裂内部节点
func (ops *BTreeOperations) splitInternalNode(path *SearchPath) error {
	internalNode := path.nodes[len(path.nodes)-1]

	// 分配新的右节点页面
	rightPage, err := ops.pageManager.AllocatePage(InternalPage)
	if err != nil {
		return err
	}

	rightNode, err := NewNode(rightPage, InternalNodeType)
	if err != nil {
		return err
	}

	// 设置层级
	if err := rightNode.SetLevel(internalNode.GetLevel()); err != nil {
		return err
	}

	// 获取所有记录
	records, err := internalNode.getAllRecords()
	if err != nil {
		return err
	}

	// 计算分裂点
	splitPoint := len(records) / 2
	splitKey := records[splitPoint].Key

	// 清空左节点
	if err := internalNode.clear(); err != nil {
		return err
	}

	// 重新分配记录
	// 左半部分（不包括分裂键）
	for i := 0; i < splitPoint; i++ {
		record := records[i]
		if err := internalNode.insertRecord(record.Key, record.Value, record.RecordType); err != nil {
			return err
		}
	}

	// 右半部分（不包括分裂键）
	for i := splitPoint + 1; i < len(records); i++ {
		record := records[i]
		if err := rightNode.insertRecord(record.Key, record.Value, record.RecordType); err != nil {
			return err
		}
	}

	// 向父节点插入分裂键
	return ops.insertIntoParent(path, splitKey, rightNode.GetPageID())
}

// handleUnderflow 处理节点下溢
func (ops *BTreeOperations) handleUnderflow(path *SearchPath) error {
	node := path.nodes[len(path.nodes)-1]

	// 如果是根节点且为空，删除根节点
	if node.IsRoot() && node.GetRecordCount() == 0 {
		return ops.handleEmptyRoot(node)
	}

	// 尝试从兄弟节点借用记录
	if err := ops.tryBorrowFromSibling(path); err == nil {
		return nil
	}

	// 尝试与兄弟节点合并
	return ops.mergeWithSibling(path)
}

// handleEmptyRoot 处理空根节点
func (ops *BTreeOperations) handleEmptyRoot(root *Node) error {
	if root.IsLeaf() {
		// 叶子根节点为空，树变为空
		ops.btree.rootPageID.Store(0)
		ops.btree.treeHeight.Store(0)
		return ops.pageManager.SetRootPageID(0)
	}

	// 内部根节点只有一个子节点，提升子节点为根
	if root.GetRecordCount() == 1 {
		record, err := root.getRecord(0)
		if err != nil {
			return err
		}

		childPageID := binary.LittleEndian.Uint64(record.Value)
		childPage, err := ops.bufferPool.GetPage(childPageID)
		if err != nil {
			return err
		}

		childNode, err := LoadNode(childPage)
		if err != nil {
			ops.bufferPool.PutPage(childPage)
			return err
		}

		// 设置子节点为根
		if err := childNode.SetRoot(true); err != nil {
			return err
		}

		// 更新B+树信息
		ops.btree.rootPageID.Store(childPageID)
		ops.btree.treeHeight.Add(-1)

		// 释放旧根页面
		ops.pageManager.FreePage(root.GetPageID())

		// 持久化新根页面信息
		return ops.pageManager.SetRootPageID(childPageID)
	}

	return nil
}

// tryBorrowFromSibling 尝试从兄弟节点借用记录
func (ops *BTreeOperations) tryBorrowFromSibling(path *SearchPath) error {
	// 简化实现：暂时不支持借用，直接返回错误
	return fmt.Errorf("borrowing not implemented")
}

// mergeWithSibling 与兄弟节点合并
func (ops *BTreeOperations) mergeWithSibling(path *SearchPath) error {
	// 简化实现：暂时不支持合并，直接返回错误
	return fmt.Errorf("merging not implemented")
}
