/*
JadeDB B+树节点实现模块

本模块实现B+树的核心数据结构，包括内部节点和叶子节点。
参考InnoDB的设计，实现高效的节点操作和数据组织。

核心设计原理：
1. 聚簇索引：数据和索引存储在一起，减少随机访问
2. 槽目录：支持二分查找，提高查找效率
3. 变长记录：支持不同大小的键值对
4. 页面内压缩：最大化页面利用率
5. 并发友好：支持页面级锁机制

节点结构设计：
- 页面头部（56字节）：页面元数据（已有）
- 节点头部（32字节）：B+树节点特定元数据
- 槽目录（变长）：记录偏移量数组
- 记录区域（变长）：实际的键值对数据
- 页面尾部（8字节）：校验信息（已有）

性能特性：
- O(log n) 查找：槽目录支持二分查找
- 空间效率：紧凑的记录存储格式
- 缓存友好：页面大小与系统页面对齐
- 并发安全：页面级读写锁保护
*/

package bplustree

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

// 节点常量定义
const (
	// NodeHeaderSize 节点头部大小（40字节）
	// 包含B+树节点的特定元数据信息
	// 节点类型(8) + 空间管理(8) + 记录管理(8) + 链接信息(16) = 40字节
	NodeHeaderSize = 40

	// SlotSize 槽目录条目大小（2字节）
	// 每个槽存储一个记录的偏移量
	SlotSize = 2

	// MinRecordSize 最小记录大小
	// 记录头部 + 最小键长度 + 最小值长度
	MinRecordSize = 16

	// MaxSlotCount 最大槽数量
	// 基于页面大小和最小记录大小计算
	MaxSlotCount = (PageSize - PageHeaderSize - NodeHeaderSize - PageTrailerSize) / (SlotSize + MinRecordSize)

	// NodeFillFactor 节点填充因子
	// 用于触发页面分裂，保持适当的空间利用率
	NodeFillFactor = 0.875 // 87.5%
)

// NodeHeader 定义B+树节点头部结构（40字节）
// 包含节点的特定元数据信息
type NodeHeader struct {
	// 节点类型和状态（8字节）
	NodeType  NodeType // 节点类型（内部/叶子）（1字节）
	IsRoot    bool     // 是否为根节点（1字节）
	Level     uint16   // 节点在树中的层级（2字节）
	SlotCount uint16   // 槽数量（2字节）
	Reserved1 uint16   // 保留字段（2字节）

	// 空间管理（8字节）
	FreeSpaceOffset uint16 // 空闲空间起始偏移（2字节）
	FreeSpaceSize   uint16 // 空闲空间大小（2字节）
	GarbageSize     uint16 // 垃圾空间大小（2字节）
	Reserved2       uint16 // 保留字段（2字节）

	// 记录管理（8字节）
	MinKey uint32 // 最小键的哈希值（4字节）
	MaxKey uint32 // 最大键的哈希值（4字节）

	// 链接信息（8字节）
	LeftSibling  uint64 // 左兄弟页面ID（8字节）
	RightSibling uint64 // 右兄弟页面ID（8字节）
}

// NodeType 定义节点类型
type NodeType uint8

const (
	// InternalNodeType 内部节点
	// 只存储键和指向子页面的指针，不存储实际数据
	InternalNodeType NodeType = iota

	// LeafNodeType 叶子节点
	// 存储实际的键值对数据
	LeafNodeType
)

// Record 表示B+树中的一条记录
type Record struct {
	// 记录头部
	RecordType RecordType // 记录类型（1字节）
	KeySize    uint16     // 键大小（2字节）
	ValueSize  uint32     // 值大小（4字节）
	Flags      uint8      // 记录标志（1字节）

	// 记录数据
	Key   []byte // 键数据
	Value []byte // 值数据（叶子节点）或子页面ID（内部节点）
}

// RecordType 定义记录类型
type RecordType uint8

const (
	// DataRecord 数据记录（叶子节点）
	DataRecord RecordType = iota
	// IndexRecord 索引记录（内部节点）
	IndexRecord
	// DeletedRecord 已删除记录
	DeletedRecord
)

// Node 表示B+树节点的操作接口
// 提供节点的基本操作方法
type Node struct {
	page   *Page       // 关联的页面
	header *NodeHeader // 节点头部
}

// NewNode 创建新的B+树节点
func NewNode(page *Page, nodeType NodeType) (*Node, error) {
	if page == nil {
		return nil, fmt.Errorf("page cannot be nil")
	}

	node := &Node{
		page: page,
		header: &NodeHeader{
			NodeType:        nodeType,
			IsRoot:          false,
			Level:           0,
			SlotCount:       0,
			FreeSpaceOffset: PageHeaderSize + NodeHeaderSize,
			FreeSpaceSize:   PageSize - PageHeaderSize - NodeHeaderSize - PageTrailerSize,
			GarbageSize:     0,
		},
	}

	// 写入节点头部到页面
	if err := node.writeNodeHeader(); err != nil {
		return nil, err
	}

	return node, nil
}

// LoadNode 从页面加载B+树节点
func LoadNode(page *Page) (*Node, error) {
	if page == nil {
		return nil, fmt.Errorf("page cannot be nil")
	}

	node := &Node{
		page:   page,
		header: &NodeHeader{},
	}

	// 从页面读取节点头部
	if err := node.readNodeHeader(); err != nil {
		return nil, err
	}

	// 验证页面类型和节点类型的一致性
	var expectedNodeType NodeType
	switch page.Type {
	case LeafPage, RootPage:
		// 根页面可能是叶子节点（当树只有一层时）
		expectedNodeType = LeafNodeType
	case InternalPage:
		expectedNodeType = InternalNodeType
	default:
		// 对于其他页面类型（如MetaPage、FreelistPage等），不进行节点类型检查
		// 这些页面不是B+树节点
		return nil, fmt.Errorf("invalid page type for B+tree node: %d", page.Type)
	}

	// 只有当页面类型明确是B+树节点时才检查节点类型
	if page.Type == InternalPage || page.Type == LeafPage {
		if node.header.NodeType != expectedNodeType {
			return nil, fmt.Errorf("page type mismatch: page type %d, node type %d, expected node type %d",
				page.Type, node.header.NodeType, expectedNodeType)
		}
	}

	return node, nil
}

// writeNodeHeader 将节点头部写入页面
func (n *Node) writeNodeHeader() error {
	offset := PageHeaderSize
	data := n.page.Data[offset : offset+NodeHeaderSize]

	// 节点类型和状态（8字节）
	data[0] = byte(n.header.NodeType)
	if n.header.IsRoot {
		data[1] = 1
	} else {
		data[1] = 0
	}
	binary.LittleEndian.PutUint16(data[2:4], n.header.Level)
	binary.LittleEndian.PutUint16(data[4:6], n.header.SlotCount)
	binary.LittleEndian.PutUint16(data[6:8], n.header.Reserved1)

	// 空间管理（8字节）
	binary.LittleEndian.PutUint16(data[8:10], n.header.FreeSpaceOffset)
	binary.LittleEndian.PutUint16(data[10:12], n.header.FreeSpaceSize)
	binary.LittleEndian.PutUint16(data[12:14], n.header.GarbageSize)
	binary.LittleEndian.PutUint16(data[14:16], n.header.Reserved2)

	// 记录管理（8字节）
	binary.LittleEndian.PutUint32(data[16:20], n.header.MinKey)
	binary.LittleEndian.PutUint32(data[20:24], n.header.MaxKey)

	// 链接信息（16字节）
	binary.LittleEndian.PutUint64(data[24:32], n.header.LeftSibling)
	binary.LittleEndian.PutUint64(data[32:40], n.header.RightSibling)

	n.page.Dirty = true
	n.page.UpdateChecksum()
	return nil
}

// readNodeHeader 从页面读取节点头部
func (n *Node) readNodeHeader() error {
	offset := PageHeaderSize
	data := n.page.Data[offset : offset+NodeHeaderSize]

	// 节点类型和状态（8字节）
	n.header.NodeType = NodeType(data[0])
	n.header.IsRoot = data[1] == 1
	n.header.Level = binary.LittleEndian.Uint16(data[2:4])
	n.header.SlotCount = binary.LittleEndian.Uint16(data[4:6])
	n.header.Reserved1 = binary.LittleEndian.Uint16(data[6:8])

	// 空间管理（8字节）
	n.header.FreeSpaceOffset = binary.LittleEndian.Uint16(data[8:10])
	n.header.FreeSpaceSize = binary.LittleEndian.Uint16(data[10:12])
	n.header.GarbageSize = binary.LittleEndian.Uint16(data[12:14])
	n.header.Reserved2 = binary.LittleEndian.Uint16(data[14:16])

	// 记录管理（8字节）
	n.header.MinKey = binary.LittleEndian.Uint32(data[16:20])
	n.header.MaxKey = binary.LittleEndian.Uint32(data[20:24])

	// 链接信息（16字节）
	n.header.LeftSibling = binary.LittleEndian.Uint64(data[24:32])
	n.header.RightSibling = binary.LittleEndian.Uint64(data[32:40])

	return nil
}

// IsLeaf 检查是否为叶子节点
func (n *Node) IsLeaf() bool {
	return n.header.NodeType == LeafNodeType
}

// IsInternal 检查是否为内部节点
func (n *Node) IsInternal() bool {
	return n.header.NodeType == InternalNodeType
}

// IsRoot 检查是否为根节点
func (n *Node) IsRoot() bool {
	return n.header.IsRoot
}

// SetRoot 设置根节点标志
func (n *Node) SetRoot(isRoot bool) error {
	n.header.IsRoot = isRoot
	return n.writeNodeHeader()
}

// GetLevel 获取节点层级
func (n *Node) GetLevel() uint16 {
	return n.header.Level
}

// SetLevel 设置节点层级
func (n *Node) SetLevel(level uint16) error {
	n.header.Level = level
	return n.writeNodeHeader()
}

// GetRecordCount 获取记录数量
func (n *Node) GetRecordCount() uint16 {
	return n.header.SlotCount
}

// GetFreeSpace 获取空闲空间大小
func (n *Node) GetFreeSpace() uint16 {
	return n.header.FreeSpaceSize
}

// GetPageID 获取页面ID
func (n *Node) GetPageID() uint64 {
	return n.page.ID
}

// GetPage 获取关联的页面
func (n *Node) GetPage() *Page {
	return n.page
}

// getSlotOffset 获取槽目录的起始偏移
func (n *Node) getSlotOffset() uint16 {
	return PageHeaderSize + NodeHeaderSize
}

// getSlot 获取指定索引的槽值（记录偏移量）
func (n *Node) getSlot(index uint16) (uint16, error) {
	if index >= n.header.SlotCount {
		return 0, fmt.Errorf("slot index %d out of range", index)
	}

	slotOffset := n.getSlotOffset() + index*SlotSize
	return binary.LittleEndian.Uint16(n.page.Data[slotOffset : slotOffset+SlotSize]), nil
}

// setSlot 设置指定索引的槽值
func (n *Node) setSlot(index uint16, recordOffset uint16) error {
	if index >= MaxSlotCount {
		return fmt.Errorf("slot index %d exceeds maximum", index)
	}

	slotOffset := n.getSlotOffset() + index*SlotSize
	binary.LittleEndian.PutUint16(n.page.Data[slotOffset:slotOffset+SlotSize], recordOffset)
	n.page.Dirty = true
	n.page.UpdateChecksum()
	return nil
}

// serializeRecord 序列化记录到字节数组
func (r *Record) serializeRecord() []byte {
	// 计算记录总大小
	totalSize := 8 + len(r.Key) + len(r.Value) // 头部8字节 + 键 + 值
	data := make([]byte, totalSize)

	// 写入记录头部
	data[0] = byte(r.RecordType)
	binary.LittleEndian.PutUint16(data[1:3], r.KeySize)
	binary.LittleEndian.PutUint32(data[3:7], r.ValueSize)
	data[7] = r.Flags

	// 写入键和值
	copy(data[8:8+len(r.Key)], r.Key)
	copy(data[8+len(r.Key):], r.Value)

	return data
}

// deserializeRecord 从字节数组反序列化记录
func deserializeRecord(data []byte) (*Record, error) {
	if len(data) < 8 {
		return nil, fmt.Errorf("record data too short")
	}

	record := &Record{
		RecordType: RecordType(data[0]),
		KeySize:    binary.LittleEndian.Uint16(data[1:3]),
		ValueSize:  binary.LittleEndian.Uint32(data[3:7]),
		Flags:      data[7],
	}

	// 验证数据长度
	expectedSize := 8 + int(record.KeySize) + int(record.ValueSize)
	if len(data) < expectedSize {
		return nil, fmt.Errorf("record data incomplete: expected %d, got %d", expectedSize, len(data))
	}

	// 读取键和值
	keyStart := 8
	keyEnd := keyStart + int(record.KeySize)
	valueStart := keyEnd
	valueEnd := valueStart + int(record.ValueSize)

	record.Key = make([]byte, record.KeySize)
	record.Value = make([]byte, record.ValueSize)
	copy(record.Key, data[keyStart:keyEnd])
	copy(record.Value, data[valueStart:valueEnd])

	return record, nil
}

// insertRecord 在节点中插入记录
func (n *Node) insertRecord(key []byte, value []byte, recordType RecordType) error {
	// 首先检查键是否已存在
	if existingRecord, index, err := n.searchRecord(key); err == nil {
		// 键已存在，更新现有记录
		return n.updateRecord(uint16(index), existingRecord, value)
	}

	// 创建新记录
	record := &Record{
		RecordType: recordType,
		KeySize:    uint16(len(key)),
		ValueSize:  uint32(len(value)),
		Flags:      0,
		Key:        key,
		Value:      value,
	}

	// 序列化记录
	recordData := record.serializeRecord()
	recordSize := len(recordData)

	// 检查空间是否足够
	requiredSpace := recordSize + SlotSize
	if int(n.header.FreeSpaceSize) < requiredSpace {
		return fmt.Errorf("insufficient space: need %d, have %d", requiredSpace, n.header.FreeSpaceSize)
	}

	// 找到插入位置（保持键的有序性）
	insertPos, err := n.findInsertPosition(key)
	if err != nil {
		return err
	}

	// 分配记录空间（从页面末尾向前分配）
	recordOffset := n.header.FreeSpaceOffset + n.header.FreeSpaceSize - uint16(recordSize)

	// 写入记录数据
	copy(n.page.Data[recordOffset:recordOffset+uint16(recordSize)], recordData)
	n.page.Dirty = true

	// 插入槽（需要移动后续槽）
	if err := n.insertSlot(insertPos, recordOffset); err != nil {
		return err
	}

	// 更新节点头部
	n.header.SlotCount++
	n.header.FreeSpaceSize -= uint16(requiredSpace)
	n.updateKeyRange(key)

	// 写回节点头部
	if err := n.writeNodeHeader(); err != nil {
		return err
	}

	// 最后更新页面校验和
	n.page.UpdateChecksum()
	return nil
}

// findInsertPosition 找到键的插入位置（二分查找）
func (n *Node) findInsertPosition(key []byte) (uint16, error) {
	if n.header.SlotCount == 0 {
		return 0, nil
	}

	// 二分查找
	left, right := uint16(0), n.header.SlotCount
	for left < right {
		mid := (left + right) / 2
		midRecord, err := n.getRecord(mid)
		if err != nil {
			return 0, err
		}

		cmp := bytes.Compare(key, midRecord.Key)
		if cmp <= 0 {
			right = mid
		} else {
			left = mid + 1
		}
	}

	return left, nil
}

// insertSlot 在指定位置插入槽
func (n *Node) insertSlot(position uint16, recordOffset uint16) error {
	// 移动后续槽
	for i := n.header.SlotCount; i > position; i-- {
		slot, err := n.getSlot(i - 1)
		if err != nil {
			return err
		}
		if err := n.setSlot(i, slot); err != nil {
			return err
		}
	}

	// 设置新槽
	return n.setSlot(position, recordOffset)
}

// getRecord 获取指定索引的记录
func (n *Node) getRecord(index uint16) (*Record, error) {
	if index >= n.header.SlotCount {
		return nil, fmt.Errorf("record index %d out of range", index)
	}

	// 获取记录偏移量
	recordOffset, err := n.getSlot(index)
	if err != nil {
		return nil, err
	}

	// 读取记录头部以确定记录大小
	if recordOffset+8 > PageSize {
		return nil, fmt.Errorf("invalid record offset %d", recordOffset)
	}

	headerData := n.page.Data[recordOffset : recordOffset+8]
	keySize := binary.LittleEndian.Uint16(headerData[1:3])
	valueSize := binary.LittleEndian.Uint32(headerData[3:7])

	// 读取完整记录
	recordSize := 8 + keySize + uint16(valueSize)
	if recordOffset+recordSize > PageSize {
		return nil, fmt.Errorf("record extends beyond page boundary")
	}

	recordData := n.page.Data[recordOffset : recordOffset+recordSize]
	return deserializeRecord(recordData)
}

// updateKeyRange 更新键范围
func (n *Node) updateKeyRange(key []byte) {
	keyHash := hashKey(key)
	if n.header.SlotCount == 1 {
		// 第一条记录
		n.header.MinKey = keyHash
		n.header.MaxKey = keyHash
	} else {
		if keyHash < n.header.MinKey {
			n.header.MinKey = keyHash
		}
		if keyHash > n.header.MaxKey {
			n.header.MaxKey = keyHash
		}
	}
}

// hashKey 计算键的哈希值
func hashKey(key []byte) uint32 {
	// 简单的哈希函数，实际应用中可以使用更复杂的算法
	hash := uint32(0)
	for _, b := range key {
		hash = hash*31 + uint32(b)
	}
	return hash
}

// searchRecord 在节点中搜索记录
func (n *Node) searchRecord(key []byte) (*Record, int, error) {
	if n.header.SlotCount == 0 {
		return nil, -1, fmt.Errorf("record not found")
	}

	// 二分查找
	left, right := 0, int(n.header.SlotCount)-1
	for left <= right {
		mid := (left + right) / 2
		record, err := n.getRecord(uint16(mid))
		if err != nil {
			return nil, -1, err
		}

		cmp := bytes.Compare(key, record.Key)
		if cmp == 0 {
			return record, mid, nil
		} else if cmp < 0 {
			right = mid - 1
		} else {
			left = mid + 1
		}
	}

	return nil, -1, fmt.Errorf("record not found")
}

// updateRecord 更新指定索引的记录值
func (n *Node) updateRecord(index uint16, existingRecord *Record, newValue []byte) error {
	if index >= n.header.SlotCount {
		return fmt.Errorf("record index %d out of range", index)
	}

	// 如果新值大小与旧值相同，可以就地更新
	if len(newValue) == len(existingRecord.Value) {
		// 获取记录偏移量
		recordOffset, err := n.getSlot(index)
		if err != nil {
			return err
		}

		// 计算值的偏移量（跳过记录头部和键）
		valueOffset := recordOffset + 8 + uint16(len(existingRecord.Key))

		// 直接更新值
		copy(n.page.Data[valueOffset:valueOffset+uint16(len(newValue))], newValue)
		n.page.Dirty = true
		n.page.UpdateChecksum()
		return nil
	}

	// 值大小不同，需要删除旧记录并插入新记录
	// 先保存键
	key := make([]byte, len(existingRecord.Key))
	copy(key, existingRecord.Key)

	// 删除旧记录
	if err := n.deleteRecord(index); err != nil {
		return err
	}

	// 插入新记录（递归调用，但此时键不存在，不会再次更新）
	return n.insertRecord(key, newValue, existingRecord.RecordType)
}

// deleteRecord 删除指定索引的记录
func (n *Node) deleteRecord(index uint16) error {
	if index >= n.header.SlotCount {
		return fmt.Errorf("record index %d out of range", index)
	}

	// 获取要删除的记录信息
	record, err := n.getRecord(index)
	if err != nil {
		return err
	}

	recordSize := 8 + len(record.Key) + len(record.Value)

	// 移动后续槽
	for i := index; i < n.header.SlotCount-1; i++ {
		slot, err := n.getSlot(i + 1)
		if err != nil {
			return err
		}
		if err := n.setSlot(i, slot); err != nil {
			return err
		}
	}

	// 更新节点头部
	n.header.SlotCount--
	n.header.GarbageSize += uint16(recordSize)
	n.header.FreeSpaceSize += SlotSize

	// 如果删除的是最后一条记录，可以回收空间
	if index == n.header.SlotCount {
		n.header.FreeSpaceSize += uint16(recordSize)
		n.header.GarbageSize -= uint16(recordSize)
	}

	// 重新计算键范围
	if err := n.recalculateKeyRange(); err != nil {
		return err
	}

	// 写回节点头部
	if err := n.writeNodeHeader(); err != nil {
		return err
	}

	// 最后更新页面校验和
	n.page.UpdateChecksum()
	return nil
}

// recalculateKeyRange 重新计算键范围
func (n *Node) recalculateKeyRange() error {
	if n.header.SlotCount == 0 {
		n.header.MinKey = 0
		n.header.MaxKey = 0
		return nil
	}

	// 获取第一条和最后一条记录
	firstRecord, err := n.getRecord(0)
	if err != nil {
		return err
	}

	lastRecord, err := n.getRecord(n.header.SlotCount - 1)
	if err != nil {
		return err
	}

	n.header.MinKey = hashKey(firstRecord.Key)
	n.header.MaxKey = hashKey(lastRecord.Key)
	return nil
}

// needsSplit 检查节点是否需要分裂
func (n *Node) needsSplit() bool {
	usedSpace := PageSize - PageHeaderSize - NodeHeaderSize - PageTrailerSize - int(n.header.FreeSpaceSize)
	totalSpace := PageSize - PageHeaderSize - NodeHeaderSize - PageTrailerSize
	fillRatio := float64(usedSpace) / float64(totalSpace)
	return fillRatio > NodeFillFactor
}

// canMerge 检查节点是否可以与兄弟节点合并
func (n *Node) canMerge(sibling *Node) bool {
	if sibling == nil {
		return false
	}

	// 计算合并后的空间需求
	myUsedSpace := PageSize - PageHeaderSize - NodeHeaderSize - PageTrailerSize - int(n.header.FreeSpaceSize)
	siblingUsedSpace := PageSize - PageHeaderSize - NodeHeaderSize - PageTrailerSize - int(sibling.header.FreeSpaceSize)
	totalUsedSpace := myUsedSpace + siblingUsedSpace

	totalAvailableSpace := PageSize - PageHeaderSize - NodeHeaderSize - PageTrailerSize
	return totalUsedSpace <= totalAvailableSpace
}

// getAllRecords 获取节点中的所有记录
func (n *Node) getAllRecords() ([]*Record, error) {
	records := make([]*Record, n.header.SlotCount)
	for i := uint16(0); i < n.header.SlotCount; i++ {
		record, err := n.getRecord(i)
		if err != nil {
			return nil, err
		}
		records[i] = record
	}
	return records, nil
}

// clear 清空节点内容
func (n *Node) clear() error {
	n.header.SlotCount = 0
	n.header.FreeSpaceOffset = PageHeaderSize + NodeHeaderSize
	n.header.FreeSpaceSize = PageSize - PageHeaderSize - NodeHeaderSize - PageTrailerSize
	n.header.GarbageSize = 0
	n.header.MinKey = 0
	n.header.MaxKey = 0

	return n.writeNodeHeader()
}

// compact 压缩节点空间，清理垃圾空间
func (n *Node) compact() error {
	if n.header.GarbageSize == 0 {
		return nil // 无需压缩
	}

	// 获取所有记录
	records, err := n.getAllRecords()
	if err != nil {
		return err
	}

	// 清空节点
	if err := n.clear(); err != nil {
		return err
	}

	// 重新插入所有记录
	for _, record := range records {
		if record.RecordType != DeletedRecord {
			if err := n.insertRecord(record.Key, record.Value, record.RecordType); err != nil {
				return err
			}
		}
	}

	return nil
}

// String 返回节点的字符串表示
func (n *Node) String() string {
	nodeTypeStr := "Internal"
	if n.IsLeaf() {
		nodeTypeStr = "Leaf"
	}

	return fmt.Sprintf("Node{PageID:%d, Type:%s, Level:%d, Records:%d, FreeSpace:%d, IsRoot:%v}",
		n.page.ID, nodeTypeStr, n.header.Level, n.header.SlotCount, n.header.FreeSpaceSize, n.header.IsRoot)
}
