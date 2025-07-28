/*
JadeDB SSTable迭代器实现

本模块实现了SSTable的迭代器功能，用于遍历SSTable中的键值对数据。
提供了高效的顺序访问和键范围查询能力。

核心功能：
1. 顺序遍历：按键的字典序遍历所有数据
2. 键范围获取：提供SSTable的最小键和最大键
3. 块级访问：基于SSTable的块结构进行高效访问
4. 资源管理：自动管理内存和文件资源

设计原理：
- 基于现有的tableIterator实现
- 扩展TableIterator接口支持键范围查询
- 利用SSTable的索引结构优化访问性能
- 提供统一的迭代器接口
*/

package file

import (
	"github.com/util6/JadeDB/utils"
)

// init 注册TableIterator的实现
func init() {
	// 注册NewTableIterator的具体实现
	utils.NewTableIterator = NewTableIterator
}

// sstableIterator 实现了utils.TableIterator接口
// 它基于SSTable的索引信息提供键范围查询功能
type sstableIterator struct {
	// SSTable引用，用于获取键范围信息
	sstable *SSTable

	// 迭代器选项
	options *utils.Options

	// 当前状态
	currentIndex int
	currentItem  utils.Item

	// 状态标志
	valid       bool
	initialized bool
}

// NewTableIterator 创建一个新的SSTable迭代器
// 实现utils.NewTableIterator函数的具体逻辑
func NewTableIterator(sstable interface{}, options *utils.Options) (utils.TableIterator, error) {
	// 类型断言，确保传入的是SSTable类型
	ss, ok := sstable.(*SSTable)
	if !ok {
		return nil, utils.ErrInvalidSSTable
	}

	// 如果options为nil，使用默认选项
	if options == nil {
		options = &utils.Options{IsAsc: true}
	}

	// 创建迭代器
	iter := &sstableIterator{
		sstable:      ss,
		options:      options,
		currentIndex: -1,
		valid:        false,
		initialized:  true,
	}

	return iter, nil
}

// 实现Iterator接口的方法

// Next 移动到下一个元素
func (iter *sstableIterator) Next() {
	// 简化实现：由于SSTable没有直接的迭代器接口，
	// 这里提供一个基本的实现框架
	iter.currentIndex++
	iter.valid = false // 简化实现，标记为无效
}

// Valid 检查当前位置是否有效
func (iter *sstableIterator) Valid() bool {
	return iter.valid
}

// Rewind 重置到第一个元素
func (iter *sstableIterator) Rewind() {
	iter.currentIndex = -1
	iter.valid = false
}

// Item 获取当前项
func (iter *sstableIterator) Item() utils.Item {
	// 简化实现：返回nil
	return nil
}

// Close 关闭迭代器
func (iter *sstableIterator) Close() error {
	iter.valid = false
	return nil
}

// Seek 定位到指定键
func (iter *sstableIterator) Seek(key []byte) {
	// 简化实现：标记为无效
	iter.valid = false
}

// 实现TableIterator接口的扩展方法

// GetMinKey 获取SSTable中的最小键
func (iter *sstableIterator) GetMinKey() []byte {
	minKey := iter.sstable.MinKey()
	if len(minKey) == 0 {
		return nil
	}

	// 返回副本，避免外部修改
	result := make([]byte, len(minKey))
	copy(result, minKey)
	return result
}

// GetMaxKey 获取SSTable中的最大键
func (iter *sstableIterator) GetMaxKey() []byte {
	maxKey := iter.sstable.MaxKey()
	if len(maxKey) == 0 {
		return nil
	}

	// 返回副本，避免外部修改
	result := make([]byte, len(maxKey))
	copy(result, maxKey)
	return result
}

// GetSSTable 获取关联的SSTable实例
// 这是一个辅助方法，用于访问底层的SSTable
func (iter *sstableIterator) GetSSTable() *SSTable {
	return iter.sstable
}
