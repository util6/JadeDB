// Copyright 2021 logicrec Project Authors
//
// Licensed under the Apache License, Version 2.0 (the "License")
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

/*
JadeDB 迭代器模块

迭代器提供了遍历数据库中所有键值对的能力，支持：
- 顺序和逆序遍历
- 范围查询和前缀匹配
- 跨多个存储层的统一访问
- 值日志的透明处理

设计原理：
1. 合并迭代器：统一访问内存表和 SSTable
2. 值指针解析：自动从值日志中读取大值
3. 过期过滤：自动跳过已删除或过期的条目
4. 懒加载：只在需要时读取值数据

使用场景：
- 全表扫描和数据导出
- 范围查询和前缀搜索
- 数据分析和统计
- 备份和恢复操作

性能考虑：
- 使用合并迭代器减少重复数据
- 批量读取优化磁盘访问
- 内存缓存减少重复解析
- 异步预取提高吞吐量
*/

package JadeDB

import (
	"github.com/util6/JadeDB/utils"
)

// DBIterator 是 JadeDB 的主要迭代器实现。
// 它提供了遍历整个数据库的能力，自动处理多个存储层的数据合并。
//
// 核心功能：
// 1. 统一访问：将内存表和 SSTable 的数据统一呈现
// 2. 值解析：自动处理值指针，从值日志中读取大值
// 3. 过滤功能：跳过已删除或过期的条目
// 4. 有序遍历：保证按键的字典序返回数据
//
// 实现原理：
// DBIterator 是一个包装器，内部使用合并迭代器来统一多个数据源。
// 它在合并迭代器的基础上添加了值日志处理和过滤逻辑。
//
// 线程安全：
// DBIterator 不是线程安全的，不应在多个 goroutine 间共享。
// 每个 goroutine 应该创建自己的迭代器实例。
type DBIterator struct {
	// iitr 是底层的合并迭代器。
	// 负责统一访问内存表和各层 SSTable 的数据。
	// 提供有序的键值对遍历能力。
	iitr utils.Iterator

	// vlog 是值日志的引用。
	// 用于读取存储在值日志中的大值数据。
	// 当遇到值指针时，通过这个引用读取实际值。
	vlog *valueLog
}

// Item 表示迭代器返回的数据项。
// 这是一个简单的包装器，用于统一接口。
type Item struct {
	// e 是底层的条目数据。
	// 包含键、值、元数据等完整信息。
	e *utils.Entry
}

// Entry 返回数据项中的条目。
// 这是 Item 接口的实现方法。
//
// 返回值：
// 包含完整信息的条目指针
func (it *Item) Entry() *utils.Entry {
	return it.e
}

// NewIterator 创建一个新的数据库迭代器。
// 这是数据库遍历功能的主要入口点。
//
// 参数说明：
// opt: 迭代器选项，包含遍历方向、范围等配置
//
// 返回值：
// 新创建的迭代器实例
//
// 创建过程：
// 1. 收集所有数据源的迭代器（内存表、SSTable）
// 2. 创建合并迭代器统一访问这些数据源
// 3. 包装为 DBIterator 添加值日志处理能力
//
// 性能考虑：
// - 合并迭代器使用堆排序，时间复杂度为 O(k log k)，k 为数据源数量
// - 内存表访问速度最快，SSTable 按层级访问
// - 值日志访问可能涉及磁盘 I/O，会影响遍历速度
func (db *DB) NewIterator(opt *utils.Options) utils.Iterator {
	// 收集所有数据源的迭代器
	iters := make([]utils.Iterator, 0)
	iters = append(iters, db.lsm.NewIterators(opt)...)

	// 创建 DBIterator 实例
	res := &DBIterator{
		vlog: db.vlog,                                   // 设置值日志引用
		iitr: db.lsm.NewMergeIterator(iters, opt.IsAsc), // 创建合并迭代器
	}

	return res
}

// Next 将迭代器移动到下一个有效的条目。
// 这个方法会自动跳过已删除或过期的条目，确保返回的都是有效数据。
//
// 工作流程：
// 1. 调用底层合并迭代器的 Next 方法
// 2. 检查当前条目是否有效（未删除且未过期）
// 3. 如果无效，继续移动到下一个条目
// 4. 重复直到找到有效条目或到达末尾
//
// 性能考虑：
// - 跳过无效条目可能需要多次底层迭代
// - 大量删除的数据库可能影响遍历性能
// - 建议定期进行压缩以清理无效数据
//
// 使用注意：
// - 调用前应确保迭代器处于有效状态
// - 调用后应检查 Valid() 确认是否到达末尾
func (iter *DBIterator) Next() {
	// 移动底层迭代器到下一个位置
	iter.iitr.Next()

	// 跳过所有无效的条目（已删除、过期或值为空）
	for ; iter.Valid() && iter.Item() == nil; iter.iitr.Next() {
		// 循环体为空，条件检查在 for 语句中完成
	}
}

// Valid 检查迭代器是否指向有效的条目。
// 这个方法用于判断是否已经遍历完所有数据。
//
// 返回值：
// true: 迭代器指向有效条目，可以调用 Item() 获取数据
// false: 迭代器已到达末尾，没有更多数据
//
// 使用场景：
// - 在 for 循环中作为继续条件
// - 在调用 Item() 前进行检查
// - 判断遍历是否完成
func (iter *DBIterator) Valid() bool {
	return iter.iitr.Valid()
}

// Rewind 将迭代器重置到起始位置。
// 这个方法允许重新开始遍历，无需创建新的迭代器。
//
// 工作流程：
// 1. 调用底层合并迭代器的 Rewind 方法
// 2. 跳过起始位置的无效条目
// 3. 定位到第一个有效条目
//
// 使用场景：
// - 需要多次遍历同一数据集
// - 在遍历过程中需要重新开始
// - 实现复杂的数据处理逻辑
//
// 性能考虑：
// - Rewind 操作相对较快，主要是重置内部状态
// - 跳过无效条目可能需要额外时间
// - 比创建新迭代器更高效
func (iter *DBIterator) Rewind() {
	// 重置底层迭代器到起始位置
	iter.iitr.Rewind()

	// 跳过起始位置的无效条目
	for ; iter.Valid() && iter.Item() == nil; iter.iitr.Next() {
		// 循环体为空，条件检查在 for 语句中完成
	}
}

// Item 返回迭代器当前指向的数据项。
// 这是获取遍历数据的主要方法，会自动处理值指针和过滤逻辑。
//
// 返回值：
// 有效的数据项，如果当前位置无效则返回 nil
//
// 处理流程：
// 1. 从底层迭代器获取原始条目
// 2. 检查是否为值指针，如果是则从值日志读取实际值
// 3. 检查条目是否已删除或过期
// 4. 构造并返回完整的条目数据
//
// 值指针处理：
// - 自动识别值指针标记
// - 从值日志中读取实际值数据
// - 处理读取错误和资源清理
//
// 过滤逻辑：
// - 跳过已删除的条目（墓碑标记）
// - 跳过已过期的条目（TTL 检查）
// - 跳过值为空的条目
//
// 错误处理：
// - 值日志读取失败时返回 nil
// - 自动执行回调函数进行资源清理
func (iter *DBIterator) Item() utils.Item {
	// 从底层迭代器获取原始条目
	e := iter.iitr.Item().Entry()
	var value []byte

	// 处理值指针：如果条目包含值指针，从值日志中读取实际值
	if e != nil && utils.IsValuePtr(e) {
		var vp utils.ValuePtr
		vp.Decode(e.Value) // 解码值指针

		// 从值日志中读取实际值
		result, cb, err := iter.vlog.read(&vp)
		defer utils.RunCallback(cb) // 确保资源清理回调被执行

		if err != nil {
			return nil // 读取失败，返回空项
		}

		// 安全复制值数据，避免内存引用问题
		value = utils.SafeCopy(nil, result)
	} else if e != nil {
		// 如果不是值指针，直接使用条目中的值
		value = e.Value
	}

	// 过滤无效条目：已删除、过期或值为空
	if e.IsDeletedOrExpired() || value == nil {
		return nil
	}

	// 构造完整的条目数据
	res := &utils.Entry{
		Key:          e.Key,          // 键
		Value:        value,          // 值（可能从值日志读取）
		ExpiresAt:    e.ExpiresAt,    // 过期时间
		Meta:         e.Meta,         // 元数据
		Version:      e.Version,      // 版本号
		Offset:       e.Offset,       // 偏移量
		Hlen:         e.Hlen,         // 哈希长度
		ValThreshold: e.ValThreshold, // 值阈值
	}

	return res
}

// Close 关闭迭代器并释放相关资源。
// 这个方法应该在迭代器使用完毕后调用，确保资源的正确释放。
//
// 返回值：
// 如果关闭过程中发生错误，返回错误信息
//
// 清理过程：
// 1. 关闭底层的合并迭代器
// 2. 释放所有子迭代器的资源
// 3. 清理内存缓存和文件句柄
//
// 使用建议：
// - 使用 defer 语句确保迭代器被关闭
// - 在长时间运行的程序中及时关闭迭代器
// - 避免关闭后继续使用迭代器
func (iter *DBIterator) Close() error {
	return iter.iitr.Close()
}

// Seek 将迭代器定位到指定键的位置。
// 这个方法用于实现范围查询和定点查找。
//
// 参数说明：
// key: 目标键，迭代器将定位到大于等于此键的第一个位置
//
// 实现说明：
// 当前实现为空，需要根据具体需求补充实现。
// 典型的实现会调用底层迭代器的 Seek 方法。
//
// TODO: 实现 Seek 功能
// - 调用底层合并迭代器的 Seek 方法
// - 跳过无效条目定位到有效位置
// - 处理边界情况和错误条件
func (iter *DBIterator) Seek(key []byte) {
	// TODO: 实现 Seek 功能
	// iter.iitr.Seek(key)
	// for ; iter.Valid() && iter.Item() == nil; iter.iitr.Next() {
	// }
}
