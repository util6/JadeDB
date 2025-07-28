// Copyright 2021 hardcore-os Project Authors
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

package utils

// Iterator 定义了一个迭代器接口，用于遍历键值存储中的数据。
// 它提供了一组方法来遍历、访问和定位数据集中的项。
type Iterator interface {
	// Next 移动到迭代器的下一个有效项。
	Next()

	// Valid 返回当前项是否有效。
	// 如果返回true，则表示当前项有效，可以调用Item方法获取。
	Valid() bool

	// Rewind 将迭代器重置到第一个元素之前。
	Rewind()

	// Item 返回当前项。
	// 调用此方法之前应确保Valid返回true。
	Item() Item

	// Close 关闭迭代器。
	// 关闭后，不再允许调用其他方法。
	Close() error

	// Seek 将迭代器移动到指定键的位置。
	// 如果存在指定的键，则移动到该键的位置；如果不存在，则移动到下一个大于该键的位置。
	Seek(key []byte)
}

// Item 定义了数据项的接口。
// 它提供了一种访问项内部Entry的方法。
type Item interface {
	// Entry 返回项的Entry结构体指针。
	Entry() *Entry
}

// Options 定义了迭代器选项的结构。
// 重构后的设计更加灵活，支持更多的迭代器配置选项。
type Options struct {
	// 基本选项
	Prefix []byte // Prefix 指定排序的前缀字节，用于确定排序的范围。
	IsAsc  bool   // IsAsc 指定排序顺序，为 true 时按升序排列，为 false 时按降序排列。

	// 范围选项
	StartKey []byte // StartKey 指定迭代的起始键（包含）
	EndKey   []byte // EndKey 指定迭代的结束键（不包含）

	// 性能选项
	PrefetchSize int  // PrefetchSize 指定预读的条目数量，0表示使用默认值
	ReadAhead    bool // ReadAhead 是否启用预读优化

	// 过滤选项
	SkipEmpty bool // SkipEmpty 是否跳过空值条目
}

// DefaultOptions 返回默认的迭代器选项
func DefaultOptions() *Options {
	return &Options{
		IsAsc:        true,
		PrefetchSize: 100, // 默认预读100个条目
		ReadAhead:    true,
		SkipEmpty:    false,
	}
}

// NewOptions 创建新的迭代器选项，基于默认选项进行修改
func NewOptions() *Options {
	return DefaultOptions()
}

// WithPrefix 设置前缀过滤
func (o *Options) WithPrefix(prefix []byte) *Options {
	if prefix == nil {
		o.Prefix = nil
		return o
	}
	o.Prefix = make([]byte, len(prefix))
	copy(o.Prefix, prefix)
	return o
}

// WithRange 设置键范围
func (o *Options) WithRange(startKey, endKey []byte) *Options {
	if len(startKey) > 0 {
		o.StartKey = make([]byte, len(startKey))
		copy(o.StartKey, startKey)
	}
	if len(endKey) > 0 {
		o.EndKey = make([]byte, len(endKey))
		copy(o.EndKey, endKey)
	}
	return o
}

// WithDescending 设置降序排列
func (o *Options) WithDescending() *Options {
	o.IsAsc = false
	return o
}

// WithPrefetchSize 设置预读大小
func (o *Options) WithPrefetchSize(size int) *Options {
	o.PrefetchSize = size
	return o
}

// TableIterator 表示SSTable的迭代器接口
// 用于遍历SSTable中的键值对数据，并提供键范围信息
type TableIterator interface {
	Iterator

	// GetMinKey 获取SSTable中的最小键
	GetMinKey() []byte

	// GetMaxKey 获取SSTable中的最大键
	GetMaxKey() []byte
}

// NewTableIterator 创建一个新的SSTable迭代器
// 这个函数的具体实现在file包中提供
// 参数：
//   - sstable: SSTable实例（具体类型由实现决定）
//   - options: 迭代器选项
//
// 返回：
//   - TableIterator: 新创建的迭代器
//   - error: 如果创建失败则返回错误
//
// 注意：这个函数的实际实现通过init()函数在file包中注册
var NewTableIterator func(sstable interface{}, options *Options) (TableIterator, error)

func init() {
	// 默认实现，如果没有注册具体实现则返回错误
	if NewTableIterator == nil {
		NewTableIterator = func(sstable interface{}, options *Options) (TableIterator, error) {
			return nil, ErrInvalidRequest
		}
	}
}
