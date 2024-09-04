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

// Options 定义了排序选项的结构。
// TODO: 此结构体可能需要重构以优化设计。
type Options struct {
	Prefix []byte // Prefix 指定排序的前缀字节，用于确定排序的范围。
	IsAsc  bool   // IsAsc 指定排序顺序，为 true 时按升序排列，为 false 时按降序排列。
}
