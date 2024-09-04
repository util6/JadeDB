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

package file

import "io"

// Options 结构体定义了文件操作的一组选项参数。
// 这些选项参数用于控制文件的各种操作，如创建、读取和写入等。
type Options struct {
	// FID 用于唯一标识文件。
	FID uint64
	// FileName 指定了文件的名称。
	FileName string
	// Dir 表示文件所在的目录。
	Dir string
	// Path 为文件的完整路径，用于精确定位文件。
	Path string
	// Flag 代表文件的打开模式，如只读、只写或读写等。
	Flag int
	// MaxSz 指明了文件的最大大小。
	MaxSz int
}

// CoreFile 定义了文件操作的一组接口，包括关闭文件、截断文件、重命名文件等操作。
type CoreFile interface {
	// Close 关闭文件。
	// 返回值：如果关闭过程中发生错误，返回错误信息。
	Close() error

	// Truncature 截断文件到指定长度。
	// 参数 n：指定文件的新长度。
	// 返回值：如果截断操作失败，返回错误信息。
	Truncature(n int64) error

	// ReName 重命名文件。
	// 参数 name：新的文件名。
	// 返回值：如果重命名操作失败，返回错误信息。
	ReName(name string) error

	// NewReader 创建一个从文件的指定位置开始读取的Reader对象。
	// 参数 offset：Reader对象开始读取的起始位置。
	// 返回值：返回一个io.Reader接口，用于从文件中读取数据。
	NewReader(offset int) io.Reader

	// Bytes 从文件中读取指定长度的字节。
	// 参数 off：读取的起始位置。
	// 参数 sz：要读取的字节数。
	// 返回值：读取到的字节数组和可能的错误信息。
	Bytes(off, sz int) ([]byte, error)

	// AllocateSlice 为文件中的指定区域分配一个字节数组。
	// 参数 sz：分配的字节数组的大小。
	// 参数 offset：分配的起始位置。
	// 返回值：分配到的字节数组、实际分配的大小和可能的错误信息。
	AllocateSlice(sz, offset int) ([]byte, int, error)

	// Sync 将文件内容刷入磁盘。
	// 返回值：如果同步操作失败，返回错误信息。
	Sync() error

	// Delete 删除文件。
	// 返回值：如果删除操作失败，返回错误信息。
	Delete() error

	// Slice 获取文件中指定偏移量开始的字节数组。
	// 参数 offset：获取字节数组的起始位置。
	// 返回值：返回从指定位置开始的字节数组。
	Slice(offset int) []byte
}
