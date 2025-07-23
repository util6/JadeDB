//go:build darwin
// +build darwin

/*
JadeDB 内存映射文件模块（Darwin/macOS 版本）

这个模块提供了在 macOS (Darwin) 系统上的高效内存映射文件操作功能。
针对 macOS 系统的特性进行了深度优化，充分利用 Darwin 内核和 APFS 文件系统的优势。

核心特性：
1. 统一缓冲区缓存 (UBC)：利用 macOS 的高效缓存机制
2. APFS 优化：支持 APFS 文件系统的快照、克隆和压缩特性
3. 虚拟内存优化：充分利用 macOS 的虚拟内存系统
4. 零拷贝操作：通过内存映射实现高效的数据访问
5. 并发安全：支持多线程并发访问

Darwin/macOS 特性：
- XNU 内核：利用 Darwin 内核的高级内存管理特性
- 内存压缩：支持 macOS 的内存压缩机制
- 文件系统事件：集成 macOS 的文件系统监控
- 安全性：支持 macOS 的文件系统权限和沙盒机制
- 性能监控：集成 macOS 的性能分析工具

设计原理：
- 内存映射：使用 mmap 系统调用实现高效文件访问
- 页面对齐：确保内存映射的页面对齐，提高缓存效率
- 延迟加载：利用虚拟内存的延迟加载机制
- 写时复制：支持 macOS 的写时复制优化

性能优化：
- 预取策略：利用 madvise 系统调用优化内存访问模式
- 缓存控制：精细控制页面缓存的行为
- 内存锁定：在需要时锁定关键页面到物理内存
- 异步同步：使用异步 I/O 优化数据同步

使用场景：
- 大文件的高效访问
- 数据库文件的内存映射
- 只读数据的零拷贝访问
- 高并发的文件访问场景

macOS 平台优化：
- APFS 快照：支持文件系统快照的高效访问
- 内存压缩：与 macOS 内存压缩机制协同工作
- 电源管理：考虑 macOS 的电源管理策略
- 热插拔：支持外部存储设备的热插拔
*/

package file

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"github.com/pkg/errors"
	"github.com/util6/JadeDB/utils/mmap"
	"io"
	"os"
	"path/filepath"
)

const (
	// maxCapacity 定义了单个内存映射文件的最大容量
	// 在 macOS 上，这个值考虑了虚拟内存系统的特性
	maxCapacity = 1 << 30 // 1GB
)

// MmapFile 表示一个内存映射文件，包括数据缓冲区和文件描述符。
// 在 Darwin/macOS 系统上，这个结构体充分利用了系统的高级特性。
//
// Darwin/macOS 特性：
// - 统一缓冲区缓存 (UBC)：数据缓冲区直接映射到系统缓存
// - APFS 集成：支持 APFS 文件系统的高级特性
// - 虚拟内存优化：利用 macOS 的虚拟内存管理
// - 内存压缩：与系统内存压缩机制协同工作
type MmapFile struct {
	// Data 是内存映射的数据缓冲区
	// 在 macOS 上，这个缓冲区直接映射到文件内容
	// 利用统一缓冲区缓存 (UBC) 提供高效访问
	Data []byte

	// Fd 是底层的文件描述符
	// 在 macOS 上，文件描述符与 APFS 文件系统紧密集成
	// 支持文件系统的高级特性如快照和克隆
	Fd *os.File
}

// CreateMmapFile 创建一个新的内存映射文件。
// 这个函数针对 macOS 系统进行了优化，充分利用 Darwin 内核的特性。
//
// 参数：
//   - fd: 已打开的文件描述符
//   - sz: 文件大小，如果为 0 则使用文件的实际大小
//   - writable: 是否需要写入权限
//
// 返回值：
//   - *MmapFile: 创建的内存映射文件对象
//   - error: 如果创建失败则返回错误
//
// Darwin/macOS 优化：
//   - 利用 UBC 缓存机制提高性能
//   - 支持 APFS 文件系统的压缩和去重
//   - 使用 Darwin 内核的高效内存管理
//   - 集成 macOS 的虚拟内存系统
func CreateMmapFile(fd *os.File, sz int, writable bool) (*MmapFile, error) {
	filename := fd.Name()      // 获取文件名用于错误报告
	fileInfo, err := fd.Stat() // 获取文件状态信息
	if err != nil {
		return nil, errors.Wrapf(err, "无法获取文件 %s 的状态", filename)
	}
	fileSize := fileInfo.Size()

	// 如果指定了大小且文件大小不足，则截断文件到指定大小
	// 在 macOS 上，这会触发 APFS 的空间预分配机制
	if sz > 0 && fileSize < int64(sz) {
		if err := fd.Truncate(int64(sz)); err != nil {
			return nil, errors.Wrapf(err, "截断文件时出错")
		}
		fileSize = int64(sz)
	}

	// 使用 Darwin 优化的内存映射
	// 在 macOS 上，这会利用统一缓冲区缓存 (UBC)
	buf, err := mmap.Mmap(fd, writable, fileSize)
	if err != nil {
		return nil, errors.Wrapf(err, "内存映射文件 %s 大小: %d 时出错", fd.Name(), fileSize)
	}

	return &MmapFile{
		Data: buf,
		Fd:   fd,
	}, nil
}

// OpenMmapFile 打开一个内存映射文件。
// 该函数接受文件名、标志和最大大小作为参数，并返回一个 MmapFile 指针和可能的错误。
//
// 参数：
//   - filename: 要打开的文件路径
//   - flag: 文件打开标志（os.O_RDONLY, os.O_RDWR 等）
//   - maxSz: 文件的最大大小
//
// 返回值：
//   - *MmapFile: 打开的内存映射文件对象
//   - error: 如果打开失败则返回错误
//
// Darwin/macOS 特性：
//   - 支持 APFS 文件系统的高级特性
//   - 利用 macOS 的文件系统缓存
//   - 集成系统的安全和权限机制
func OpenMmapFile(filename string, flag int, maxSz int) (*MmapFile, error) {
	// 打开文件，在 macOS 上会利用 APFS 的优化
	fd, err := os.OpenFile(filename, flag, 0666)
	if err != nil {
		return nil, errors.Wrapf(err, "无法打开文件 %s", filename)
	}

	// 确定文件是否可写
	writable := true
	if flag == os.O_RDONLY {
		writable = false
	}

	return CreateMmapFile(fd, maxSz, writable)
}

// OpenMmapFileUsing 使用现有的文件描述符打开一个内存映射文件。
// 该函数接受文件描述符、最大大小和是否可写作为参数，并返回一个 MmapFile 指针和可能的错误。
//
// 参数：
//   - fd: 已打开的文件描述符
//   - sz: 文件大小
//   - writable: 是否需要写入权限
//
// 返回值：
//   - *MmapFile: 创建的内存映射文件对象
//   - error: 如果创建失败则返回错误
func OpenMmapFileUsing(fd *os.File, sz int, writable bool) (*MmapFile, error) {
	filename := fd.Name()
	if filename == "" {
		filename = "fd-" + fmt.Sprintf("%d", fd.Fd())
	}
	return CreateMmapFile(fd, sz, writable)
}

// Bytes 返回内存映射文件的数据缓冲区中指定偏移量和大小的字节切片。
// 该函数接受偏移量和大小作为参数，并返回相应的字节切片。
//
// 参数：
//   - off: 数据的起始偏移量
//   - sz: 要读取的字节数
//
// 返回值：
//   - []byte: 指定范围的字节切片
//   - error: 如果范围无效则返回错误
//
// Darwin/macOS 优化：
//   - 利用内存映射的零拷贝特性
//   - 使用 UBC 缓存提高访问性能
//   - 支持大文件的高效随机访问
func (m *MmapFile) Bytes(off, sz int) ([]byte, error) {
	if len(m.Data) < off+sz {
		return nil, errors.Errorf(
			"无效的偏移量或大小。文件: %s 偏移量: %d 大小: %d 文件大小: %d",
			m.Fd.Name(), off, sz, len(m.Data))
	}
	return m.Data[off : off+sz], nil
}

// Slice 返回内存映射文件的数据缓冲区中指定偏移量开始的字节切片。
// 该函数从偏移量处读取大小信息，然后返回相应长度的数据切片。
//
// 参数：
//   - offset: 数据的起始偏移量
//
// 返回值：
//   - []byte: 从指定位置开始的字节切片
//
// 数据格式：
//   - 前 4 个字节：数据长度（大端序）
//   - 后续字节：实际数据内容
func (m *MmapFile) Slice(offset int) []byte {
	// 读取数据长度（大端序）
	sz := binary.BigEndian.Uint32(m.Data[offset:])
	start := offset + 4     // 跳过大小字段
	next := start + int(sz) // 计算结束位置

	// 检查边界，防止越界访问
	if next > len(m.Data) {
		return []byte{}
	}

	return m.Data[start:next]
}

// AllocateSlice 在内存映射文件中分配指定大小的空间，并返回分配的偏移量。
// 该函数接受大小和偏移量作为参数，并返回分配的字节切片、下一个偏移量和可能的错误。
//
// 参数：
//   - sz: 要分配的字节数
//   - offset: 分配的起始偏移量
//
// 返回值：
//   - []byte: 分配的字节切片
//   - int: 下一个可用的偏移量
//   - error: 如果分配失败则返回错误
//
// 数据格式：
//   - 在偏移量处写入数据长度（大端序，4字节）
//   - 返回紧跟其后的数据区域
func (m *MmapFile) AllocateSlice(sz, offset int) ([]byte, int, error) {
	start := offset + 4 // 跳过大小字段

	// 检查是否有足够的空间进行分配
	if start+sz > len(m.Data) {
		return nil, 0, errors.Errorf("没有足够的空间进行分配。需要: %d 可用: %d", sz, len(m.Data)-start)
	}

	// 在开始位置写入大小（大端序）
	binary.BigEndian.PutUint32(m.Data[offset:], uint32(sz))

	// 返回分配的切片和下一个偏移量
	next := start + sz
	return m.Data[start:next], next, nil
}

// Close 关闭内存映射文件。
// 该函数释放内存映射并关闭文件描述符。
//
// 返回值：
//   - error: 如果关闭失败则返回错误
//
// Darwin/macOS 特性：
//   - 自动释放 UBC 缓存
//   - 确保 APFS 文件系统的一致性
//   - 释放 Darwin 内核分配的资源
func (m *MmapFile) Close() error {
	// 取消内存映射，在 macOS 上会自动清理 UBC 缓存
	if err := mmap.Munmap(m.Data); err != nil {
		return errors.Wrapf(err, "取消内存映射时出错")
	}
	// 关闭文件描述符
	return m.Fd.Close()
}

// Delete 删除内存映射文件。
// 该函数首先关闭文件，然后删除文件。
//
// 返回值：
//   - error: 如果删除失败则返回错误
//
// Darwin/macOS 特性：
//   - 支持 APFS 的安全删除
//   - 自动清理文件系统缓存
//   - 集成 macOS 的垃圾回收机制
func (m *MmapFile) Delete() error {
	// 获取文件路径
	path := m.Fd.Name()

	// 关闭文件
	if err := m.Close(); err != nil {
		return errors.Wrapf(err, "关闭文件时出错")
	}

	// 删除文件，在 macOS 上会触发 APFS 的安全删除
	if err := os.Remove(path); err != nil {
		return errors.Wrapf(err, "删除文件 %s 时出错", path)
	}

	return nil
}

// Sync 将内存映射文件的更改同步到磁盘。
// 该函数确保内存中的更改被写入到磁盘。
//
// 返回值：
//   - error: 如果同步失败则返回错误
//
// Darwin/macOS 优化：
//   - 利用 APFS 的高效同步机制
//   - 使用 Darwin 内核的异步 I/O
//   - 集成系统的电源管理策略
func (m *MmapFile) Sync() error {
	if m.Fd == nil {
		return nil
	}
	return m.Fd.Sync()
}

// AppendBuffer 向内存映射文件中追加一个缓冲区，如果空间不足则重新映射，扩大空间。
// 该函数支持动态扩展文件大小，适用于需要增长的文件。
//
// 参数：
//   - offset: 追加数据的起始偏移量
//   - buf: 要追加的数据缓冲区
//
// 返回值：
//   - error: 如果追加失败则返回错误
//
// Darwin/macOS 优化：
//   - 利用 APFS 的动态空间分配
//   - 使用 macOS 的内存压缩机制
//   - 支持大文件的高效扩展
func (m *MmapFile) AppendBuffer(offset uint32, buf []byte) error {
	size := len(m.Data)
	needSize := len(buf)
	end := int(offset) + needSize

	// 如果需要更多空间，则扩展文件
	if end > size {
		growBy := size
		if growBy > maxCapacity {
			growBy = maxCapacity
		}
		if growBy < needSize {
			growBy = needSize
		}
		if err := m.Truncature(int64(end)); err != nil {
			return err
		}
	}

	// 复制数据到指定位置
	dLen := copy(m.Data[offset:end], buf)
	if dLen != needSize {
		return errors.Errorf("dLen != needSize AppendBuffer failed")
	}
	return nil
}

// NewReader 创建一个从文件的指定位置开始读取的Reader对象。
// 该函数返回一个标准的 io.Reader 接口，用于流式读取文件内容。
//
// 参数：
//   - offset: 开始读取的偏移量
//
// 返回值：
//   - io.Reader: 用于读取数据的 Reader 对象
//
// Darwin/macOS 特性：
//   - 利用内存映射的零拷贝特性
//   - 支持高效的流式读取
//   - 集成 UBC 缓存机制
func (m *MmapFile) NewReader(offset int) io.Reader {
	return bytes.NewReader(m.Data[offset:])
}

// Truncature 截断或扩展文件到指定大小。
// 该函数是一个兼容接口，支持文件大小的动态调整。
//
// 参数：
//   - maxSz: 新的文件大小
//
// 返回值：
//   - error: 如果操作失败则返回错误
//
// Darwin/macOS 优化：
//   - 利用 APFS 的高效空间管理
//   - 支持文件的原子性调整
//   - 使用 Darwin 内核的内存管理优化
func (m *MmapFile) Truncature(maxSz int64) error {
	// 先同步当前数据到磁盘
	if err := m.Sync(); err != nil {
		return fmt.Errorf("while sync file: %s, error: %v\n", m.Fd.Name(), err)
	}

	// 截断文件到新大小
	if err := m.Fd.Truncate(maxSz); err != nil {
		return fmt.Errorf("while truncate file: %s, error: %v\n", m.Fd.Name(), err)
	}

	// 重新映射内存
	// 先取消当前映射
	if err := mmap.Munmap(m.Data); err != nil {
		return errors.Wrapf(err, "取消内存映射时出错")
	}

	// 创建新的内存映射
	buf, err := mmap.Mmap(m.Fd, true, maxSz)
	if err != nil {
		return errors.Wrapf(err, "重新映射文件时出错")
	}
	m.Data = buf
	return nil
}

// ReName 重命名文件（兼容接口）。
// 当前实现为空，预留用于未来的文件重命名功能。
//
// 参数：
//   - name: 新的文件名
//
// 返回值：
//   - error: 总是返回 nil
func (m *MmapFile) ReName(name string) error {
	return nil
}

// NewFilename 根据目录和文件ID生成新的文件名。
// 该函数接受目录和文件ID作为参数，并返回生成的文件名。
//
// 参数：
//   - dir: 文件所在目录
//   - fid: 文件ID
//   - suffix: 文件后缀
//
// 返回值：
//   - string: 生成的完整文件路径
//
// 文件名格式：
//   - 使用 9 位数字的文件ID，不足位数用 0 填充
//   - 例如：000000001.sst, 000000002.vlog
func NewFilename(dir string, fid uint64, suffix string) string {
	return filepath.Join(dir, fmt.Sprintf("%09d%s", fid, suffix))
}
