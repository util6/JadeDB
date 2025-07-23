//go:build !windows && !darwin
// +build !windows,!darwin

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
	maxCapacity = 1 << 30
)

// MmapFile 表示一个内存映射文件，包括数据缓冲区和文件描述符。
type MmapFile struct {
	Data []byte   // 数据缓冲区
	Fd   *os.File // 文件描述符
}

func CreateMmapFile(fd *os.File, sz int, writable bool) (*MmapFile, error) {
	filename := fd.Name()      // 获取文件名
	fileInfo, err := fd.Stat() // 获取文件状态
	if err != nil {
		return nil, errors.Wrapf(err, "无法获取文件 %s 的状态", filename)
	}
	fileSize := fileInfo.Size()
	if sz > 0 && fileSize < int64(sz) {
		// 如果文件大小不足，将其截断为指定大小
		if err := fd.Truncate(int64(sz)); err != nil {
			return nil, errors.Wrapf(err, "截断文件时出错")
		}
		fileSize = int64(sz)
	}
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
func OpenMmapFile(filename string, flag int, maxSz int) (*MmapFile, error) {
	// 打开文件
	fd, err := os.OpenFile(filename, flag, 0666)
	if err != nil {
		return nil, errors.Wrapf(err, "无法打开文件 %s", filename)
	}
	writable := true
	if flag == os.O_RDONLY {
		writable = false
	}
	return CreateMmapFile(fd, maxSz, writable)
}

// OpenMmapFileUsing 使用现有的文件描述符打开一个内存映射文件。
// 该函数接受文件描述符、最大大小和是否可写作为参数，并返回一个 MmapFile 指针和可能的错误。
func OpenMmapFileUsing(fd *os.File, sz int, writable bool) (*MmapFile, error) {
	filename := fd.Name()
	if filename == "" {
		filename = "fd-" + fmt.Sprintf("%d", fd.Fd())
	}
	return CreateMmapFile(fd, sz, writable)
}

// Bytes 返回内存映射文件的数据缓冲区中指定偏移量和大小的字节切片。
// 该函数接受偏移量和大小作为参数，并返回相应的字节切片。
func (m *MmapFile) Bytes(off, sz int) ([]byte, error) {
	if len(m.Data) < off+sz {
		return nil, errors.Errorf(
			"无效的偏移量或大小。文件: %s 偏移量: %d 大小: %d 文件大小: %d",
			m.Fd.Name(), off, sz, len(m.Data))
	}
	return m.Data[off : off+sz], nil
}

// Slice 返回内存映射文件的数据缓冲区中指定偏移量和大小的字节切片。
// 该函数接受偏移量和大小作为参数，并返回相应的字节切片。
func (m *MmapFile) Slice(offset int) []byte {
	sz := binary.BigEndian.Uint32(m.Data[offset:])
	start := offset + 4     // 跳过大小字段
	next := start + int(sz) // 计算下一个位置
	if next > len(m.Data) { // 检查边界
		return []byte{}
	}
	res := m.Data[start:next]
	return res
}

// AllocateSlice 在内存映射文件中分配指定大小的空间，并返回分配的偏移量。
// 该函数接受大小作为参数，并返回分配的偏移量和可能的错误。
func (m *MmapFile) AllocateSlice(sz, offset int) ([]byte, int, error) {
	start := offset + 4 // 跳过大小字段

	// 检查是否有足够的空间
	if start+sz > len(m.Data) {
		return nil, 0, errors.Errorf("没有足够的空间进行分配。需要: %d 可用: %d", sz, len(m.Data)-start)
	}

	// 在开始位置写入大小
	binary.BigEndian.PutUint32(m.Data[offset:], uint32(sz))

	// 返回分配的切片和下一个偏移量
	next := start + sz
	return m.Data[start:next], next, nil
}

// Close 关闭内存映射文件。
// 该函数释放内存映射并关闭文件描述符。
func (m *MmapFile) Close() error {
	// 取消内存映射
	if err := mmap.Munmap(m.Data); err != nil {
		return errors.Wrapf(err, "取消内存映射时出错")
	}
	// 关闭文件描述符
	return m.Fd.Close()
}

// Delete 删除内存映射文件。
// 该函数首先关闭文件，然后删除文件。
func (m *MmapFile) Delete() error {
	// 获取文件路径
	path := m.Fd.Name()

	// 关闭文件
	if err := m.Close(); err != nil {
		return errors.Wrapf(err, "关闭文件时出错")
	}

	// 删除文件
	if err := os.Remove(path); err != nil {
		return errors.Wrapf(err, "删除文件 %s 时出错", path)
	}

	return nil
}

// Sync 将内存映射文件的更改同步到磁盘。
// 该函数确保内存中的更改被写入到磁盘。
func (m *MmapFile) Sync() error {
	if m.Fd == nil {
		return nil
	}
	return m.Fd.Sync()
}

// AppendBuffer 向内存中追加一个buffer，如果空间不足则重新映射，扩大空间
func (m *MmapFile) AppendBuffer(offset uint32, buf []byte) error {
	size := len(m.Data)
	needSize := len(buf)
	end := int(offset) + needSize
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
	dLen := copy(m.Data[offset:end], buf)
	if dLen != needSize {
		return errors.Errorf("dLen != needSize AppendBuffer failed")
	}
	return nil
}

// NewReader 创建一个从文件的指定位置开始读取的Reader对象
func (m *MmapFile) NewReader(offset int) io.Reader {
	return bytes.NewReader(m.Data[offset:])
}

// Truncature 兼容接口
func (m *MmapFile) Truncature(maxSz int64) error {
	if err := m.Sync(); err != nil {
		return fmt.Errorf("while sync file: %s, error: %v\n", m.Fd.Name(), err)
	}
	if err := m.Fd.Truncate(maxSz); err != nil {
		return fmt.Errorf("while truncate file: %s, error: %v\n", m.Fd.Name(), err)
	}

	// 重新映射内存
	if err := mmap.Munmap(m.Data); err != nil {
		return errors.Wrapf(err, "取消内存映射时出错")
	}

	buf, err := mmap.Mmap(m.Fd, true, maxSz)
	if err != nil {
		return errors.Wrapf(err, "重新映射文件时出错")
	}
	m.Data = buf
	return nil
}

// ReName 兼容接口
func (m *MmapFile) ReName(name string) error {
	return nil
}

// NewFilename 根据目录和文件ID生成新的文件名。
// 该函数接受目录和文件ID作为参数，并返回生成的文件名。
func NewFilename(dir string, fid uint64, suffix string) string {
	return filepath.Join(dir, fmt.Sprintf("%09d%s", fid, suffix))
}
