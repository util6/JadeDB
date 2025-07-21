package file

import (
	"encoding/binary"
	"fmt"
	"github.com/pkg/errors"
	"github.com/util6/JadeDB/utils/mmap"

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
	if sz > 0 && fileSize == 0 {
		// 如果文件为空，将其截断为指定大小
		if err := fd.Truncate(int64(sz)); err != nil {
			return nil, errors.Wrapf(err, "截断文件时出错")
		}
		fileSize = int64(sz)
	}
	buf, err := mmap.Mmap(fd, writable, fileSize)
	if err != nil {
		return nil, errors.Wrapf(err, "内存映射文件 %s 大小: %d 时出错", fd.Name(), fileSize)
	}

	if fileSize == 0 {
		dir, _ := filepath.Split(filename) // 分离目录路径
		go SyncDir(dir)                    // 同步目录
	}
	return &MmapFile{
		Data: buf,
		Fd:   fd,
	}, nil
}

func OpenMmapFile(fileName string, flag int, maxSize int) (*MmapFile, error) {
	fd, err := os.OpenFile(fileName, flag, 0666)
	if err != nil {
		return nil, errors.Errorf("无法打开文件, %s", fileName)
	}
	writable := true
	if flag == os.O_RDONLY {
		writable = false
	}
	if fileInfo, err := fd.Stat(); err == nil && fileInfo != nil && fileInfo.Size() > 0 {
		maxSize = int(fileInfo.Size())
	}
	return CreateMmapFile(fd, maxSize, writable)
}

func (m *MmapFile) AllocateSlice(sz, offset int) ([]byte, int, error) {
	start := offset + 4

	// If the file is too small, double its size or increase it by 1GB, whichever is smaller.
	if start+sz > len(m.Data) {

		growBy := len(m.Data)
		if growBy > maxCapacity {
			growBy = maxCapacity
		}
		if growBy < sz+4 {
			growBy = sz + 4
		}
		if err := m.Truncature(int64(len(m.Data) + growBy)); err != nil {
			return nil, 0, err
		}
	}

	binary.BigEndian.PutUint32(m.Data[offset:], uint32(sz))
	return m.Data[start : start+sz], start + sz, nil
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

func (m *MmapFile) Sync() error {
	if m == nil {
		return nil
	}
	return mmap.Msync(m.Data)
}

func (m *MmapFile) Delete() error {
	if m.Fd == nil {
		return nil
	}

	if err := mmap.Munmap(m.Data); err != nil {
		return fmt.Errorf("while munmap file: %s, error: %v\n", m.Fd.Name(), err)
	}
	m.Data = nil
	if err := m.Fd.Truncate(0); err != nil {
		return fmt.Errorf("while truncate file: %s, error: %v\n", m.Fd.Name(), err)
	}
	if err := m.Fd.Close(); err != nil {
		return fmt.Errorf("while close file: %s, error: %v\n", m.Fd.Name(), err)
	}
	return os.Remove(m.Fd.Name())
}

// Close would close the file. It would also truncate the file if maxSz >= 0.
func (m *MmapFile) Close() error {
	if m.Fd == nil {
		return nil
	}
	if err := m.Sync(); err != nil {
		return fmt.Errorf("while sync file: %s, error: %v\n", m.Fd.Name(), err)
	}
	if err := mmap.Munmap(m.Data); err != nil {
		return fmt.Errorf("while munmap file: %s, error: %v\n", m.Fd.Name(), err)
	}
	return m.Fd.Close()
}

func SyncDir(dir string) error {
	df, err := os.Open(dir)
	if err != nil {
		return errors.Wrapf(err, "while opening %s", dir)
	}
	if err := df.Sync(); err != nil {
		return errors.Wrapf(err, "while syncing %s", dir)
	}
	if err := df.Close(); err != nil {
		return errors.Wrapf(err, "while closing %s", dir)
	}
	return nil
}

// Truncature 兼容接口
func (m *MmapFile) Truncature(maxSz int64) error {
	if err := m.Sync(); err != nil {
		return fmt.Errorf("while sync file: %s, error: %v\n", m.Fd.Name(), err)
	}
	if err := m.Fd.Truncate(maxSz); err != nil {
		return fmt.Errorf("while truncate file: %s, error: %v\n", m.Fd.Name(), err)
	}

	var err error
	m.Data, err = mmap.Mremap(m.Data, int(maxSz)) // Mmap up to max size.
	return err
}

// ReName 兼容接口
func (m *MmapFile) ReName(name string) error {
	return nil
}
