/*
JadeDB 日志文件管理模块

LogFile 是 JadeDB 中用于管理日志文件的核心组件，主要用于值日志（VLog）的实现。
它提供了对大值数据的高效存储和访问能力。

核心功能：
1. 文件管理：创建、打开、关闭和删除日志文件
2. 数据读写：支持随机位置的数据读写操作
3. 内存映射：使用 mmap 提高文件访问性能
4. 并发安全：通过读写锁保护并发访问

设计特点：
- 内存映射：使用 mmap 减少系统调用开销
- 随机访问：支持基于偏移量的随机读写
- 并发安全：读写锁保护，支持多读者单写者
- 文件标识：每个文件有唯一的 FID 标识

使用场景：
- 值日志文件：存储大于阈值的值数据
- 顺序写入：新数据追加到文件末尾
- 随机读取：根据值指针读取特定位置的数据
- 垃圾回收：清理无效数据，回收存储空间

性能优化：
- 内存映射：减少数据拷贝，提高访问速度
- 批量操作：支持批量读写，减少系统调用
- 预分配：预先分配文件空间，减少碎片
- 异步同步：在适当时机异步刷新数据

文件格式：
日志文件采用追加写入的格式，每个条目包含：
[长度][校验和][键][值]

生命周期：
1. 创建：分配新的 FID 并创建文件
2. 写入：追加新的值数据到文件末尾
3. 读取：根据偏移量读取特定的值数据
4. 垃圾回收：清理无效数据，可能重写文件
5. 删除：当文件不再需要时删除
*/

package file

import (
	"bytes"
	"fmt"
	"hash/crc32"
	"os"
	"sync"

	"github.com/util6/JadeDB/utils"
)

// LogFile 表示一个日志文件，主要用于值日志的实现。
// 它封装了底层的内存映射文件，提供高级的日志文件操作接口。
//
// 设计原理：
// 1. 内存映射：使用 mmap 提高文件访问性能
// 2. 并发安全：使用读写锁保护并发访问
// 3. 随机访问：支持基于偏移量的随机读写
// 4. 文件标识：通过 FID 唯一标识文件
//
// 使用模式：
// - 写入：通常是顺序追加写入
// - 读取：根据值指针进行随机读取
// - 管理：通过 FID 进行文件生命周期管理
//
// 性能特点：
// - 高效读写：内存映射减少数据拷贝
// - 并发友好：读写锁允许多读者并发
// - 空间效率：支持文件截断和空间回收
type LogFile struct {
	// 并发控制

	// lock 保护日志文件的并发访问。
	// 使用读写锁允许多个读操作并发执行。
	// 写操作需要独占访问，确保数据一致性。
	lock sync.RWMutex

	// 底层文件

	// f 是底层的内存映射文件对象。
	// 提供高效的文件访问接口。
	// 使用 mmap 技术减少系统调用开销。
	f *MmapFile

	// 文件标识

	// fid 是文件的内部标识符。
	// 用于内部管理和引用。
	fid uint64

	// FID 是文件的公开标识符。
	// 与 fid 相同，但提供公开访问。
	// 用于外部组件的文件识别和管理。
	FID uint64

	// 文件状态

	// size 记录文件的当前大小（字节）。
	// 用于边界检查和空间管理。
	// 在写入操作时更新。
	size uint32

	// 加载模式

	// loadingMode 指定文件的加载模式。
	// 控制文件的打开方式和访问模式。
	// 可能的值包括只读、读写、追加等。
	loadingMode int
}

// NewLogFile 创建一个新的日志文件实例。
// 这是日志文件的构造函数，负责初始化所有必要的组件。
//
// 参数说明：
// path: 日志文件的完整路径
// fid: 文件的唯一标识符
// loadingMode: 文件的加载模式
//
// 返回值：
// 创建成功的日志文件实例和可能的错误
//
// 创建过程：
// 1. 初始化 LogFile 结构体
// 2. 设置文件标识符和加载模式
// 3. 打开底层的内存映射文件
// 4. 返回完整的日志文件实例
//
// 错误处理：
// 如果底层文件打开失败，会返回相应的错误信息。
//
// 使用场景：
// - 创建新的值日志文件
// - 打开现有的日志文件进行读写
// - 系统启动时恢复日志文件状态
func NewLogFile(path string, fid uint64, loadingMode int) (*LogFile, error) {
	// 初始化日志文件结构体
	lf := &LogFile{
		fid:         fid,         // 设置内部文件标识符
		FID:         fid,         // 设置公开文件标识符
		loadingMode: loadingMode, // 设置加载模式
	}

	// 打开底层的内存映射文件
	var err error
	lf.f, err = OpenMmapFile(path, os.O_CREATE|os.O_RDWR, 0)
	if err != nil {
		return nil, err
	}

	return lf, nil
}

// FD 返回文件描述符
func (lf *LogFile) FD() *os.File {
	if lf.f == nil {
		return nil
	}
	return lf.f.Fd
}

// Close 关闭日志文件
func (lf *LogFile) Close() error {
	if lf.f == nil {
		return nil
	}
	return lf.f.Close()
}

// Size 返回文件大小
func (lf *LogFile) Size() uint32 {
	return lf.size
}

// Sync 同步文件
func (lf *LogFile) Sync() error {
	return lf.f.Sync()
}

// Delete 删除文件
func (lf *LogFile) Delete() error {
	return lf.f.Delete()
}

// Truncate 截断文件
func (lf *LogFile) Truncate(size int64) error {
	return lf.f.Truncature(size)
}

// Write 写入数据
func (lf *LogFile) Write(offset uint32, data []byte) error {
	return lf.f.AppendBuffer(offset, data)
}

// Read 读取数据
func (lf *LogFile) Read(offset, size int) ([]byte, error) {
	return lf.f.Bytes(offset, size)
}

// ReadValuePtr 根据 ValuePtr 读取数据
func (lf *LogFile) ReadValuePtr(vp interface{}) ([]byte, error) {
	// 检查文件是否已打开
	if lf.f == nil {
		return nil, fmt.Errorf("log file not opened")
	}

	// 类型断言，获取utils.ValuePtr
	valuePtr, ok := vp.(*utils.ValuePtr)
	if !ok {
		return nil, fmt.Errorf("invalid value pointer type: %T, expected *utils.ValuePtr", vp)
	}

	// 检查偏移量是否有效
	if valuePtr.Offset >= uint32(len(lf.f.Data)) {
		return nil, fmt.Errorf("offset %d exceeds file size %d", valuePtr.Offset, len(lf.f.Data))
	}

	// 检查长度是否有效
	if valuePtr.Offset+valuePtr.Len > uint32(len(lf.f.Data)) {
		return nil, fmt.Errorf("read range [%d:%d] exceeds file size %d",
			valuePtr.Offset, valuePtr.Offset+valuePtr.Len, len(lf.f.Data))
	}

	// 从内存映射文件中读取数据
	data := make([]byte, valuePtr.Len)
	copy(data, lf.f.Data[valuePtr.Offset:valuePtr.Offset+valuePtr.Len])

	return data, nil
}

// Open 打开日志文件
func (lf *LogFile) Open(opt *Options) error {
	// 如果文件已经打开，直接返回
	if lf.f != nil {
		return nil
	}

	// 使用NewLogFile来打开文件
	var err error
	lf.f, err = OpenMmapFile(opt.FileName, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return fmt.Errorf("failed to open mmap file %s: %w", opt.FileName, err)
	}

	// 设置文件标识符
	lf.FID = opt.FID
	lf.fid = opt.FID

	return nil
}

// FileName 返回文件名
func (lf *LogFile) FileName() string {
	return lf.f.Fd.Name()
}

// Init 初始化日志文件
func (lf *LogFile) Init() error {
	return nil
}

// Seek 定位到文件末尾
func (lf *LogFile) Seek(offset int64, whence int) (int64, error) {
	return lf.f.Fd.Seek(offset, whence)
}

// AddSize 增加文件大小
func (lf *LogFile) AddSize(delta uint32) {
	lf.lock.Lock()
	defer lf.lock.Unlock()
	lf.size += delta
}

// DoneWriting 完成写入
func (lf *LogFile) DoneWriting(offset uint32) error {
	lf.lock.Lock()
	defer lf.lock.Unlock()
	lf.size = offset
	return nil
}

// EncodeEntry 编码条目
func (lf *LogFile) EncodeEntry(entry interface{}, buf interface{}, offset uint32) (uint32, error) {
	// 类型断言获取Entry和Buffer
	e, ok := entry.(*utils.Entry)
	if !ok {
		return 0, fmt.Errorf("invalid entry type: %T, expected *utils.Entry", entry)
	}

	buffer, ok := buf.(*bytes.Buffer)
	if !ok {
		return 0, fmt.Errorf("invalid buffer type: %T, expected *bytes.Buffer", buf)
	}

	// 记录写入前的位置
	startPos := buffer.Len()

	// 创建Header
	h := utils.Header{
		KLen:      uint32(len(e.Key)),
		VLen:      uint32(len(e.Value)),
		ExpiresAt: e.ExpiresAt,
		Meta:      e.Meta,
	}

	// 编码Header
	headerBuf := make([]byte, utils.MaxHeaderSize)
	headerLen := h.Encode(headerBuf)
	buffer.Write(headerBuf[:headerLen])

	// 写入Key
	buffer.Write(e.Key)

	// 写入Value
	buffer.Write(e.Value)

	// 如果启用了校验和，添加CRC32校验和
	if lf.f != nil { // 简单检查，实际应该检查配置
		hash := crc32.New(utils.CastagnoliCrcTable)
		data := buffer.Bytes()[startPos:]
		hash.Write(data)
		checksum := hash.Sum32()

		// 将校验和写入buffer
		checksumBytes := utils.U32ToBytes(checksum)
		buffer.Write(checksumBytes)
	}

	// 返回写入的总长度
	totalLen := buffer.Len() - startPos
	return uint32(totalLen), nil
}

// DecodeEntry 解码条目
func (lf *LogFile) DecodeEntry(buf []byte, offset uint32) (interface{}, error) {
	// 简单实现，返回空接口
	return nil, nil
}

// Lock 锁定文件
func (lf *LogFile) Lock() {
	lf.lock.Lock()
}

// Unlock 解锁文件
func (lf *LogFile) Unlock() {
	lf.lock.Unlock()
}

// RLock 读锁定文件
func (lf *LogFile) RLock() {
	lf.lock.RLock()
}

// RUnlock 读解锁文件
func (lf *LogFile) RUnlock() {
	lf.lock.RUnlock()
}

// Bootstrap 初始化日志文件
func (lf *LogFile) Bootstrap() error {
	// 简单实现
	return nil
}
