package file

import (
	"os"
	"sync"
)

// LogFile 表示一个日志文件
type LogFile struct {
	lock        sync.RWMutex
	f           *MmapFile
	fid         uint64
	FID         uint64 // 公开的 FID 字段
	size        uint32
	loadingMode int
}

// NewLogFile 创建一个新的日志文件
func NewLogFile(path string, fid uint64, loadingMode int) (*LogFile, error) {
	lf := &LogFile{
		fid:         fid,
		FID:         fid,
		loadingMode: loadingMode,
	}

	var err error
	lf.f, err = OpenMmapFile(path, os.O_CREATE|os.O_RDWR, 0)
	if err != nil {
		return nil, err
	}

	return lf, nil
}

// FD 返回文件描述符
func (lf *LogFile) FD() *os.File {
	return lf.f.Fd
}

// Close 关闭日志文件
func (lf *LogFile) Close() error {
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
	// 简单实现，返回空字节
	return []byte{}, nil
}

// Open 打开日志文件
func (lf *LogFile) Open(opt *Options) error {
	// 文件已经在 NewLogFile 中打开了
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
	// 简单实现，返回固定长度
	return 0, nil
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
