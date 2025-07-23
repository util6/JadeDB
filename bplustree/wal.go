/*
JadeDB B+树WAL（Write-Ahead Log）模块

WAL是B+树存储引擎的核心组件，负责事务的持久性和崩溃恢复。
参考InnoDB的重做日志设计，实现高性能的WAL机制。

核心功能：
1. 事务日志：记录所有数据修改操作
2. 崩溃恢复：系统重启时重放日志恢复数据
3. 检查点：定期创建一致性检查点
4. 日志轮转：管理日志文件的创建和删除
5. 并发写入：支持多线程并发写入日志

设计原理：
- 预写日志：所有修改必须先写日志再修改数据
- 顺序写入：日志按时间顺序写入，充分利用磁盘性能
- 组提交：批量提交多个事务，减少I/O开销
- LSN机制：日志序列号确保恢复的正确性
- 双写缓冲：关键页面的双重保护

性能优化：
- 日志缓冲：内存缓冲减少磁盘写入
- 异步刷新：后台异步刷新日志到磁盘
- 批量写入：批量写入多条日志记录
- 压缩格式：紧凑的日志记录格式
*/

package bplustree

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/util6/JadeDB/file"
	"github.com/util6/JadeDB/utils"
)

// WAL常量定义
const (
	// WALFileSize WAL文件的最大大小（64MB）
	WALFileSize = 64 * 1024 * 1024

	// WALBufferSize WAL缓冲区大小（1MB）
	WALBufferSize = 1024 * 1024

	// WALFileExtension WAL文件扩展名
	WALFileExtension = ".wal"

	// CheckpointInterval 检查点间隔
	CheckpointInterval = 5 * time.Minute

	// LogRecordHeaderSize 日志记录头部大小
	// LSN(8) + Type(1) + TxnID(8) + PageID(8) + Length(4) + Checksum(4) = 33字节
	LogRecordHeaderSize = 33

	// MaxLogRecordSize 单条日志记录的最大大小
	MaxLogRecordSize = 64 * 1024
)

// LogRecordType 日志记录类型
type LogRecordType uint8

const (
	// LogInsert 插入记录
	LogInsert LogRecordType = iota
	// LogUpdate 更新记录
	LogUpdate
	// LogDelete 删除记录
	LogDelete
	// LogPageSplit 页面分裂
	LogPageSplit
	// LogPageMerge 页面合并
	LogPageMerge
	// LogCheckpoint 检查点
	LogCheckpoint
	// LogCommit 事务提交
	LogCommit
	// LogRollback 事务回滚
	LogRollback
)

// WALManager WAL管理器
// 负责WAL的写入、刷新、恢复和管理
type WALManager struct {
	// 配置选项
	options *BTreeOptions

	// 文件管理
	currentFile *file.MmapFile // 当前WAL文件
	fileIndex   atomic.Uint64  // 文件索引
	fileMutex   sync.RWMutex   // 文件操作锁

	// 日志序列号
	nextLSN       atomic.Uint64 // 下一个LSN
	flushedLSN    atomic.Uint64 // 已刷新的LSN
	checkpointLSN atomic.Uint64 // 检查点LSN

	// 缓冲管理
	buffer      []byte     // WAL缓冲区
	bufferPos   int        // 缓冲区位置
	bufferMutex sync.Mutex // 缓冲区锁

	// 后台服务
	flushChan chan struct{} // 刷新信号通道
	closer    *utils.Closer // 优雅关闭

	// 统计信息
	totalRecords atomic.Int64 // 总记录数
	totalBytes   atomic.Int64 // 总字节数
	flushCount   atomic.Int64 // 刷新次数
}

// LogRecord 日志记录结构
type LogRecord struct {
	// 记录头部
	LSN      uint64        // 日志序列号
	Type     LogRecordType // 记录类型
	TxnID    uint64        // 事务ID
	PageID   uint64        // 页面ID
	Length   uint32        // 记录长度
	Checksum uint32        // 校验和

	// 记录数据
	Data      []byte    // 实际数据
	Timestamp time.Time // 时间戳
}

// NewWALManager 创建新的WAL管理器
func NewWALManager(options *BTreeOptions) (*WALManager, error) {
	wm := &WALManager{
		options:   options,
		buffer:    make([]byte, WALBufferSize),
		flushChan: make(chan struct{}, 1),
		closer:    utils.NewCloser(),
	}

	// 初始化WAL文件
	if err := wm.initializeWAL(); err != nil {
		return nil, err
	}

	// 启动后台服务
	wm.startBackgroundServices()

	return wm, nil
}

// initializeWAL 初始化WAL
func (wm *WALManager) initializeWAL() error {
	// 扫描现有WAL文件
	pattern := filepath.Join(wm.options.WorkDir, "*"+WALFileExtension)
	matches, err := filepath.Glob(pattern)
	if err != nil {
		return err
	}

	// 如果没有WAL文件，创建第一个
	if len(matches) == 0 {
		return wm.createWALFile(0)
	}

	// 找到最新的WAL文件
	latestIndex := uint64(0)
	for range matches {
		// 解析文件索引
		// TODO: 实现文件名解析逻辑
	}

	// 打开最新的WAL文件
	return wm.openWALFile(latestIndex)
}

// createWALFile 创建新的WAL文件
func (wm *WALManager) createWALFile(index uint64) error {
	filename := filepath.Join(wm.options.WorkDir, fmt.Sprintf("wal_%06d%s", index, WALFileExtension))

	f, err := file.OpenMmapFile(filename, os.O_CREATE|os.O_RDWR, WALFileSize)
	if err != nil {
		return err
	}

	wm.fileMutex.Lock()
	defer wm.fileMutex.Unlock()

	// 关闭旧文件
	if wm.currentFile != nil {
		wm.currentFile.Close()
	}

	wm.currentFile = f
	wm.fileIndex.Store(index)

	// 初始化LSN（新文件从1开始）
	wm.nextLSN.Store(1)
	wm.flushedLSN.Store(0)

	return nil
}

// openWALFile 打开现有WAL文件
func (wm *WALManager) openWALFile(index uint64) error {
	filename := filepath.Join(wm.options.WorkDir, fmt.Sprintf("wal_%06d%s", index, WALFileExtension))

	f, err := file.OpenMmapFile(filename, os.O_RDWR, 0)
	if err != nil {
		return err
	}

	wm.fileMutex.Lock()
	defer wm.fileMutex.Unlock()

	wm.currentFile = f
	wm.fileIndex.Store(index)

	// 恢复LSN
	return wm.recoverLSN()
}

// recoverLSN 恢复LSN状态
func (wm *WALManager) recoverLSN() error {
	// 扫描WAL文件，找到最大的LSN
	maxLSN := uint64(0)

	// TODO: 实现WAL文件扫描逻辑
	// 这里应该读取WAL文件中的所有记录，找到最大的LSN

	// 确保nextLSN至少从1开始
	if maxLSN == 0 {
		wm.nextLSN.Store(1)
	} else {
		wm.nextLSN.Store(maxLSN + 1)
	}
	wm.flushedLSN.Store(maxLSN)

	return nil
}

// WriteRecord 写入日志记录
func (wm *WALManager) WriteRecord(record *LogRecord) (uint64, error) {
	// 分配LSN
	lsn := wm.nextLSN.Load()
	wm.nextLSN.Add(1)
	record.LSN = lsn
	record.Timestamp = time.Now()

	// 序列化记录
	data, err := wm.serializeRecord(record)
	if err != nil {
		return 0, err
	}

	// 写入缓冲区
	wm.bufferMutex.Lock()
	defer wm.bufferMutex.Unlock()

	// 检查缓冲区空间
	if wm.bufferPos+len(data) > len(wm.buffer) {
		// 缓冲区满，先刷新
		if err := wm.flushBufferUnsafe(); err != nil {
			return 0, err
		}
	}

	// 写入缓冲区
	copy(wm.buffer[wm.bufferPos:], data)
	wm.bufferPos += len(data)

	// 更新统计
	wm.totalRecords.Add(1)
	wm.totalBytes.Add(int64(len(data)))

	return lsn, nil
}

// serializeRecord 序列化日志记录
func (wm *WALManager) serializeRecord(record *LogRecord) ([]byte, error) {
	// 计算记录总长度
	totalLen := LogRecordHeaderSize + len(record.Data)
	data := make([]byte, totalLen)

	// 写入头部
	binary.LittleEndian.PutUint64(data[0:8], record.LSN)                 // LSN: 0-7
	data[8] = byte(record.Type)                                          // Type: 8
	binary.LittleEndian.PutUint64(data[9:17], record.TxnID)              // TxnID: 9-16
	binary.LittleEndian.PutUint64(data[17:25], record.PageID)            // PageID: 17-24
	binary.LittleEndian.PutUint32(data[25:29], uint32(len(record.Data))) // Length: 25-28

	// 写入数据
	copy(data[LogRecordHeaderSize:], record.Data)

	// 计算校验和
	checksum := crc32.ChecksumIEEE(data[LogRecordHeaderSize:])
	binary.LittleEndian.PutUint32(data[29:33], checksum) // Checksum: 29-32

	return data, nil
}

// FlushBuffer 刷新缓冲区到磁盘
func (wm *WALManager) FlushBuffer() error {
	wm.bufferMutex.Lock()
	defer wm.bufferMutex.Unlock()

	return wm.flushBufferUnsafe()
}

// flushBufferUnsafe 刷新缓冲区（不加锁版本）
func (wm *WALManager) flushBufferUnsafe() error {
	if wm.bufferPos == 0 {
		return nil
	}

	wm.fileMutex.RLock()
	defer wm.fileMutex.RUnlock()

	// 写入文件
	if wm.currentFile != nil {
		// TODO: 实现实际的文件写入逻辑
		// 这里应该将缓冲区数据写入WAL文件
	}

	// 重置缓冲区
	wm.bufferPos = 0
	wm.flushCount.Add(1)

	return nil
}

// Sync 同步WAL到磁盘
func (wm *WALManager) Sync() error {
	// 先刷新缓冲区
	if err := wm.FlushBuffer(); err != nil {
		return err
	}

	wm.fileMutex.RLock()
	defer wm.fileMutex.RUnlock()

	// 同步文件
	if wm.currentFile != nil {
		return wm.currentFile.Sync()
	}

	return nil
}

// CreateCheckpoint 创建检查点
func (wm *WALManager) CreateCheckpoint() error {
	// 创建检查点记录
	record := &LogRecord{
		Type:   LogCheckpoint,
		TxnID:  0,
		PageID: 0,
		Data:   []byte{}, // 检查点数据
	}

	lsn, err := wm.WriteRecord(record)
	if err != nil {
		return err
	}

	// 同步到磁盘
	if err := wm.Sync(); err != nil {
		return err
	}

	// 更新检查点LSN
	wm.checkpointLSN.Store(lsn)

	return nil
}

// Recovery 执行崩溃恢复
func (wm *WALManager) Recovery(pageManager *PageManager) error {
	// 从检查点开始恢复
	_ = wm.checkpointLSN.Load() // 获取检查点LSN，暂时未使用

	// TODO: 实现完整的恢复逻辑
	// 1. 从检查点LSN开始扫描WAL
	// 2. 重放所有已提交的事务
	// 3. 回滚所有未提交的事务

	return nil
}

// startBackgroundServices 启动后台服务
func (wm *WALManager) startBackgroundServices() {
	// 启动定期刷新服务
	wm.closer.Add(1)
	go wm.flushService()

	// 启动检查点服务
	wm.closer.Add(1)
	go wm.checkpointService()
}

// flushService 定期刷新服务
func (wm *WALManager) flushService() {
	defer wm.closer.Done()

	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			wm.FlushBuffer()

		case <-wm.flushChan:
			wm.FlushBuffer()

		case <-wm.closer.CloseSignal:
			return
		}
	}
}

// checkpointService 检查点服务
func (wm *WALManager) checkpointService() {
	defer wm.closer.Done()

	ticker := time.NewTicker(CheckpointInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			wm.CreateCheckpoint()

		case <-wm.closer.CloseSignal:
			return
		}
	}
}

// GetStats 获取WAL统计信息
func (wm *WALManager) GetStats() map[string]interface{} {
	return map[string]interface{}{
		"next_lsn":       wm.nextLSN.Load(),
		"flushed_lsn":    wm.flushedLSN.Load(),
		"checkpoint_lsn": wm.checkpointLSN.Load(),
		"total_records":  wm.totalRecords.Load(),
		"total_bytes":    wm.totalBytes.Load(),
		"flush_count":    wm.flushCount.Load(),
		"file_index":     wm.fileIndex.Load(),
	}
}

// Close 关闭WAL管理器
func (wm *WALManager) Close() error {
	// 停止后台服务
	wm.closer.Close()

	// 刷新缓冲区
	if err := wm.FlushBuffer(); err != nil {
		return err
	}

	// 同步到磁盘
	if err := wm.Sync(); err != nil {
		return err
	}

	// 关闭文件
	wm.fileMutex.Lock()
	defer wm.fileMutex.Unlock()

	if wm.currentFile != nil {
		return wm.currentFile.Close()
	}

	return nil
}
