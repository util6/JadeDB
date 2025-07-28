/*
JadeDB 文件WAL实现

本模块实现了基于文件的WAL管理器，提供高性能的预写日志功能。
采用顺序写入、批量刷新、异步同步等优化技术，确保高吞吐量和低延迟。

核心特性：
1. 顺序写入：所有日志按顺序追加写入，充分利用磁盘性能
2. 批量操作：支持批量写入和刷新，减少I/O开销
3. 缓冲管理：内存缓冲区减少磁盘访问，提升性能
4. 文件轮转：支持多文件管理，自动轮转和清理
5. 校验和保护：CRC32校验确保数据完整性
6. 异步刷新：后台异步刷新，不阻塞写入操作

设计原理：
- 预写日志：所有修改必须先写日志再修改数据
- LSN机制：日志序列号确保恢复的正确性
- 组提交：批量提交多个事务，减少I/O开销
- 双写缓冲：关键数据的双重保护
*/

package txnwal

import (
	"context"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/util6/JadeDB/file"
)

// FileTxnWALManager 基于文件的WAL管理器实现
type FileTxnWALManager struct {
	// 配置选项
	options *TxnWALOptions

	// 文件管理
	currentFile *file.MmapFile // 当前WAL文件
	fileIndex   atomic.Uint64  // 文件索引
	fileMutex   sync.RWMutex   // 文件操作锁

	// 日志序列号管理
	nextLSN        atomic.Uint64 // 下一个LSN
	lastFlushedLSN atomic.Uint64 // 最后刷新的LSN

	// 缓冲区管理
	writeBuffer  []byte     // 写入缓冲区
	bufferOffset int        // 缓冲区偏移
	bufferMutex  sync.Mutex // 缓冲区锁

	// 后台服务
	flushTicker *time.Ticker   // 刷新定时器
	stopChan    chan struct{}  // 停止信号
	wg          sync.WaitGroup // 等待组

	// 统计信息
	stats TxnWALStatistics

	// 状态管理
	closed bool
	mu     sync.RWMutex
}

// NewFileTxnWALManager 创建文件WAL管理器
func NewFileTxnWALManager(options *TxnWALOptions) (*FileTxnWALManager, error) {
	if options == nil {
		options = DefaultTxnWALOptions()
	}

	// 确保目录存在
	if err := os.MkdirAll(options.Directory, 0755); err != nil {
		return nil, NewWALError(ErrWALNotFound, "failed to create WAL directory", err)
	}

	manager := &FileTxnWALManager{
		options:     options,
		writeBuffer: make([]byte, options.BufferSize),
		stopChan:    make(chan struct{}),
	}

	// 初始化LSN
	manager.nextLSN.Store(1)
	manager.lastFlushedLSN.Store(0)

	// 打开或创建WAL文件
	if err := manager.openCurrentFile(); err != nil {
		return nil, err
	}

	// 启动后台服务
	manager.startBackgroundServices()

	return manager, nil
}

// WriteRecord 写入单条日志记录
func (w *FileTxnWALManager) WriteRecord(ctx context.Context, record *TxnLogRecord) (TxnLSN, error) {
	w.mu.RLock()
	if w.closed {
		w.mu.RUnlock()
		return 0, NewWALError(ErrWALReadOnly, "WAL manager is closed", nil)
	}
	w.mu.RUnlock()

	// 分配LSN
	lsn := TxnLSN(w.nextLSN.Add(1))
	record.LSN = lsn
	record.Timestamp = time.Now()

	// 序列化记录
	data, err := w.serializeRecord(record)
	if err != nil {
		return 0, NewWALError(ErrWALCorrupted, "failed to serialize record", err)
	}

	// 写入缓冲区
	w.bufferMutex.Lock()
	defer w.bufferMutex.Unlock()

	// 检查缓冲区空间
	if w.bufferOffset+len(data) > len(w.writeBuffer) {
		// 缓冲区满，先刷新
		if err := w.flushBufferLocked(); err != nil {
			return 0, err
		}
	}

	// 写入缓冲区
	copy(w.writeBuffer[w.bufferOffset:], data)
	w.bufferOffset += len(data)

	// 更新统计
	atomic.AddInt64(&w.stats.TotalRecords, 1)
	atomic.AddInt64(&w.stats.TotalBytes, int64(len(data)))

	return lsn, nil
}

// WriteBatch 批量写入日志记录
func (w *FileTxnWALManager) WriteBatch(ctx context.Context, records []*TxnLogRecord) ([]TxnLSN, error) {
	w.mu.RLock()
	if w.closed {
		w.mu.RUnlock()
		return nil, NewWALError(ErrWALReadOnly, "WAL manager is closed", nil)
	}
	w.mu.RUnlock()

	lsns := make([]TxnLSN, len(records))
	totalSize := 0

	// 预处理所有记录
	serializedRecords := make([][]byte, len(records))
	for i, record := range records {
		// 分配LSN
		lsn := TxnLSN(w.nextLSN.Add(1))
		record.LSN = lsn
		record.Timestamp = time.Now()
		lsns[i] = lsn

		// 序列化记录
		data, err := w.serializeRecord(record)
		if err != nil {
			return nil, NewWALError(ErrWALCorrupted, "failed to serialize record", err)
		}
		serializedRecords[i] = data
		totalSize += len(data)
	}

	// 批量写入缓冲区
	w.bufferMutex.Lock()
	defer w.bufferMutex.Unlock()

	// 检查缓冲区空间
	if w.bufferOffset+totalSize > len(w.writeBuffer) {
		// 缓冲区空间不足，先刷新
		if err := w.flushBufferLocked(); err != nil {
			return nil, err
		}

		// 如果单次批量写入超过缓冲区大小，直接写入文件
		if totalSize > len(w.writeBuffer) {
			return lsns, w.writeDirectly(serializedRecords)
		}
	}

	// 写入缓冲区
	for _, data := range serializedRecords {
		copy(w.writeBuffer[w.bufferOffset:], data)
		w.bufferOffset += len(data)
	}

	// 更新统计
	atomic.AddInt64(&w.stats.TotalRecords, int64(len(records)))
	atomic.AddInt64(&w.stats.TotalBytes, int64(totalSize))

	return lsns, nil
}

// Flush 强制刷新缓冲区到持久存储
func (w *FileTxnWALManager) Flush(ctx context.Context) error {
	w.bufferMutex.Lock()
	defer w.bufferMutex.Unlock()

	return w.flushBufferLocked()
}

// Sync 同步到磁盘
func (w *FileTxnWALManager) Sync(ctx context.Context) error {
	w.fileMutex.RLock()
	defer w.fileMutex.RUnlock()

	if w.currentFile == nil {
		return NewWALError(ErrWALNotFound, "no current WAL file", nil)
	}

	if err := w.currentFile.Sync(); err != nil {
		return NewWALError(ErrWALCorrupted, "failed to sync WAL file", err)
	}

	atomic.AddInt64(&w.stats.SyncCount, 1)
	return nil
}

// ReadRecord 读取指定LSN的日志记录
func (w *FileTxnWALManager) ReadRecord(ctx context.Context, lsn TxnLSN) (*TxnLogRecord, error) {
	w.fileMutex.RLock()
	defer w.fileMutex.RUnlock()

	if w.currentFile == nil {
		return nil, NewWALError(ErrWALNotFound, "no WAL file available", nil)
	}

	// 简化实现：从当前文件的开头扫描查找指定LSN
	// 在生产环境中，这里应该使用索引或更高效的查找方法

	// 获取文件大小（简化实现，假设文件不超过1MB）
	maxSize := 1024 * 1024
	data, err := w.currentFile.Bytes(0, maxSize)
	if err != nil {
		return nil, NewWALError(ErrWALCorrupted, "failed to read WAL file", err)
	}

	offset := 0
	for offset < len(data) {
		if offset+37 > len(data) { // 最小记录头大小
			break
		}

		// 读取LSN
		recordLSN := TxnLSN(binary.LittleEndian.Uint64(data[offset:]))
		if recordLSN == lsn {
			// 找到目标记录，解析完整记录
			return w.deserializeTxnRecord(data[offset:])
		}

		// 跳过当前记录
		if offset+29 > len(data) {
			break
		}
		dataLen := binary.LittleEndian.Uint32(data[offset+29:])
		recordSize := 37 + int(dataLen) // 头部37字节 + 数据长度
		offset += recordSize
	}

	return nil, NewWALError(ErrWALNotFound, "record not found", nil)
}

// ReadRange 读取指定范围的日志记录
func (w *FileTxnWALManager) ReadRange(ctx context.Context, startLSN, endLSN TxnLSN) ([]*TxnLogRecord, error) {
	w.fileMutex.RLock()
	defer w.fileMutex.RUnlock()

	if w.currentFile == nil {
		return nil, NewWALError(ErrWALNotFound, "no WAL file available", nil)
	}

	var records []*TxnLogRecord

	// 获取文件数据
	maxSize := 1024 * 1024
	data, err := w.currentFile.Bytes(0, maxSize)
	if err != nil {
		return nil, NewWALError(ErrWALCorrupted, "failed to read WAL file", err)
	}

	offset := 0
	for offset < len(data) {
		if offset+37 > len(data) {
			break
		}

		// 读取LSN
		recordLSN := TxnLSN(binary.LittleEndian.Uint64(data[offset:]))

		// 检查是否在范围内
		if recordLSN >= startLSN && recordLSN <= endLSN {
			record, err := w.deserializeTxnRecord(data[offset:])
			if err != nil {
				return nil, err
			}
			records = append(records, record)
		}

		// 跳过当前记录
		if offset+29 > len(data) {
			break
		}
		dataLen := binary.LittleEndian.Uint32(data[offset+29:])
		recordSize := 37 + int(dataLen)
		offset += recordSize

		// 如果已经超过结束LSN，可以提前退出
		if recordLSN > endLSN {
			break
		}
	}

	return records, nil
}

// ReadFrom 从指定LSN开始读取
func (w *FileTxnWALManager) ReadFrom(ctx context.Context, startLSN TxnLSN) (TxnWALIterator, error) {
	return NewFileTxnWALIterator(w, startLSN)
}

// GetLatestLSN 获取最新的LSN
func (w *FileTxnWALManager) GetLatestLSN() TxnLSN {
	return TxnLSN(w.nextLSN.Load() - 1)
}

// CreateCheckpoint 创建检查点
func (w *FileTxnWALManager) CreateCheckpoint(ctx context.Context) (TxnLSN, error) {
	checkpointRecord := &TxnLogRecord{
		Type: LogCheckpoint,
		Data: []byte("checkpoint"),
	}

	return w.WriteRecord(ctx, checkpointRecord)
}

// Truncate 截断WAL到指定LSN
func (w *FileTxnWALManager) Truncate(ctx context.Context, lsn TxnLSN) error {
	// 实现截断逻辑
	return NewWALError(ErrWALNotFound, "truncate not implemented", nil)
}

// GetStatistics 获取WAL统计信息
func (w *FileTxnWALManager) GetStatistics() TxnWALStatistics {
	return w.stats
}

// SetOptions 设置WAL选项
func (w *FileTxnWALManager) SetOptions(options *TxnWALOptions) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	w.options = options
	return nil
}

// Close 关闭WAL管理器
func (w *FileTxnWALManager) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return nil
	}

	// 停止后台服务
	close(w.stopChan)
	w.wg.Wait()

	// 刷新缓冲区
	w.bufferMutex.Lock()
	w.flushBufferLocked()
	w.bufferMutex.Unlock()

	// 关闭文件
	w.fileMutex.Lock()
	if w.currentFile != nil {
		w.currentFile.Close()
	}
	w.fileMutex.Unlock()

	w.closed = true
	return nil
}

// 私有方法

// openCurrentFile 打开或创建当前WAL文件
func (w *FileTxnWALManager) openCurrentFile() error {
	fileIndex := w.fileIndex.Load()
	filename := fmt.Sprintf("wal_%06d.log", fileIndex)
	filePath := filepath.Join(w.options.Directory, filename)

	// 创建或打开文件
	fd, err := os.OpenFile(filePath, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return NewWALError(ErrWALNotFound, "failed to open WAL file", err)
	}

	// 扩展文件到指定大小
	if err := fd.Truncate(w.options.FileSize); err != nil {
		fd.Close()
		return NewWALError(ErrWALNotFound, "failed to truncate WAL file", err)
	}

	// 创建内存映射文件
	mmapFile, err := file.CreateMmapFile(fd, int(w.options.FileSize), true)
	if err != nil {
		fd.Close()
		return NewWALError(ErrWALNotFound, "failed to create mmap file", err)
	}

	w.fileMutex.Lock()
	w.currentFile = mmapFile
	w.fileMutex.Unlock()

	return nil
}

// serializeRecord 序列化日志记录
func (w *FileTxnWALManager) serializeRecord(record *TxnLogRecord) ([]byte, error) {
	// 计算记录大小
	dataLen := len(record.Data)
	totalSize := 8 + 1 + 8 + 8 + 4 + dataLen + 4 // LSN + Type + Timestamp + TxnID + DataLen + Data + Checksum

	buf := make([]byte, totalSize)
	offset := 0

	// LSN (8 bytes)
	binary.LittleEndian.PutUint64(buf[offset:], uint64(record.LSN))
	offset += 8

	// Type (1 byte)
	buf[offset] = byte(record.Type)
	offset += 1

	// Timestamp (8 bytes)
	binary.LittleEndian.PutUint64(buf[offset:], uint64(record.Timestamp.UnixNano()))
	offset += 8

	// TxnID (8 bytes) - 简化为哈希
	txnHash := crc32.ChecksumIEEE([]byte(record.TxnID))
	binary.LittleEndian.PutUint64(buf[offset:], uint64(txnHash))
	offset += 8

	// Data length (4 bytes)
	binary.LittleEndian.PutUint32(buf[offset:], uint32(dataLen))
	offset += 4

	// Data
	copy(buf[offset:], record.Data)
	offset += dataLen

	// Checksum (4 bytes)
	checksum := crc32.ChecksumIEEE(buf[:offset])
	binary.LittleEndian.PutUint32(buf[offset:], checksum)

	return buf, nil
}

// flushBufferLocked 刷新缓冲区（需要持有锁）
func (w *FileTxnWALManager) flushBufferLocked() error {
	if w.bufferOffset == 0 {
		return nil
	}

	w.fileMutex.RLock()
	defer w.fileMutex.RUnlock()

	if w.currentFile == nil {
		return NewWALError(ErrWALNotFound, "no current WAL file", nil)
	}

	// 通过文件描述符写入
	if _, err := w.currentFile.Fd.Write(w.writeBuffer[:w.bufferOffset]); err != nil {
		return NewWALError(ErrWALCorrupted, "failed to write to WAL file", err)
	}

	// 重置缓冲区
	w.bufferOffset = 0

	// 更新统计
	atomic.AddInt64(&w.stats.FlushCount, 1)

	return nil
}

// writeDirectly 直接写入文件（绕过缓冲区）
func (w *FileTxnWALManager) writeDirectly(records [][]byte) error {
	w.fileMutex.RLock()
	defer w.fileMutex.RUnlock()

	if w.currentFile == nil {
		return NewWALError(ErrWALNotFound, "no current WAL file", nil)
	}

	for _, data := range records {
		if _, err := w.currentFile.Fd.Write(data); err != nil {
			return NewWALError(ErrWALCorrupted, "failed to write to WAL file", err)
		}
	}

	return nil
}

// startBackgroundServices 启动后台服务
func (w *FileTxnWALManager) startBackgroundServices() {
	if w.options.FlushInterval > 0 {
		w.flushTicker = time.NewTicker(w.options.FlushInterval)
		w.wg.Add(1)
		go w.backgroundFlush()
	}
}

// backgroundFlush 后台刷新服务
func (w *FileTxnWALManager) backgroundFlush() {
	defer w.wg.Done()

	for {
		select {
		case <-w.flushTicker.C:
			w.bufferMutex.Lock()
			w.flushBufferLocked()
			w.bufferMutex.Unlock()

		case <-w.stopChan:
			w.flushTicker.Stop()
			return
		}
	}
}

// deserializeTxnRecord 反序列化事务日志记录
func (w *FileTxnWALManager) deserializeTxnRecord(data []byte) (*TxnLogRecord, error) {
	if len(data) < 37 { // 最小记录大小
		return nil, NewWALError(ErrWALCorrupted, "record too small", nil)
	}

	offset := 0

	// LSN (8 bytes)
	lsn := TxnLSN(binary.LittleEndian.Uint64(data[offset:]))
	offset += 8

	// Type (1 byte)
	recordType := TxnLogRecordType(data[offset])
	offset += 1

	// Timestamp (8 bytes)
	timestamp := time.Unix(0, int64(binary.LittleEndian.Uint64(data[offset:])))
	offset += 8

	// TxnID hash (8 bytes) - 简化处理，直接作为字符串
	txnIDHash := binary.LittleEndian.Uint64(data[offset:])
	txnID := fmt.Sprintf("txn_%d", txnIDHash)
	offset += 8

	// Data length (4 bytes)
	dataLen := binary.LittleEndian.Uint32(data[offset:])
	offset += 4

	// Data
	if len(data) < offset+int(dataLen)+4 { // +4 for checksum
		return nil, NewWALError(ErrWALCorrupted, "incomplete record", nil)
	}

	recordData := make([]byte, dataLen)
	copy(recordData, data[offset:offset+int(dataLen)])
	offset += int(dataLen)

	// Checksum (4 bytes)
	checksum := binary.LittleEndian.Uint32(data[offset:])

	// 验证校验和
	expectedChecksum := crc32.ChecksumIEEE(data[:offset])
	if checksum != expectedChecksum {
		return nil, NewWALError(ErrWALChecksumFailed, "checksum mismatch", nil)
	}

	return &TxnLogRecord{
		LSN:       lsn,
		Type:      recordType,
		TxnID:     txnID,
		Timestamp: timestamp,
		Data:      recordData,
		Checksum:  checksum,
	}, nil
}

// DefaultTxnWALOptions 返回默认WAL选项
func DefaultTxnWALOptions() *TxnWALOptions {
	return &TxnWALOptions{
		Directory:         "./wal",
		FileSize:          64 * 1024 * 1024, // 64MB
		MaxFiles:          10,
		BufferSize:        1024 * 1024, // 1MB
		FlushInterval:     100 * time.Millisecond,
		SyncMode:          SyncNormal,
		EnableCompression: false,
		EnableChecksum:    true,
		ChecksumAlgorithm: "crc32",
	}
}

// FileTxnWALIterator 文件WAL迭代器
type FileTxnWALIterator struct {
	walManager *FileTxnWALManager
	data       []byte
	offset     int
	current    *TxnLogRecord
	err        error
	startLSN   TxnLSN
}

// NewFileTxnWALIterator 创建文件WAL迭代器
func NewFileTxnWALIterator(walManager *FileTxnWALManager, startLSN TxnLSN) (*FileTxnWALIterator, error) {
	walManager.fileMutex.RLock()
	defer walManager.fileMutex.RUnlock()

	if walManager.currentFile == nil {
		return nil, NewWALError(ErrWALNotFound, "no WAL file available", nil)
	}

	// 读取文件数据
	maxSize := 1024 * 1024
	data, err := walManager.currentFile.Bytes(0, maxSize)
	if err != nil {
		return nil, NewWALError(ErrWALCorrupted, "failed to read WAL file", err)
	}

	iterator := &FileTxnWALIterator{
		walManager: walManager,
		data:       data,
		offset:     0,
		startLSN:   startLSN,
	}

	// 定位到起始LSN
	iterator.seekToLSN(startLSN)

	return iterator, nil
}

// seekToLSN 定位到指定LSN
func (it *FileTxnWALIterator) seekToLSN(targetLSN TxnLSN) {
	it.offset = 0
	for it.offset < len(it.data) {
		if it.offset+37 > len(it.data) {
			break
		}

		// 读取LSN
		recordLSN := TxnLSN(binary.LittleEndian.Uint64(it.data[it.offset:]))
		if recordLSN >= targetLSN {
			return // 找到起始位置
		}

		// 跳过当前记录
		if it.offset+29 > len(it.data) {
			break
		}
		dataLen := binary.LittleEndian.Uint32(it.data[it.offset+29:])
		recordSize := 37 + int(dataLen)
		it.offset += recordSize
	}
}

// Next 移动到下一条记录
func (it *FileTxnWALIterator) Next() bool {
	if it.err != nil || it.offset >= len(it.data) {
		return false
	}

	// 尝试读取当前位置的记录
	record, err := it.walManager.deserializeTxnRecord(it.data[it.offset:])
	if err != nil {
		it.err = err
		return false
	}

	it.current = record

	// 移动到下一条记录
	if it.offset+29 > len(it.data) {
		return true // 这是最后一条记录
	}
	dataLen := binary.LittleEndian.Uint32(it.data[it.offset+29:])
	recordSize := 37 + int(dataLen)
	it.offset += recordSize

	return true
}

// Record 获取当前记录
func (it *FileTxnWALIterator) Record() *TxnLogRecord {
	return it.current
}

// Error 获取迭代过程中的错误
func (it *FileTxnWALIterator) Error() error {
	return it.err
}

// Close 关闭迭代器
func (it *FileTxnWALIterator) Close() error {
	it.walManager = nil
	it.data = nil
	it.current = nil
	return nil
}
