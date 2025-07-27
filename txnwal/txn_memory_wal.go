/*
JadeDB 内存WAL实现

本模块实现了基于内存的WAL管理器，主要用于测试和开发环境。
提供与文件WAL相同的接口，但数据存储在内存中，重启后数据丢失。

核心特性：
1. 内存存储：所有数据存储在内存中，访问速度快
2. 完整接口：实现所有WAL接口，与文件WAL兼容
3. 测试友好：支持快速重置，便于单元测试
4. 调试支持：提供详细的调试信息和统计

使用场景：
- 单元测试：快速创建和销毁WAL实例
- 开发调试：无需处理文件I/O，专注业务逻辑
- 性能基准：测试WAL接口的性能上限
- 原型验证：快速验证WAL相关功能

注意：内存WAL不提供持久性保证，仅用于测试和开发！
*/

package txnwal

import (
	"context"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// MemoryTxnWALManager 基于内存的WAL管理器实现
type MemoryTxnWALManager struct {
	// 配置选项
	options *TxnWALOptions

	// 内存存储
	records   map[TxnLSN]*TxnLogRecord // 日志记录存储
	recordsMu sync.RWMutex             // 记录访问锁

	// LSN管理
	nextLSN        atomic.Uint64 // 下一个LSN
	lastFlushedLSN atomic.Uint64 // 最后刷新的LSN

	// 统计信息
	stats TxnWALStatistics

	// 状态管理
	closed bool
	mu     sync.RWMutex
}

// NewMemoryTxnWALManager 创建内存WAL管理器
func NewMemoryTxnWALManager(options *TxnWALOptions) (*MemoryTxnWALManager, error) {
	if options == nil {
		options = DefaultTxnWALOptions()
	}

	manager := &MemoryTxnWALManager{
		options: options,
		records: make(map[TxnLSN]*TxnLogRecord),
	}

	// 初始化LSN
	manager.nextLSN.Store(1)
	manager.lastFlushedLSN.Store(0)

	return manager, nil
}

// WriteRecord 写入单条日志记录
func (m *MemoryTxnWALManager) WriteRecord(ctx context.Context, record *TxnLogRecord) (TxnLSN, error) {
	m.mu.RLock()
	if m.closed {
		m.mu.RUnlock()
		return 0, NewWALError(ErrWALReadOnly, "memory WAL manager is closed", nil)
	}
	m.mu.RUnlock()

	// 分配LSN
	lsn := TxnLSN(m.nextLSN.Add(1))

	// 复制记录（避免外部修改）
	recordCopy := &TxnLogRecord{
		LSN:       lsn,
		Type:      record.Type,
		TxnID:     record.TxnID,
		Timestamp: time.Now(),
		Data:      make([]byte, len(record.Data)),
		Checksum:  record.Checksum,
	}
	copy(recordCopy.Data, record.Data)

	// 存储记录
	m.recordsMu.Lock()
	m.records[lsn] = recordCopy
	m.recordsMu.Unlock()

	// 更新统计
	atomic.AddInt64(&m.stats.TotalRecords, 1)
	atomic.AddInt64(&m.stats.TotalBytes, int64(len(record.Data)))

	return lsn, nil
}

// WriteBatch 批量写入日志记录
func (m *MemoryTxnWALManager) WriteBatch(ctx context.Context, records []*TxnLogRecord) ([]TxnLSN, error) {
	m.mu.RLock()
	if m.closed {
		m.mu.RUnlock()
		return nil, NewWALError(ErrWALReadOnly, "memory WAL manager is closed", nil)
	}
	m.mu.RUnlock()

	lsns := make([]TxnLSN, len(records))
	totalBytes := int64(0)

	// 批量分配LSN和复制记录
	recordCopies := make([]*TxnLogRecord, len(records))
	for i, record := range records {
		lsn := TxnLSN(m.nextLSN.Add(1))
		lsns[i] = lsn

		recordCopy := &TxnLogRecord{
			LSN:       lsn,
			Type:      record.Type,
			TxnID:     record.TxnID,
			Timestamp: time.Now(),
			Data:      make([]byte, len(record.Data)),
			Checksum:  record.Checksum,
		}
		copy(recordCopy.Data, record.Data)
		recordCopies[i] = recordCopy
		totalBytes += int64(len(record.Data))
	}

	// 批量存储记录
	m.recordsMu.Lock()
	for i, recordCopy := range recordCopies {
		m.records[lsns[i]] = recordCopy
	}
	m.recordsMu.Unlock()

	// 更新统计
	atomic.AddInt64(&m.stats.TotalRecords, int64(len(records)))
	atomic.AddInt64(&m.stats.TotalBytes, totalBytes)

	return lsns, nil
}

// Flush 强制刷新缓冲区到持久存储
func (m *MemoryTxnWALManager) Flush(ctx context.Context) error {
	// 内存WAL无需刷新，直接更新统计
	atomic.AddInt64(&m.stats.FlushCount, 1)
	m.lastFlushedLSN.Store(m.nextLSN.Load() - 1)
	return nil
}

// Sync 同步到磁盘
func (m *MemoryTxnWALManager) Sync(ctx context.Context) error {
	// 内存WAL无需同步，直接更新统计
	atomic.AddInt64(&m.stats.SyncCount, 1)
	return nil
}

// ReadRecord 读取指定LSN的日志记录
func (m *MemoryTxnWALManager) ReadRecord(ctx context.Context, lsn TxnLSN) (*TxnLogRecord, error) {
	m.recordsMu.RLock()
	defer m.recordsMu.RUnlock()

	record, exists := m.records[lsn]
	if !exists {
		return nil, NewWALError(ErrWALNotFound, "record not found", nil)
	}

	// 返回记录副本
	return m.copyRecord(record), nil
}

// ReadRange 读取指定范围的日志记录
func (m *MemoryTxnWALManager) ReadRange(ctx context.Context, startLSN, endLSN TxnLSN) ([]*TxnLogRecord, error) {
	m.recordsMu.RLock()
	defer m.recordsMu.RUnlock()

	var records []*TxnLogRecord
	for lsn := startLSN; lsn <= endLSN; lsn++ {
		if record, exists := m.records[lsn]; exists {
			records = append(records, m.copyRecord(record))
		}
	}

	return records, nil
}

// ReadFrom 从指定LSN开始读取
func (m *MemoryTxnWALManager) ReadFrom(ctx context.Context, startLSN TxnLSN) (TxnWALIterator, error) {
	return NewMemoryTxnWALIterator(m, startLSN), nil
}

// GetLatestLSN 获取最新的LSN
func (m *MemoryTxnWALManager) GetLatestLSN() TxnLSN {
	nextLSN := m.nextLSN.Load()
	if nextLSN == 1 {
		return 0 // 没有记录
	}
	return TxnLSN(nextLSN - 1)
}

// CreateCheckpoint 创建检查点
func (m *MemoryTxnWALManager) CreateCheckpoint(ctx context.Context) (TxnLSN, error) {
	checkpointRecord := &TxnLogRecord{
		Type: LogCheckpoint,
		Data: []byte("checkpoint"),
	}

	return m.WriteRecord(ctx, checkpointRecord)
}

// Truncate 截断WAL到指定LSN
func (m *MemoryTxnWALManager) Truncate(ctx context.Context, lsn TxnLSN) error {
	m.recordsMu.Lock()
	defer m.recordsMu.Unlock()

	// 删除指定LSN之后的所有记录
	for recordLSN := range m.records {
		if recordLSN > lsn {
			delete(m.records, recordLSN)
		}
	}

	// 重置LSN
	m.nextLSN.Store(uint64(lsn + 1))

	return nil
}

// GetStatistics 获取WAL统计信息
func (m *MemoryTxnWALManager) GetStatistics() TxnWALStatistics {
	m.recordsMu.RLock()
	recordCount := len(m.records)
	m.recordsMu.RUnlock()

	stats := m.stats
	stats.TotalRecords = int64(recordCount)
	return stats
}

// SetOptions 设置WAL选项
func (m *MemoryTxnWALManager) SetOptions(options *TxnWALOptions) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.options = options
	return nil
}

// Close 关闭WAL管理器
func (m *MemoryTxnWALManager) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.closed {
		return nil
	}

	// 清空内存数据
	m.recordsMu.Lock()
	m.records = make(map[TxnLSN]*TxnLogRecord)
	m.recordsMu.Unlock()

	m.closed = true
	return nil
}

// Reset 重置WAL管理器（测试用）
func (m *MemoryTxnWALManager) Reset() {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.recordsMu.Lock()
	m.records = make(map[TxnLSN]*TxnLogRecord)
	m.recordsMu.Unlock()

	m.nextLSN.Store(1)
	m.lastFlushedLSN.Store(0)
	m.stats = TxnWALStatistics{}
}

// GetAllRecords 获取所有记录（测试用）
func (m *MemoryTxnWALManager) GetAllRecords() []*TxnLogRecord {
	m.recordsMu.RLock()
	defer m.recordsMu.RUnlock()

	records := make([]*TxnLogRecord, 0, len(m.records))
	for _, record := range m.records {
		records = append(records, m.copyRecord(record))
	}

	// 按LSN排序
	sort.Slice(records, func(i, j int) bool {
		return records[i].LSN < records[j].LSN
	})

	return records
}

// copyRecord 复制记录
func (m *MemoryTxnWALManager) copyRecord(record *TxnLogRecord) *TxnLogRecord {
	recordCopy := &TxnLogRecord{
		LSN:       record.LSN,
		Type:      record.Type,
		TxnID:     record.TxnID,
		Timestamp: record.Timestamp,
		Checksum:  record.Checksum,
	}
	if len(record.Data) > 0 {
		recordCopy.Data = make([]byte, len(record.Data))
		copy(recordCopy.Data, record.Data)
	}
	return recordCopy
}

// MemoryTxnWALIterator 内存WAL迭代器
type MemoryTxnWALIterator struct {
	manager *MemoryTxnWALManager
	lsns    []TxnLSN
	current int
	err     error
}

// NewMemoryTxnWALIterator 创建内存WAL迭代器
func NewMemoryTxnWALIterator(manager *MemoryTxnWALManager, startLSN TxnLSN) *MemoryTxnWALIterator {
	manager.recordsMu.RLock()
	defer manager.recordsMu.RUnlock()

	// 收集所有LSN并排序
	var lsns []TxnLSN
	for lsn := range manager.records {
		if lsn >= startLSN {
			lsns = append(lsns, lsn)
		}
	}

	sort.Slice(lsns, func(i, j int) bool {
		return lsns[i] < lsns[j]
	})

	return &MemoryTxnWALIterator{
		manager: manager,
		lsns:    lsns,
		current: -1,
	}
}

// Next 移动到下一条记录
func (it *MemoryTxnWALIterator) Next() bool {
	it.current++
	return it.current < len(it.lsns)
}

// Record 获取当前记录
func (it *MemoryTxnWALIterator) Record() *TxnLogRecord {
	if it.current < 0 || it.current >= len(it.lsns) {
		return nil
	}

	lsn := it.lsns[it.current]
	it.manager.recordsMu.RLock()
	record, exists := it.manager.records[lsn]
	it.manager.recordsMu.RUnlock()

	if !exists {
		return nil
	}

	return it.manager.copyRecord(record)
}

// Error 获取迭代过程中的错误
func (it *MemoryTxnWALIterator) Error() error {
	return it.err
}

// Close 关闭迭代器
func (it *MemoryTxnWALIterator) Close() error {
	it.manager = nil
	it.lsns = nil
	return nil
}
