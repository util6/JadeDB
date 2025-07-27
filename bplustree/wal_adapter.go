/*
B+树WAL适配器

本模块实现了B+树存储引擎与统一WAL系统的适配器。
通过适配器模式，将B+树内部的页面级WAL与事务级WAL进行集成。

设计原理：
1. 分层WAL：事务级WAL记录事务操作，存储级WAL记录页面操作
2. 适配器模式：通过适配器连接两个层次的WAL
3. 职责分离：事务WAL负责ACID，存储WAL负责崩溃恢复
4. 统一接口：对外提供统一的WAL接口

架构层次：
- 事务层：使用统一WAL接口记录事务操作
- 适配器层：协调事务WAL和存储WAL
- 存储层：使用内部WAL记录页面操作
*/

package bplustree

import (
	"context"
	"fmt"

	"github.com/util6/JadeDB/txnwal"
)

// BTreeWALAdapter B+树WAL适配器
// 实现统一WAL接口，同时管理B+树内部WAL
type BTreeWALAdapter struct {
	// 统一WAL管理器（事务级）
	transactionWAL txnwal.TxnWALManager

	// B+树内部WAL管理器（存储级）
	storageWAL *WALManager

	// B+树引擎引用
	btree *BPlusTree
}

// NewBTreeWALAdapter 创建B+树WAL适配器
func NewBTreeWALAdapter(btree *BPlusTree, transactionWAL txnwal.TxnWALManager) *BTreeWALAdapter {
	return &BTreeWALAdapter{
		transactionWAL: transactionWAL,
		storageWAL:     btree.walManager,
		btree:          btree,
	}
}

// WriteRecord 写入日志记录
func (adapter *BTreeWALAdapter) WriteRecord(ctx context.Context, record *txnwal.TxnLogRecord) (txnwal.TxnLSN, error) {
	// 根据记录类型决定写入哪个WAL
	switch record.Type {
	case txnwal.LogTransactionBegin, txnwal.LogTransactionCommit, txnwal.LogTransactionRollback:
		// 事务操作写入事务WAL
		return adapter.transactionWAL.WriteRecord(ctx, record)

	case txnwal.LogDataInsert, txnwal.LogDataUpdate, txnwal.LogDataDelete:
		// 数据操作同时写入两个WAL
		// 先写事务WAL（用于事务恢复）
		txnLSN, err := adapter.transactionWAL.WriteRecord(ctx, record)
		if err != nil {
			return 0, err
		}

		// 再写存储WAL（用于页面恢复）
		if err := adapter.writeToStorageWAL(record); err != nil {
			// 存储WAL写入失败，但事务WAL已写入，记录警告
			fmt.Printf("Warning: failed to write to storage WAL: %v\n", err)
		}

		return txnLSN, nil

	case txnwal.LogPageSplit, txnwal.LogPageMerge, txnwal.LogPageAllocation, txnwal.LogPageDeallocation:
		// 页面操作只写入存储WAL
		return 0, adapter.writeToStorageWAL(record)

	default:
		// 其他操作写入事务WAL
		return adapter.transactionWAL.WriteRecord(ctx, record)
	}
}

// WriteBatch 批量写入日志记录
func (adapter *BTreeWALAdapter) WriteBatch(ctx context.Context, records []*txnwal.TxnLogRecord) ([]txnwal.TxnLSN, error) {
	// 分类记录
	var transactionRecords []*txnwal.TxnLogRecord
	var storageRecords []*txnwal.TxnLogRecord
	var dataRecords []*txnwal.TxnLogRecord

	for _, record := range records {
		switch record.Type {
		case txnwal.LogTransactionBegin, txnwal.LogTransactionCommit, txnwal.LogTransactionRollback:
			transactionRecords = append(transactionRecords, record)
		case txnwal.LogDataInsert, txnwal.LogDataUpdate, txnwal.LogDataDelete:
			dataRecords = append(dataRecords, record)
		case txnwal.LogPageSplit, txnwal.LogPageMerge, txnwal.LogPageAllocation, txnwal.LogPageDeallocation:
			storageRecords = append(storageRecords, record)
		default:
			transactionRecords = append(transactionRecords, record)
		}
	}

	var allLSNs []txnwal.TxnLSN

	// 批量写入事务记录
	if len(transactionRecords) > 0 {
		lsns, err := adapter.transactionWAL.WriteBatch(ctx, transactionRecords)
		if err != nil {
			return nil, err
		}
		allLSNs = append(allLSNs, lsns...)
	}

	// 批量写入数据记录（同时写入两个WAL）
	if len(dataRecords) > 0 {
		// 先写事务WAL
		lsns, err := adapter.transactionWAL.WriteBatch(ctx, dataRecords)
		if err != nil {
			return nil, err
		}
		allLSNs = append(allLSNs, lsns...)

		// 再写存储WAL
		for _, record := range dataRecords {
			if err := adapter.writeToStorageWAL(record); err != nil {
				fmt.Printf("Warning: failed to write data record to storage WAL: %v\n", err)
			}
		}
	}

	// 批量写入存储记录
	if len(storageRecords) > 0 {
		for _, record := range storageRecords {
			if err := adapter.writeToStorageWAL(record); err != nil {
				return nil, err
			}
		}
		// 存储WAL的LSN从0开始（表示存储级操作）
		for range storageRecords {
			allLSNs = append(allLSNs, 0)
		}
	}

	return allLSNs, nil
}

// Flush 刷新缓冲区
func (adapter *BTreeWALAdapter) Flush(ctx context.Context) error {
	// 同时刷新两个WAL
	if err := adapter.transactionWAL.Flush(ctx); err != nil {
		return err
	}

	// B+树内部WAL的刷新
	adapter.storageWAL.FlushBuffer()

	return nil
}

// Sync 同步到磁盘
func (adapter *BTreeWALAdapter) Sync(ctx context.Context) error {
	// 同时同步两个WAL
	if err := adapter.transactionWAL.Sync(ctx); err != nil {
		return err
	}

	// B+树内部WAL的同步
	adapter.storageWAL.Sync()

	return nil
}

// ReadRecord 读取日志记录
func (adapter *BTreeWALAdapter) ReadRecord(ctx context.Context, lsn txnwal.TxnLSN) (*txnwal.TxnLogRecord, error) {
	// 优先从事务WAL读取
	return adapter.transactionWAL.ReadRecord(ctx, lsn)
}

// ReadRange 读取范围记录
func (adapter *BTreeWALAdapter) ReadRange(ctx context.Context, startLSN, endLSN txnwal.TxnLSN) ([]*txnwal.TxnLogRecord, error) {
	// 从事务WAL读取
	return adapter.transactionWAL.ReadRange(ctx, startLSN, endLSN)
}

// ReadFrom 从指定LSN开始读取
func (adapter *BTreeWALAdapter) ReadFrom(ctx context.Context, startLSN txnwal.TxnLSN) (txnwal.TxnWALIterator, error) {
	// 从事务WAL读取
	return adapter.transactionWAL.ReadFrom(ctx, startLSN)
}

// GetLatestLSN 获取最新LSN
func (adapter *BTreeWALAdapter) GetLatestLSN() txnwal.TxnLSN {
	return adapter.transactionWAL.GetLatestLSN()
}

// CreateCheckpoint 创建检查点
func (adapter *BTreeWALAdapter) CreateCheckpoint(ctx context.Context) (txnwal.TxnLSN, error) {
	// 在事务WAL中创建检查点
	lsn, err := adapter.transactionWAL.CreateCheckpoint(ctx)
	if err != nil {
		return 0, err
	}

	// 同时在存储WAL中创建检查点
	adapter.storageWAL.CreateCheckpoint()

	return lsn, nil
}

// Truncate 截断WAL
func (adapter *BTreeWALAdapter) Truncate(ctx context.Context, lsn txnwal.TxnLSN) error {
	// 截断事务WAL
	return adapter.transactionWAL.Truncate(ctx, lsn)
}

// GetStatistics 获取统计信息
func (adapter *BTreeWALAdapter) GetStatistics() txnwal.TxnWALStatistics {
	// 只返回事务WAL的统计信息
	// B+树内部WAL没有GetStatistics方法
	return adapter.transactionWAL.GetStatistics()
}

// SetOptions 设置选项
func (adapter *BTreeWALAdapter) SetOptions(options *txnwal.TxnWALOptions) error {
	return adapter.transactionWAL.SetOptions(options)
}

// Close 关闭适配器
func (adapter *BTreeWALAdapter) Close() error {
	// 关闭事务WAL
	if err := adapter.transactionWAL.Close(); err != nil {
		return err
	}

	// 存储WAL由B+树管理，不在这里关闭
	return nil
}

// writeToStorageWAL 写入存储WAL
func (adapter *BTreeWALAdapter) writeToStorageWAL(record *txnwal.TxnLogRecord) error {
	// 将统一WAL记录转换为B+树内部WAL记录
	storageRecord := &LogRecord{
		LSN:       uint64(record.LSN),
		Type:      adapter.convertRecordType(record.Type),
		TxnID:     0, // B+树内部WAL使用uint64类型的TxnID
		Timestamp: record.Timestamp,
		Data:      record.Data,
	}

	_, err := adapter.storageWAL.WriteRecord(storageRecord)
	return err
}

// convertRecordType 转换记录类型
func (adapter *BTreeWALAdapter) convertRecordType(walType txnwal.TxnLogRecordType) LogRecordType {
	switch walType {
	case txnwal.LogDataInsert:
		return LogInsert
	case txnwal.LogDataUpdate:
		return LogUpdate
	case txnwal.LogDataDelete:
		return LogDelete
	case txnwal.LogPageSplit:
		return LogPageSplit
	case txnwal.LogPageMerge:
		return LogPageMerge
	case txnwal.LogTransactionCommit:
		return LogCommit
	case txnwal.LogTransactionRollback:
		return LogRollback
	default:
		return LogInsert // 默认类型
	}
}
