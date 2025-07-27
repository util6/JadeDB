/*
调试WAL功能测试

简单测试WAL的基本功能，用于调试问题。
*/

package transaction

import (
	"context"
	"os"
	"testing"

	"github.com/util6/JadeDB/storage"
	"github.com/util6/JadeDB/txnwal"
)

// TestWALBasicFunctionality 测试WAL基本功能
func TestWALBasicFunctionality(t *testing.T) {
	// 清理测试目录
	testDir := "./test_debug_wal"
	os.RemoveAll(testDir)
	defer os.RemoveAll(testDir)

	// 创建WAL管理器
	walOptions := txnwal.DefaultTxnWALOptions()
	walOptions.Directory = testDir
	walMgr, err := txnwal.NewFileTxnWALManager(walOptions)
	if err != nil {
		t.Fatalf("Failed to create WAL manager: %v", err)
	}
	defer walMgr.Close()

	// 写入一条记录
	record := &txnwal.TxnLogRecord{
		Type:  txnwal.LogTransactionBegin,
		TxnID: "test_txn_001",
		Data:  []byte("test data"),
	}

	ctx := context.Background()
	lsn, err := walMgr.WriteRecord(ctx, record)
	if err != nil {
		t.Fatalf("Failed to write WAL record: %v", err)
	}

	t.Logf("Written record with LSN: %d", lsn)

	// 刷新WAL
	if err := walMgr.Flush(ctx); err != nil {
		t.Fatalf("Failed to flush WAL: %v", err)
	}

	// 获取统计信息
	stats := walMgr.GetStatistics()
	t.Logf("WAL Statistics: %+v", stats)

	// 验证记录被写入
	if stats.TotalRecords == 0 {
		t.Error("Expected WAL records to be written, but got 0")
	}

	// 注意：读取功能暂未完全实现，主要测试写入功能
	t.Logf("WAL write functionality working correctly")
}

// TestTransactionManagerWAL 测试事务管理器的WAL
func TestTransactionManagerWAL(t *testing.T) {
	// 清理测试目录
	testDir := "./test_txnmgr_wal"
	os.RemoveAll(testDir)
	defer os.RemoveAll(testDir)

	// 创建事务管理器
	config := DefaultTransactionConfig()
	txnMgr, err := NewTransactionManager(config)
	if err != nil {
		t.Fatalf("Failed to create transaction manager: %v", err)
	}
	defer txnMgr.Close()

	// 检查WAL是否初始化
	if txnMgr.txnWAL == nil {
		t.Fatal("Transaction WAL is not initialized")
	}

	// 直接测试WAL写入
	record := &txnwal.TxnLogRecord{
		Type:  txnwal.LogTransactionBegin,
		TxnID: "direct_test_txn",
		Data:  []byte("direct test"),
	}

	ctx := context.Background()
	lsn, err := txnMgr.txnWAL.WriteRecord(ctx, record)
	if err != nil {
		t.Fatalf("Failed to write record to transaction WAL: %v", err)
	}

	t.Logf("Written record with LSN: %d", lsn)

	// 刷新并获取统计
	txnMgr.txnWAL.Flush(ctx)
	stats := txnMgr.txnWAL.GetStatistics()
	t.Logf("Transaction WAL Statistics: %+v", stats)

	if stats.TotalRecords == 0 {
		t.Error("Expected records in transaction WAL")
	}
}

// TestStorageAdapterWAL 测试存储适配器的WAL集成
func TestStorageAdapterWAL(t *testing.T) {
	// 清理测试目录
	testDir := "./test_adapter_wal"
	os.RemoveAll(testDir)
	defer os.RemoveAll(testDir)

	// 创建WAL管理器
	walOptions := txnwal.DefaultTxnWALOptions()
	walOptions.Directory = testDir + "/wal"
	walMgr, err := txnwal.NewFileTxnWALManager(walOptions)
	if err != nil {
		t.Fatalf("Failed to create WAL manager: %v", err)
	}
	defer walMgr.Close()

	// 创建事务管理器
	config := DefaultTransactionConfig()
	txnMgr, err := NewTransactionManager(config)
	if err != nil {
		t.Fatalf("Failed to create transaction manager: %v", err)
	}
	defer txnMgr.Close()

	// 创建内存存储引擎（简化测试）
	memEngine := &MockStorageEngine{}

	// 创建存储适配器
	adapter, err := NewStorageTransactionAdapter(
		"test_adapter_txn",
		memEngine,
		txnMgr,
		walMgr,
		nil,
	)
	if err != nil {
		t.Fatalf("Failed to create storage adapter: %v", err)
	}
	defer adapter.Close()

	// 执行一些操作
	key := []byte("test_key")
	value := []byte("test_value")
	if err := adapter.Put(key, value); err != nil {
		t.Fatalf("Failed to put key-value: %v", err)
	}

	// 提交事务
	if err := adapter.Commit(); err != nil {
		t.Fatalf("Failed to commit transaction: %v", err)
	}

	// 检查WAL统计
	stats := walMgr.GetStatistics()
	t.Logf("Adapter WAL Statistics: %+v", stats)

	if stats.TotalRecords == 0 {
		t.Error("Expected WAL records from adapter operations")
	}
}

// MockStorageEngine 模拟存储引擎
type MockStorageEngine struct{}

func (m *MockStorageEngine) Put(key, value []byte) error              { return nil }
func (m *MockStorageEngine) Get(key []byte) ([]byte, error)           { return nil, nil }
func (m *MockStorageEngine) Delete(key []byte) error                  { return nil }
func (m *MockStorageEngine) Exists(key []byte) (bool, error)          { return false, nil }
func (m *MockStorageEngine) BatchPut(batch []storage.KVPair) error    { return nil }
func (m *MockStorageEngine) BatchGet(keys [][]byte) ([][]byte, error) { return nil, nil }
func (m *MockStorageEngine) BatchDelete(keys [][]byte) error          { return nil }
func (m *MockStorageEngine) Scan(startKey, endKey []byte, limit int) ([]storage.KVPair, error) {
	return nil, nil
}
func (m *MockStorageEngine) NewIterator(options *storage.IteratorOptions) (storage.Iterator, error) {
	return nil, nil
}
func (m *MockStorageEngine) GetEngineType() storage.EngineType {
	return storage.HashTableEngine // 使用哈希表类型作为模拟
}

func (m *MockStorageEngine) GetEngineInfo() *storage.EngineInfo {
	return &storage.EngineInfo{
		Type:        storage.HashTableEngine,
		Version:     "1.0.0",
		Description: "Mock storage engine for testing",
	}
}
func (m *MockStorageEngine) GetStats() *storage.EngineStats {
	return &storage.EngineStats{
		KeyCount:       0,
		DataSize:       0,
		IndexSize:      0,
		EngineSpecific: make(map[string]interface{}),
	}
}
func (m *MockStorageEngine) Open() error                           { return nil }
func (m *MockStorageEngine) Sync() error                           { return nil }
func (m *MockStorageEngine) Close() error                          { return nil }
func (m *MockStorageEngine) GetStatistics() map[string]interface{} { return nil }
