package test

import (
	"testing"
	"time"

	"github.com/util6/JadeDB/common"
	"github.com/util6/JadeDB/transaction/mvcc"
)

// TestPercolatorIntegrationWithStorageAdapter 测试Percolator与存储适配器的集成
func TestPercolatorIntegrationWithStorageAdapter(t *testing.T) {
	// 创建事务配置
	config := &common.TransactionConfig{
		MaxVersions: 100,
		GCInterval:  time.Minute,
	}

	// 创建MVCC管理器
	manager, err := mvcc.NewMVCCManager(config)
	if err != nil {
		t.Fatalf("Failed to create MVCC manager: %v", err)
	}
	defer manager.Close()

	// 启用Percolator模式
	manager.EnablePercolatorMode()

	// 测试数据
	key := []byte("integration_test_key")
	value := []byte("integration_test_value")
	txnID := "integration_test_txn"
	startTS := uint64(2000)
	commitTS := uint64(2001)

	// 注册事务
	manager.RegisterTransaction(txnID, startTS)

	// 使用传统API测试Percolator模式
	err = manager.Put(key, value, txnID, startTS)
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	// 提交事务
	err = manager.PercolatorCommit(key, txnID, startTS, commitTS)
	if err != nil {
		t.Fatalf("PercolatorCommit failed: %v", err)
	}

	manager.CommitTransaction(txnID, commitTS)

	// 使用传统API读取
	readValue, err := manager.Get(key, txnID, commitTS)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	if string(readValue) != string(value) {
		t.Errorf("Expected value %s, got %s", string(value), string(readValue))
	}
}

// TestPercolatorMetrics 测试Percolator指标收集
func TestPercolatorMetrics(t *testing.T) {
	// 创建事务配置
	config := &common.TransactionConfig{
		MaxVersions: 100,
		GCInterval:  time.Minute,
	}

	// 创建MVCC管理器
	manager, err := mvcc.NewMVCCManager(config)
	if err != nil {
		t.Fatalf("Failed to create MVCC manager: %v", err)
	}
	defer manager.Close()

	// 启用Percolator模式
	manager.EnablePercolatorMode()

	// 初始指标应该为0
	metrics := manager.GetPercolatorMetrics()
	if metrics["lock_records"] != int64(0) {
		t.Errorf("Expected 0 lock records, got %v", metrics["lock_records"])
	}

	// 执行一些操作
	key := []byte("metrics_test_key")
	value := []byte("metrics_test_value")
	txnID := "metrics_test_txn"
	startTS := uint64(3000)
	commitTS := uint64(3001)

	manager.RegisterTransaction(txnID, startTS)

	// 预写入
	err = manager.PercolatorPrewrite(key, value, txnID, startTS, key, common.LockTypePut)
	if err != nil {
		t.Fatalf("PercolatorPrewrite failed: %v", err)
	}

	// 检查指标
	metrics = manager.GetPercolatorMetrics()
	if metrics["lock_records"] != int64(1) {
		t.Errorf("Expected 1 lock record, got %v", metrics["lock_records"])
	}
	if metrics["data_records"] != int64(1) {
		t.Errorf("Expected 1 data record, got %v", metrics["data_records"])
	}
	if metrics["prewrite_ops"] != int64(1) {
		t.Errorf("Expected 1 prewrite op, got %v", metrics["prewrite_ops"])
	}

	// 提交
	err = manager.PercolatorCommit(key, txnID, startTS, commitTS)
	if err != nil {
		t.Fatalf("PercolatorCommit failed: %v", err)
	}

	// 检查指标
	metrics = manager.GetPercolatorMetrics()
	if metrics["lock_records"] != int64(0) {
		t.Errorf("Expected 0 lock records after commit, got %v", metrics["lock_records"])
	}
	if metrics["write_records"] != int64(1) {
		t.Errorf("Expected 1 write record after commit, got %v", metrics["write_records"])
	}

	t.Logf("Percolator integration test passed")
}
