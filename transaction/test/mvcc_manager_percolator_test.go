package test

import (
	"testing"
	"time"

	"github.com/util6/JadeDB/common"
	"github.com/util6/JadeDB/transaction/mvcc"
)

// TestMVCCManagerPercolatorBasic 测试MVCC管理器的Percolator基本功能
func TestMVCCManagerPercolatorBasic(t *testing.T) {
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

	if !manager.IsPercolatorModeEnabled() {
		t.Fatal("Percolator mode should be enabled")
	}

	// 测试数据
	key := []byte("test_key")
	value := []byte("test_value")
	txnID := "test_txn_1"
	startTS := uint64(1000)
	commitTS := uint64(1001)

	// 注册事务
	manager.RegisterTransaction(txnID, startTS)

	// 测试预写入操作
	err = manager.PercolatorPrewrite(key, value, txnID, startTS, key, common.LockTypePut)
	if err != nil {
		t.Fatalf("PercolatorPrewrite failed: %v", err)
	}

	// 验证锁记录存在
	// 注意：这里需要访问内部结构，实际实现中应该提供公共方法

	// 测试提交操作
	err = manager.PercolatorCommit(key, txnID, startTS, commitTS)
	if err != nil {
		t.Fatalf("PercolatorCommit failed: %v", err)
	}

	// 测试读取操作
	manager.CommitTransaction(txnID, commitTS)
	readValue, err := manager.PercolatorGet(key, commitTS)
	if err != nil {
		t.Fatalf("PercolatorGet failed: %v", err)
	}

	if string(readValue) != string(value) {
		t.Errorf("Expected value %s, got %s", string(value), string(readValue))
	}
}

// TestMVCCManagerPercolatorReadWriteConflict 测试读写冲突
func TestMVCCManagerPercolatorReadWriteConflict(t *testing.T) {
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
	key := []byte("conflict_key")
	value := []byte("conflict_value")
	txnID1 := "txn_1"
	startTS1 := uint64(1000)
	startTS2 := uint64(1001)

	// 注册事务1
	manager.RegisterTransaction(txnID1, startTS1)

	// 事务1预写入
	err = manager.PercolatorPrewrite(key, value, txnID1, startTS1, key, common.LockTypePut)
	if err != nil {
		t.Fatalf("PercolatorPrewrite for txn1 failed: %v", err)
	}

	// 事务2尝试读取应该失败（因为锁存在）
	_, err = manager.PercolatorGet(key, startTS2)
	if err == nil {
		t.Fatal("PercolatorGet should fail due to lock")
	}
}

// TestMVCCManagerPercolatorWriteWriteConflict 测试写写冲突
func TestMVCCManagerPercolatorWriteWriteConflict(t *testing.T) {
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
	key := []byte("ww_conflict_key")
	value1 := []byte("value_1")
	value2 := []byte("value_2")
	txnID1 := "txn_1"
	txnID2 := "txn_2"
	startTS1 := uint64(1000)
	startTS2 := uint64(1001)
	commitTS1 := uint64(1002)

	// 注册事务1
	manager.RegisterTransaction(txnID1, startTS1)

	// 事务1预写入并提交
	err = manager.PercolatorPrewrite(key, value1, txnID1, startTS1, key, common.LockTypePut)
	if err != nil {
		t.Fatalf("PercolatorPrewrite for txn1 failed: %v", err)
	}

	err = manager.PercolatorCommit(key, txnID1, startTS1, commitTS1)
	if err != nil {
		t.Fatalf("PercolatorCommit for txn1 failed: %v", err)
	}
	manager.CommitTransaction(txnID1, commitTS1)

	// 注册事务2
	manager.RegisterTransaction(txnID2, startTS2)

	// 事务2预写入应该失败（因为写写冲突）
	err = manager.PercolatorPrewrite(key, value2, txnID2, startTS2, key, common.LockTypePut)
	if err == nil {
		t.Fatal("PercolatorPrewrite for txn2 should fail due to write-write conflict")
	}
}

// TestMVCCManagerPercolatorModeSwitch 测试Percolator模式切换
func TestMVCCManagerPercolatorModeSwitch(t *testing.T) {
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

	// 默认应该不启用Percolator模式
	if manager.IsPercolatorModeEnabled() {
		t.Fatal("Percolator mode should be disabled by default")
	}

	// 启用Percolator模式
	manager.EnablePercolatorMode()
	if !manager.IsPercolatorModeEnabled() {
		t.Fatal("Percolator mode should be enabled")
	}

	// 禁用Percolator模式
	manager.DisablePercolatorMode()
	if manager.IsPercolatorModeEnabled() {
		t.Fatal("Percolator mode should be disabled")
	}
}
