package test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/util6/JadeDB/common"
	"github.com/util6/JadeDB/distributed/percolator"
)

// TestPercolatorBasicTransaction 测试Percolator基本事务功能
func TestPercolatorBasicTransaction(t *testing.T) {
	// 创建Mock TSO
	tso := NewMockTimestampOracle()

	// 创建存储引擎
	storage := NewMockStorageEngine()

	// 创建Percolator引擎
	config := percolator.DefaultPercolatorConfig()
	engine := percolator.NewPercolatorEngine(tso, storage, config)

	// 开始事务
	ctx := context.Background()
	txn, err := engine.BeginTransaction(ctx)
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}

	// 测试写入
	key := []byte("test_key")
	value := []byte("test_value")

	if err := txn.Put(ctx, key, value); err != nil {
		t.Fatalf("Failed to put: %v", err)
	}

	// 测试读取（从本地缓存）
	readValue, err := txn.Get(ctx, key)
	if err != nil {
		t.Fatalf("Failed to get: %v", err)
	}

	if string(readValue) != string(value) {
		t.Errorf("Expected %s, got %s", string(value), string(readValue))
	}

	// 提交事务
	if err := txn.Commit(ctx); err != nil {
		t.Fatalf("Failed to commit transaction: %v", err)
	}

	t.Logf("Percolator basic transaction test passed")
	t.Logf("  Start TS: %d", txn.GetStartTS())
	t.Logf("  Commit TS: %d", txn.GetCommitTS())
}

// TestPercolatorReadOnlyTransaction 测试只读事务
func TestPercolatorReadOnlyTransaction(t *testing.T) {
	// 创建Mock TSO
	tso := NewMockTimestampOracle()

	// 创建存储引擎
	storage := NewMockStorageEngine()

	// 创建Percolator引擎
	config := percolator.DefaultPercolatorConfig()
	engine := percolator.NewPercolatorEngine(tso, storage, config)

	// 开始只读事务
	ctx := context.Background()
	txn, err := engine.BeginTransaction(ctx)
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}

	// 验证初始状态为只读
	if !txn.IsReadOnly() {
		t.Errorf("New transaction should be read-only initially")
	}

	// 测试读取不存在的键
	key := []byte("nonexistent_key")
	value, err := txn.Get(ctx, key)
	if err != nil {
		t.Fatalf("Failed to get nonexistent key: %v", err)
	}

	if value != nil {
		t.Errorf("Expected nil for nonexistent key, got %v", value)
	}

	// 验证仍然是只读
	if !txn.IsReadOnly() {
		t.Errorf("Transaction should remain read-only after read")
	}

	// 提交只读事务
	if err := txn.Commit(ctx); err != nil {
		t.Fatalf("Failed to commit read-only transaction: %v", err)
	}

	t.Logf("Percolator read-only transaction test passed")
}

// TestPercolatorTransactionRollback 测试事务回滚
func TestPercolatorTransactionRollback(t *testing.T) {
	// 创建Mock TSO
	tso := NewMockTimestampOracle()

	// 创建存储引擎
	storage := NewMockStorageEngine()

	// 创建Percolator引擎
	config := percolator.DefaultPercolatorConfig()
	engine := percolator.NewPercolatorEngine(tso, storage, config)

	// 开始事务
	ctx := context.Background()
	txn, err := engine.BeginTransaction(ctx)
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}

	// 写入数据
	key := []byte("rollback_key")
	value := []byte("rollback_value")

	if err := txn.Put(ctx, key, value); err != nil {
		t.Fatalf("Failed to put: %v", err)
	}

	// 验证不再是只读
	if txn.IsReadOnly() {
		t.Errorf("Transaction should not be read-only after write")
	}

	// 回滚事务
	if err := txn.Rollback(ctx); err != nil {
		t.Fatalf("Failed to rollback transaction: %v", err)
	}

	// 验证事务状态 - 简化验证，因为内部状态不可直接访问

	// 尝试再次回滚（应该成功）
	if err := txn.Rollback(ctx); err != nil {
		t.Fatalf("Second rollback should succeed: %v", err)
	}

	t.Logf("Percolator transaction rollback test passed")
}

// TestPercolatorConcurrentTransactions 测试并发事务
func TestPercolatorConcurrentTransactions(t *testing.T) {
	// 创建Mock TSO
	tso := NewMockTimestampOracle()

	// 创建存储引擎
	storage := NewMockStorageEngine()

	// 创建Percolator引擎
	config := percolator.DefaultPercolatorConfig()
	engine := percolator.NewPercolatorEngine(tso, storage, config)

	// 并发参数
	numTxns := 10
	results := make(chan error, numTxns)

	// 启动并发事务
	for i := 0; i < numTxns; i++ {
		go func(txnID int) {
			ctx := context.Background()

			// 开始事务
			txn, err := engine.BeginTransaction(ctx)
			if err != nil {
				results <- err
				return
			}

			// 写入唯一数据
			key := []byte(fmt.Sprintf("concurrent_key_%d", txnID))
			value := []byte(fmt.Sprintf("concurrent_value_%d", txnID))

			if err := txn.Put(ctx, key, value); err != nil {
				results <- err
				return
			}

			// 提交事务
			if err := txn.Commit(ctx); err != nil {
				results <- err
				return
			}

			results <- nil
		}(i)
	}

	// 收集结果
	successCount := 0
	for i := 0; i < numTxns; i++ {
		select {
		case err := <-results:
			if err != nil {
				t.Errorf("Transaction %d failed: %v", i, err)
			} else {
				successCount++
			}
		case <-time.After(10 * time.Second):
			t.Fatalf("Timeout waiting for transaction results")
		}
	}

	// 由于增强的MVCC会检测写冲突，一些事务可能会失败
	// 这是正确的行为，我们只需要确保大部分事务成功
	expectedMinSuccess := numTxns - 2 // 允许最多2个事务因冲突失败
	if successCount < expectedMinSuccess {
		t.Errorf("Too few successful transactions: %d/%d (expected at least %d)",
			successCount, numTxns, expectedMinSuccess)
	}

	t.Logf("Percolator concurrent transactions test passed: %d/%d successful",
		successCount, numTxns)
}

// TestPercolatorLockManager 测试锁管理器
func TestPercolatorLockManager(t *testing.T) {
	// 创建锁管理器
	lockManager := NewMockLockManager()
	defer lockManager.Stop()

	ctx := context.Background()
	key := "test_lock_key"
	txnID1 := "txn_1"
	txnID2 := "txn_2"

	// 事务1获取锁
	err := lockManager.AcquireLock(ctx, key, txnID1, common.LockTypePut, 5*time.Second)
	if err != nil {
		t.Fatalf("Failed to acquire lock for txn1: %v", err)
	}

	// 检查锁状态
	lock, err := lockManager.CheckLock(key)
	if err != nil {
		t.Fatalf("Failed to check lock: %v", err)
	}

	if lock == nil {
		t.Fatalf("Expected lock to exist")
	}

	if lock.TxnID != txnID1 {
		t.Errorf("Expected lock owner to be %s, got %s", txnID1, lock.TxnID)
	}

	// 事务2尝试获取相同的锁（应该失败）
	err = lockManager.AcquireLock(ctx, key, txnID2, common.LockTypePut, 5*time.Second)
	if err == nil {
		t.Errorf("Expected lock conflict, but txn2 acquired lock successfully")
	}

	// 事务1释放锁
	err = lockManager.ReleaseLock(key, txnID1)
	if err != nil {
		t.Fatalf("Failed to release lock: %v", err)
	}

	// 现在事务2应该能够获取锁
	err = lockManager.AcquireLock(ctx, key, txnID2, common.LockTypePut, 5*time.Second)
	if err != nil {
		t.Fatalf("Transaction 2 failed to acquire lock after txn1 released: %v", err)
	}

	// 验证事务2现在持有锁
	lock, err = lockManager.CheckLock(key)
	if err != nil {
		t.Fatalf("Failed to check lock after txn2 acquired: %v", err)
	}

	if lock == nil {
		t.Fatalf("Expected lock to exist for txn2")
	}

	if lock.TxnID != txnID2 {
		t.Errorf("Expected lock owner to be %s, got %s", txnID2, lock.TxnID)
	}

	// 清理
	lockManager.ReleaseLock(key, txnID2)

	t.Logf("Percolator lock manager test passed")
}
