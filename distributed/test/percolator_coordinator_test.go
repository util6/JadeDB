package test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/util6/JadeDB/common"
	"github.com/util6/JadeDB/distributed/percolator"
)

// TestPercolatorCoordinatorBasicOperations 测试Percolator协调器基本操作
func TestPercolatorCoordinatorBasicOperations(t *testing.T) {
	// 创建Mock TSO
	tso := NewMockTimestampOracle()

	// 创建存储引擎
	storage := NewMockStorageEngine()

	// 创建真实的锁管理器用于测试
	lockManager := NewMockDistributedLockManager()
	defer lockManager.Stop()

	// 创建MVCC
	mvcc := percolator.NewPercolatorMVCC(storage, lockManager, nil)

	// 创建Percolator协调器
	config := percolator.DefaultPercolatorCoordinatorConfig("test_coordinator")
	coordinator := percolator.NewPercolatorCoordinator("test_coordinator", tso, storage, mvcc, lockManager, config)
	defer coordinator.Stop()

	ctx := context.Background()

	// 开始事务
	txn, err := coordinator.BeginTransaction(ctx)
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}

	// 验证事务属性
	if txn.TxnID == "" {
		t.Errorf("Transaction ID should not be empty")
	}
	if txn.StartTS == 0 {
		t.Errorf("Start timestamp should not be zero")
	}
	if txn.State != common.TxnActive {
		t.Errorf("Expected transaction state %v, got %v", common.TxnActive, txn.State)
	}

	// 写入数据
	key := []byte("test_key")
	value := []byte("test_value")

	err = txn.Put(ctx, key, value)
	if err != nil {
		t.Fatalf("Failed to put data: %v", err)
	}

	// 验证主键设置
	if string(txn.PrimaryKey) != string(key) {
		t.Errorf("Expected primary key %s, got %s", string(key), string(txn.PrimaryKey))
	}

	// 读取数据
	readValue, err := txn.Get(ctx, key)
	if err != nil {
		t.Fatalf("Failed to get data: %v", err)
	}
	if string(readValue) != string(value) {
		t.Errorf("Expected value %s, got %s", string(value), string(readValue))
	}

	// 提交事务
	err = txn.Commit(ctx)
	if err != nil {
		t.Fatalf("Failed to commit transaction: %v", err)
	}

	// 验证事务状态
	if txn.State != common.TxnCommitted {
		t.Errorf("Expected transaction state %v, got %v", common.TxnCommitted, txn.State)
	}

	t.Logf("Percolator coordinator basic operations test passed")
	t.Logf("  Transaction ID: %s", txn.TxnID)
	t.Logf("  Start TS: %d", txn.StartTS)
	t.Logf("  Commit TS: %d", txn.CommitTS)
}

// TestPercolatorCoordinatorReadOnlyTransaction 测试只读事务
func TestPercolatorCoordinatorReadOnlyTransaction(t *testing.T) {
	// 创建Mock TSO
	tso := NewMockTimestampOracle()

	// 创建存储引擎
	storage := NewMockStorageEngine()

	// 创建锁管理器
	lockManager := NewMockDistributedLockManager()
	defer lockManager.Stop()

	// 创建MVCC
	mvcc := percolator.NewPercolatorMVCC(storage, lockManager, nil)

	// 创建Percolator协调器
	config := percolator.DefaultPercolatorCoordinatorConfig("test_coordinator")
	coordinator := percolator.NewPercolatorCoordinator("test_coordinator", tso, storage, mvcc, lockManager, config)
	defer coordinator.Stop()

	ctx := context.Background()

	// 开始只读事务
	txn, err := coordinator.BeginTransaction(ctx)
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}

	// 读取不存在的键
	key := []byte("nonexistent_key")
	value, err := txn.Get(ctx, key)
	if err != nil {
		t.Fatalf("Failed to get nonexistent key: %v", err)
	}
	if value != nil {
		t.Errorf("Expected nil for nonexistent key, got %v", value)
	}

	// 提交只读事务
	err = txn.Commit(ctx)
	if err != nil {
		t.Fatalf("Failed to commit read-only transaction: %v", err)
	}

	// 验证事务状态
	if txn.State != common.TxnCommitted {
		t.Errorf("Expected transaction state %v, got %v", common.TxnCommitted, txn.State)
	}

	t.Logf("Percolator coordinator read-only transaction test passed")
}

// TestPercolatorCoordinatorTransactionRollback 测试事务回滚
func TestPercolatorCoordinatorTransactionRollback(t *testing.T) {
	// 创建Mock TSO
	tso := NewMockTimestampOracle()

	// 创建存储引擎
	storage := NewMockStorageEngine()

	// 创建锁管理器
	lockManager := NewMockDistributedLockManager()
	defer lockManager.Stop()

	// 创建MVCC
	mvcc := percolator.NewPercolatorMVCC(storage, lockManager, nil)

	// 创建Percolator协调器
	config := percolator.DefaultPercolatorCoordinatorConfig("test_coordinator")
	coordinator := percolator.NewPercolatorCoordinator("test_coordinator", tso, storage, mvcc, lockManager, config)
	defer coordinator.Stop()

	ctx := context.Background()

	// 开始事务
	txn, err := coordinator.BeginTransaction(ctx)
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}

	// 写入数据
	key := []byte("rollback_key")
	value := []byte("rollback_value")

	err = txn.Put(ctx, key, value)
	if err != nil {
		t.Fatalf("Failed to put data: %v", err)
	}

	// 回滚事务
	err = txn.Rollback(ctx)
	if err != nil {
		t.Fatalf("Failed to rollback transaction: %v", err)
	}

	// 验证事务状态
	if txn.State != common.TxnAborted {
		t.Errorf("Expected transaction state %v, got %v", common.TxnAborted, txn.State)
	}

	t.Logf("Percolator coordinator transaction rollback test passed")
}

// TestPercolatorCoordinatorConcurrentTransactions 测试并发事务
func TestPercolatorCoordinatorConcurrentTransactions(t *testing.T) {
	// 创建Mock TSO
	tso := NewMockTimestampOracle()

	// 创建存储引擎
	storage := NewMockStorageEngine()

	// 创建锁管理器
	lockManager := NewMockDistributedLockManager()
	defer lockManager.Stop()

	// 创建MVCC
	mvcc := percolator.NewPercolatorMVCC(storage, lockManager, nil)

	// 创建Percolator协调器
	config := percolator.DefaultPercolatorCoordinatorConfig("test_coordinator")
	coordinator := percolator.NewPercolatorCoordinator("test_coordinator", tso, storage, mvcc, lockManager, config)
	defer coordinator.Stop()

	ctx := context.Background()
	numTxns := 5
	results := make(chan error, numTxns)

	// 启动并发事务
	for i := 0; i < numTxns; i++ {
		go func(txnIndex int) {
			// 开始事务
			txn, err := coordinator.BeginTransaction(ctx)
			if err != nil {
				results <- err
				return
			}

			// 写入唯一数据
			key := []byte(fmt.Sprintf("concurrent_key_%d", txnIndex))
			value := []byte(fmt.Sprintf("concurrent_value_%d", txnIndex))

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

	// 允许一些事务因冲突失败
	expectedMinSuccess := numTxns - 1
	if successCount < expectedMinSuccess {
		t.Errorf("Too few successful transactions: %d/%d (expected at least %d)",
			successCount, numTxns, expectedMinSuccess)
	}

	t.Logf("Percolator coordinator concurrent transactions test passed: %d/%d successful",
		successCount, numTxns)
}

// TestPercolatorCoordinatorMetrics 测试性能指标
func TestPercolatorCoordinatorMetrics(t *testing.T) {
	// 创建Mock TSO
	tso := NewMockTimestampOracle()

	// 创建存储引擎
	storage := NewMockStorageEngine()

	// 创建锁管理器
	lockManager := NewMockDistributedLockManager()
	defer lockManager.Stop()

	// 创建MVCC
	mvcc := percolator.NewPercolatorMVCC(storage, lockManager, nil)

	// 创建Percolator协调器
	config := percolator.DefaultPercolatorCoordinatorConfig("test_coordinator")
	coordinator := percolator.NewPercolatorCoordinator("test_coordinator", tso, storage, mvcc, lockManager, config)
	defer coordinator.Stop()

	ctx := context.Background()

	// 执行一些事务
	for i := 0; i < 3; i++ {
		txn, err := coordinator.BeginTransaction(ctx)
		if err != nil {
			t.Fatalf("Failed to begin transaction %d: %v", i, err)
		}

		key := []byte(fmt.Sprintf("metrics_key_%d", i))
		value := []byte(fmt.Sprintf("metrics_value_%d", i))

		txn.Put(ctx, key, value)
		txn.Commit(ctx)
	}

	// 检查指标
	metrics := coordinator.GetMetrics()
	if metrics.TotalTransactions == 0 {
		t.Errorf("Expected total transactions > 0")
	}
	if metrics.CommittedTransactions == 0 {
		t.Errorf("Expected committed transactions > 0")
	}
	if metrics.PrimaryKeyTransactions == 0 {
		t.Errorf("Expected primary key transactions > 0")
	}

	t.Logf("Percolator coordinator metrics test passed")
	t.Logf("  Total transactions: %d", metrics.TotalTransactions)
	t.Logf("  Committed transactions: %d", metrics.CommittedTransactions)
	t.Logf("  Primary key transactions: %d", metrics.PrimaryKeyTransactions)
	t.Logf("  Active transactions: %d", metrics.ActiveTransactions)
}
