package test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/util6/JadeDB/common"
	"github.com/util6/JadeDB/distributed/locks"
)

// TestDistributedLockManagerBasicOperations 测试分布式锁管理器基本操作
func TestDistributedLockManagerBasicOperations(t *testing.T) {
	// 创建存储引擎
	storage := NewMockStorageEngine()

	// 创建分布式锁管理器
	nodeID := "test_node_1"
	config := locks.DefaultDistributedLockConfig(nodeID)
	config.EnableReplication = false // 简化测试
	config.EnablePersistence = false

	dlm := locks.NewDistributedLockManager(nodeID, storage, config)
	defer dlm.Stop()

	ctx := context.Background()
	key := []byte("test_key")
	txnID := "test_txn_1"
	primaryKey := key
	startTS := uint64(1000)

	// 测试获取锁
	err := dlm.AcquireDistributedLock(ctx, key, txnID, common.LockTypePut, primaryKey, startTS)
	if err != nil {
		t.Fatalf("Failed to acquire distributed lock: %v", err)
	}

	// 验证锁存在
	lockInfo, err := dlm.CheckDistributedLock(key)
	if err != nil {
		t.Fatalf("Failed to check distributed lock: %v", err)
	}
	if lockInfo == nil {
		t.Fatalf("Lock should exist")
	}
	if lockInfo.TxnID != txnID {
		t.Errorf("Expected lock TxnID %s, got %s", txnID, lockInfo.TxnID)
	}
	if lockInfo.NodeID != nodeID {
		t.Errorf("Expected lock NodeID %s, got %s", nodeID, lockInfo.NodeID)
	}

	// 测试释放锁
	err = dlm.ReleaseDistributedLock(key, txnID)
	if err != nil {
		t.Fatalf("Failed to release distributed lock: %v", err)
	}

	// 验证锁已释放
	lockInfo, err = dlm.CheckDistributedLock(key)
	if err != nil {
		t.Fatalf("Failed to check distributed lock after release: %v", err)
	}
	if lockInfo != nil {
		t.Errorf("Lock should be released")
	}

	t.Logf("Distributed lock manager basic operations test passed")
}

// TestDistributedLockManagerSharding 测试分片功能
func TestDistributedLockManagerSharding(t *testing.T) {
	// 创建存储引擎
	storage := NewMockStorageEngine()

	// 创建分布式锁管理器
	nodeID := "test_node_sharding"
	config := locks.DefaultDistributedLockConfig(nodeID)
	config.ShardCount = 4
	config.EnableReplication = false
	config.EnablePersistence = false

	dlm := locks.NewDistributedLockManager(nodeID, storage, config)
	defer dlm.Stop()

	ctx := context.Background()

	// 测试多个键的分片分布
	keys := [][]byte{
		[]byte("key_1"),
		[]byte("key_2"),
		[]byte("key_3"),
		[]byte("key_4"),
		[]byte("key_5"),
	}

	shardDistribution := make(map[string]int)

	for i, key := range keys {
		txnID := fmt.Sprintf("txn_%d", i)

		// 获取锁
		err := dlm.AcquireDistributedLock(ctx, key, txnID, common.LockTypePut, key, uint64(1000+i))
		if err != nil {
			t.Fatalf("Failed to acquire lock for key %s: %v", string(key), err)
		}

		// 检查分片分布
		shard := dlm.GetShardForKey(key)
		shardDistribution[shard.GetShardID()]++
	}

	// 验证锁分布在不同分片中
	if len(shardDistribution) < 2 {
		t.Errorf("Expected locks to be distributed across multiple shards, got %d shards", len(shardDistribution))
	}

	t.Logf("Shard distribution: %v", shardDistribution)
	t.Logf("Distributed lock manager sharding test passed")
}

// TestDistributedLockManagerConflict 测试锁冲突
func TestDistributedLockManagerConflict(t *testing.T) {
	// 创建存储引擎
	storage := NewMockStorageEngine()

	// 创建分布式锁管理器
	nodeID := "test_node_conflict"
	config := locks.DefaultDistributedLockConfig(nodeID)
	config.EnableReplication = false
	config.EnablePersistence = false

	dlm := locks.NewDistributedLockManager(nodeID, storage, config)
	defer dlm.Stop()

	ctx := context.Background()
	key := []byte("conflict_key")
	txnID1 := "txn_1"
	txnID2 := "txn_2"

	// 事务1获取锁
	err := dlm.AcquireDistributedLock(ctx, key, txnID1, common.LockTypePut, key, 1000)
	if err != nil {
		t.Fatalf("Failed to acquire lock for txn1: %v", err)
	}

	// 事务2尝试获取相同的锁，应该失败
	err = dlm.AcquireDistributedLock(ctx, key, txnID2, common.LockTypePut, key, 1001)
	if err == nil {
		t.Fatalf("Expected lock conflict, but txn2 acquired lock successfully")
	}

	t.Logf("Lock conflict detected correctly: %v", err)

	// 事务1释放锁
	err = dlm.ReleaseDistributedLock(key, txnID1)
	if err != nil {
		t.Fatalf("Failed to release lock for txn1: %v", err)
	}

	// 现在事务2应该能够获取锁
	err = dlm.AcquireDistributedLock(ctx, key, txnID2, common.LockTypePut, key, 1001)
	if err != nil {
		t.Fatalf("Failed to acquire lock for txn2 after txn1 released: %v", err)
	}

	t.Logf("Distributed lock manager conflict test passed")
}

// TestDistributedLockManagerTransfer 测试锁转移
func TestDistributedLockManagerTransfer(t *testing.T) {
	// 创建存储引擎
	storage := NewMockStorageEngine()

	// 创建分布式锁管理器
	nodeID := "test_node_transfer"
	config := locks.DefaultDistributedLockConfig(nodeID)
	config.EnableReplication = false
	config.EnablePersistence = false

	dlm := locks.NewDistributedLockManager(nodeID, storage, config)
	defer dlm.Stop()

	ctx := context.Background()
	key := []byte("transfer_key")
	txnID1 := "txn_1"
	txnID2 := "txn_2"

	// 事务1获取锁
	err := dlm.AcquireDistributedLock(ctx, key, txnID1, common.LockTypePut, key, 1000)
	if err != nil {
		t.Fatalf("Failed to acquire lock for txn1: %v", err)
	}

	// 验证锁属于事务1
	lockInfo, err := dlm.CheckDistributedLock(key)
	if err != nil {
		t.Fatalf("Failed to check lock: %v", err)
	}
	if lockInfo.TxnID != txnID1 {
		t.Errorf("Expected lock owner %s, got %s", txnID1, lockInfo.TxnID)
	}

	// 转移锁到事务2
	err = dlm.TransferLock(key, txnID1, txnID2)
	if err != nil {
		t.Fatalf("Failed to transfer lock: %v", err)
	}

	// 验证锁现在属于事务2
	lockInfo, err = dlm.CheckDistributedLock(key)
	if err != nil {
		t.Fatalf("Failed to check lock after transfer: %v", err)
	}
	if lockInfo.TxnID != txnID2 {
		t.Errorf("Expected lock owner %s after transfer, got %s", txnID2, lockInfo.TxnID)
	}
	if lockInfo.Status != locks.LockStatusTransferred {
		t.Errorf("Expected lock status %v, got %v", locks.LockStatusTransferred, lockInfo.Status)
	}

	t.Logf("Distributed lock manager transfer test passed")
}

// TestDistributedLockManagerExpiration 测试锁过期
func TestDistributedLockManagerExpiration(t *testing.T) {
	// 创建存储引擎
	storage := NewMockStorageEngine()

	// 创建分布式锁管理器，设置很短的TTL
	nodeID := "test_node_expiration"
	config := locks.DefaultDistributedLockConfig(nodeID)
	config.LockTTL = 100 * time.Millisecond
	config.HeartbeatInterval = 1 * time.Hour // 禁用心跳服务
	config.EnableReplication = false
	config.EnablePersistence = false

	dlm := locks.NewDistributedLockManager(nodeID, storage, config)
	defer dlm.Stop()

	ctx := context.Background()
	key := []byte("expiration_key")
	txnID := "expiration_txn"

	// 获取锁
	err := dlm.AcquireDistributedLock(ctx, key, txnID, common.LockTypePut, key, 1000)
	if err != nil {
		t.Fatalf("Failed to acquire lock: %v", err)
	}

	// 验证锁存在
	lockInfo, err := dlm.CheckDistributedLock(key)
	if err != nil {
		t.Fatalf("Failed to check lock: %v", err)
	}
	if lockInfo == nil {
		t.Fatalf("Lock should exist")
	}

	// 等待锁过期，给足够的时间
	time.Sleep(300 * time.Millisecond)

	// 验证锁已过期
	lockInfo, err = dlm.CheckDistributedLock(key)
	if err != nil {
		t.Fatalf("Failed to check lock after expiration: %v", err)
	}
	if lockInfo != nil {
		t.Errorf("Lock should be expired, but still exists: %+v", lockInfo)
		t.Errorf("Current time: %v, Lock expires at: %v", time.Now(), lockInfo.ExpiresAt)
	}

	t.Logf("Distributed lock manager expiration test passed")
}

// TestDistributedLockManagerMetrics 测试性能指标
func TestDistributedLockManagerMetrics(t *testing.T) {
	// 创建存储引擎
	storage := NewMockStorageEngine()

	// 创建分布式锁管理器
	nodeID := "test_node_metrics"
	config := locks.DefaultDistributedLockConfig(nodeID)
	config.EnableReplication = false
	config.EnablePersistence = false

	dlm := locks.NewDistributedLockManager(nodeID, storage, config)
	defer dlm.Stop()

	ctx := context.Background()
	key := []byte("metrics_key")
	txnID := "metrics_txn"

	// 执行一些操作
	dlm.AcquireDistributedLock(ctx, key, txnID, common.LockTypePut, key, 1000)
	dlm.CheckDistributedLock(key)
	dlm.ReleaseDistributedLock(key, txnID)

	// 检查指标
	metrics := dlm.GetMetrics()
	if metrics.AcquireRequests == 0 {
		t.Errorf("Expected acquire requests > 0")
	}
	if metrics.ReleaseRequests == 0 {
		t.Errorf("Expected release requests > 0")
	}

	t.Logf("Distributed lock manager metrics test passed")
	t.Logf("  Acquire requests: %d", metrics.AcquireRequests)
	t.Logf("  Release requests: %d", metrics.ReleaseRequests)
	t.Logf("  Transfer requests: %d", metrics.TransferRequests)
}
