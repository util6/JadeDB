package test

import (
	"context"
	"testing"

	"github.com/util6/JadeDB/common"
	"github.com/util6/JadeDB/distributed/percolator"
)

// TestPercolatorMVCCBasicOperations 测试Percolator MVCC基本操作
func TestPercolatorMVCCBasicOperations(t *testing.T) {
	// 创建存储引擎和锁管理器
	storage := NewMockStorageEngine()
	lockManager := NewMockDistributedLockManager()
	defer lockManager.Stop()

	// 创建Percolator MVCC
	config := percolator.DefaultPercolatorMVCCConfig()
	pmvcc := percolator.NewPercolatorMVCC(storage, lockManager, config)

	ctx := context.Background()
	key := []byte("test_key")
	value := []byte("test_value")
	txnID := "test_txn_1"
	startTS := uint64(1000)
	commitTS := uint64(1001)
	primaryKey := key

	// 测试预写操作
	mutations := []*common.Mutation{
		{
			Type:  common.MutationPut,
			Key:   key,
			Value: value,
		},
	}

	err := pmvcc.Prewrite(ctx, mutations, primaryKey, startTS, txnID)
	if err != nil {
		t.Fatalf("Prewrite failed: %v", err)
	}

	// 测试提交操作
	keys := [][]byte{key}
	err = pmvcc.Commit(ctx, keys, startTS, commitTS, txnID)
	if err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	// 测试读取操作
	readValue, err := pmvcc.Get(ctx, key, commitTS+1)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if string(readValue) != string(value) {
		t.Errorf("Expected value %s, got %s", string(value), string(readValue))
	}

	t.Logf("Percolator MVCC basic operations test passed")
}

// TestPercolatorMVCCWriteConflict 测试写写冲突检测
func TestPercolatorMVCCWriteConflict(t *testing.T) {
	// 创建存储引擎和锁管理器
	storage := NewMockStorageEngine()
	lockManager := NewMockDistributedLockManager()
	defer lockManager.Stop()

	// 创建Percolator MVCC
	config := percolator.DefaultPercolatorMVCCConfig()
	pmvcc := percolator.NewPercolatorMVCC(storage, lockManager, config)

	ctx := context.Background()
	key := []byte("conflict_key")
	value1 := []byte("value1")
	value2 := []byte("value2")

	// 事务1：先提交一个写入
	txnID1 := "txn_1"
	startTS1 := uint64(1000)
	commitTS1 := uint64(1001)

	mutations1 := []*common.Mutation{
		{
			Type:  common.MutationPut,
			Key:   key,
			Value: value1,
		},
	}

	err := pmvcc.Prewrite(ctx, mutations1, key, startTS1, txnID1)
	if err != nil {
		t.Fatalf("Txn1 prewrite failed: %v", err)
	}

	err = pmvcc.Commit(ctx, [][]byte{key}, startTS1, commitTS1, txnID1)
	if err != nil {
		t.Fatalf("Txn1 commit failed: %v", err)
	}

	// 事务2：尝试写入相同的键，但开始时间戳更早
	txnID2 := "txn_2"
	startTS2 := uint64(999) // 比txn1的开始时间戳更早

	mutations2 := []*common.Mutation{
		{
			Type:  common.MutationPut,
			Key:   key,
			Value: value2,
		},
	}

	// 应该检测到写写冲突
	err = pmvcc.Prewrite(ctx, mutations2, key, startTS2, txnID2)
	if err == nil {
		t.Fatalf("Expected write conflict, but prewrite succeeded")
	}

	t.Logf("Write conflict detected correctly: %v", err)
	t.Logf("Percolator MVCC write conflict test passed")
}

// TestPercolatorMVCCLockConflict 测试锁冲突检测
func TestPercolatorMVCCLockConflict(t *testing.T) {
	// 创建存储引擎和锁管理器
	storage := NewMockStorageEngine()
	lockManager := NewMockDistributedLockManager()
	defer lockManager.Stop()

	// 创建Percolator MVCC
	config := percolator.DefaultPercolatorMVCCConfig()
	pmvcc := percolator.NewPercolatorMVCC(storage, lockManager, config)

	ctx := context.Background()
	key := []byte("lock_conflict_key")
	value1 := []byte("value1")
	value2 := []byte("value2")

	// 事务1：预写但不提交
	txnID1 := "txn_1"
	startTS1 := uint64(1000)

	mutations1 := []*common.Mutation{
		{
			Type:  common.MutationPut,
			Key:   key,
			Value: value1,
		},
	}

	err := pmvcc.Prewrite(ctx, mutations1, key, startTS1, txnID1)
	if err != nil {
		t.Fatalf("Txn1 prewrite failed: %v", err)
	}

	// 事务2：尝试预写相同的键
	txnID2 := "txn_2"
	startTS2 := uint64(1001)

	mutations2 := []*common.Mutation{
		{
			Type:  common.MutationPut,
			Key:   key,
			Value: value2,
		},
	}

	// 应该检测到锁冲突
	err = pmvcc.Prewrite(ctx, mutations2, key, startTS2, txnID2)
	if err == nil {
		t.Fatalf("Expected lock conflict, but prewrite succeeded")
	}

	t.Logf("Lock conflict detected correctly: %v", err)

	// 事务1回滚后，事务2应该能够成功预写
	err = pmvcc.Rollback(ctx, [][]byte{key}, startTS1, txnID1)
	if err != nil {
		t.Fatalf("Txn1 rollback failed: %v", err)
	}

	// 现在事务2应该能够成功预写
	err = pmvcc.Prewrite(ctx, mutations2, key, startTS2, txnID2)
	if err != nil {
		t.Fatalf("Txn2 prewrite failed after txn1 rollback: %v", err)
	}

	t.Logf("Percolator MVCC lock conflict test passed")
}

// TestPercolatorMVCCSnapshot 测试快照隔离
func TestPercolatorMVCCSnapshot(t *testing.T) {
	// 创建存储引擎和锁管理器
	storage := NewMockStorageEngine()
	lockManager := NewMockDistributedLockManager()
	defer lockManager.Stop()

	// 创建Percolator MVCC
	config := percolator.DefaultPercolatorMVCCConfig()
	pmvcc := percolator.NewPercolatorMVCC(storage, lockManager, config)

	ctx := context.Background()
	key := []byte("snapshot_key")
	value1 := []byte("value1")
	value2 := []byte("value2")

	// 写入第一个版本
	txnID1 := "txn_1"
	startTS1 := uint64(1000)
	commitTS1 := uint64(1001)

	mutations1 := []*common.Mutation{
		{
			Type:  common.MutationPut,
			Key:   key,
			Value: value1,
		},
	}

	err := pmvcc.Prewrite(ctx, mutations1, key, startTS1, txnID1)
	if err != nil {
		t.Fatalf("Txn1 prewrite failed: %v", err)
	}

	err = pmvcc.Commit(ctx, [][]byte{key}, startTS1, commitTS1, txnID1)
	if err != nil {
		t.Fatalf("Txn1 commit failed: %v", err)
	}

	// 写入第二个版本
	txnID2 := "txn_2"
	startTS2 := uint64(1002)
	commitTS2 := uint64(1003)

	mutations2 := []*common.Mutation{
		{
			Type:  common.MutationPut,
			Key:   key,
			Value: value2,
		},
	}

	err = pmvcc.Prewrite(ctx, mutations2, key, startTS2, txnID2)
	if err != nil {
		t.Fatalf("Txn2 prewrite failed: %v", err)
	}

	err = pmvcc.Commit(ctx, [][]byte{key}, startTS2, commitTS2, txnID2)
	if err != nil {
		t.Fatalf("Txn2 commit failed: %v", err)
	}

	// 测试快照隔离：不同时间戳应该看到不同的值

	// 在commitTS1之后但commitTS2之前读取，应该看到value1
	readValue, err := pmvcc.Get(ctx, key, commitTS1+1)
	if err != nil {
		t.Fatalf("Get at timestamp %d failed: %v", commitTS1+1, err)
	}
	if string(readValue) != string(value1) {
		t.Errorf("Expected value1 at timestamp %d, got %s", commitTS1+1, string(readValue))
	}

	// 在commitTS2之后读取，应该看到value2
	readValue, err = pmvcc.Get(ctx, key, commitTS2+1)
	if err != nil {
		t.Fatalf("Get at timestamp %d failed: %v", commitTS2+1, err)
	}
	if string(readValue) != string(value2) {
		t.Errorf("Expected value2 at timestamp %d, got %s", commitTS2+1, string(readValue))
	}

	// 在startTS1之前读取，应该看不到任何值
	readValue, err = pmvcc.Get(ctx, key, startTS1-1)
	if err != nil {
		t.Fatalf("Get at timestamp %d failed: %v", startTS1-1, err)
	}
	if readValue != nil {
		t.Errorf("Expected nil at timestamp %d, got %s", startTS1-1, string(readValue))
	}

	t.Logf("Percolator MVCC snapshot isolation test passed")
}

// TestPercolatorMVCCMetrics 测试性能指标
func TestPercolatorMVCCMetrics(t *testing.T) {
	// 创建存储引擎和锁管理器
	storage := NewMockStorageEngine()
	lockManager := NewMockDistributedLockManager()
	defer lockManager.Stop()

	// 创建Percolator MVCC
	config := percolator.DefaultPercolatorMVCCConfig()
	pmvcc := percolator.NewPercolatorMVCC(storage, lockManager, config)

	ctx := context.Background()
	key := []byte("metrics_key")
	value := []byte("metrics_value")
	txnID := "metrics_txn"
	startTS := uint64(1000)
	commitTS := uint64(1001)

	// 执行一些操作
	mutations := []*common.Mutation{
		{
			Type:  common.MutationPut,
			Key:   key,
			Value: value,
		},
	}

	pmvcc.Prewrite(ctx, mutations, key, startTS, txnID)
	pmvcc.Commit(ctx, [][]byte{key}, startTS, commitTS, txnID)
	pmvcc.Get(ctx, key, commitTS+1)

	// 检查指标
	metrics := pmvcc.GetMetrics()
	if metrics.PrewriteOps == 0 {
		t.Errorf("Expected prewrite operations > 0")
	}
	if metrics.CommitOps == 0 {
		t.Errorf("Expected commit operations > 0")
	}
	if metrics.ReadOps == 0 {
		t.Errorf("Expected read operations > 0")
	}

	t.Logf("Percolator MVCC metrics test passed")
	t.Logf("  Prewrite ops: %d", metrics.PrewriteOps)
	t.Logf("  Commit ops: %d", metrics.CommitOps)
	t.Logf("  Read ops: %d", metrics.ReadOps)
}
