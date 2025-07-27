/*
JadeDB B+树锁管理器测试模块

本模块包含锁管理器的单元测试和集成测试。
测试覆盖页面级锁、死锁检测、锁超时等核心功能。
*/

package bplustree

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestPageLockManagerBasic 测试锁管理器基本功能
func TestPageLockManagerBasic(t *testing.T) {
	options := &BTreeOptions{
		LockTimeout:    time.Second,
		DeadlockDetect: true,
	}

	lm := NewPageLockManager(options)
	defer lm.Close()

	ctx := context.Background()
	txnID := uint64(1)
	pageID := uint64(100)

	// 测试获取读锁
	err := lm.AcquireLock(ctx, txnID, pageID, ReadLock)
	require.NoError(t, err)

	// 验证统计信息
	stats := lm.GetStats()
	assert.Equal(t, int64(1), stats["lock_requests"])
	assert.Equal(t, int64(1), stats["lock_grants"])
	assert.Equal(t, int64(0), stats["lock_waits"])

	// 释放锁
	err = lm.ReleaseLock(txnID, pageID)
	require.NoError(t, err)
}

// TestPageLockManagerReadWriteCompatibility 测试读写锁兼容性
func TestPageLockManagerReadWriteCompatibility(t *testing.T) {
	options := &BTreeOptions{
		LockTimeout:    time.Second,
		DeadlockDetect: false,
	}

	lm := NewPageLockManager(options)
	defer lm.Close()

	ctx := context.Background()
	pageID := uint64(100)

	// 事务1获取读锁
	err := lm.AcquireLock(ctx, 1, pageID, ReadLock)
	require.NoError(t, err)

	// 事务2也可以获取读锁（兼容）
	err = lm.AcquireLock(ctx, 2, pageID, ReadLock)
	require.NoError(t, err)

	// 事务3尝试获取写锁（应该等待）
	done := make(chan bool)
	go func() {
		err := lm.AcquireLock(ctx, 3, pageID, WriteLock)
		assert.NoError(t, err)
		done <- true
	}()

	// 等待一小段时间，确保事务3在等待
	time.Sleep(100 * time.Millisecond)
	select {
	case <-done:
		t.Fatal("Write lock should be waiting")
	default:
		// 正确，写锁在等待
	}

	// 释放读锁
	err = lm.ReleaseLock(1, pageID)
	require.NoError(t, err)

	err = lm.ReleaseLock(2, pageID)
	require.NoError(t, err)

	// 现在写锁应该被授予
	select {
	case <-done:
		// 正确，写锁被授予
	case <-time.After(time.Second):
		t.Fatal("Write lock should be granted after read locks are released")
	}

	// 清理
	err = lm.ReleaseLock(3, pageID)
	require.NoError(t, err)
}

// TestPageLockManagerTimeout 测试锁超时
func TestPageLockManagerTimeout(t *testing.T) {
	options := &BTreeOptions{
		LockTimeout:    100 * time.Millisecond, // 短超时时间
		DeadlockDetect: false,
	}

	lm := NewPageLockManager(options)
	defer lm.Close()

	ctx := context.Background()
	pageID := uint64(100)

	// 事务1获取写锁
	err := lm.AcquireLock(ctx, 1, pageID, WriteLock)
	require.NoError(t, err)

	// 事务2尝试获取写锁，应该超时
	start := time.Now()
	err = lm.AcquireLock(ctx, 2, pageID, WriteLock)
	duration := time.Since(start)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "timeout")
	assert.True(t, duration >= 100*time.Millisecond)
	assert.True(t, duration < 200*time.Millisecond) // 允许一些误差

	// 清理
	err = lm.ReleaseLock(1, pageID)
	require.NoError(t, err)
}

// TestPageLockManagerReleaseAllLocks 测试释放所有锁
func TestPageLockManagerReleaseAllLocks(t *testing.T) {
	options := &BTreeOptions{
		LockTimeout:    time.Second,
		DeadlockDetect: false,
	}

	lm := NewPageLockManager(options)
	defer lm.Close()

	ctx := context.Background()
	txnID := uint64(1)

	// 获取多个页面的锁
	pageIDs := []uint64{100, 101, 102}
	for _, pageID := range pageIDs {
		err := lm.AcquireLock(ctx, txnID, pageID, ReadLock)
		require.NoError(t, err)
	}

	// 验证锁被持有
	stats := lm.GetStats()
	assert.Equal(t, int64(3), stats["lock_grants"])

	// 释放所有锁
	err := lm.ReleaseAllLocks(txnID)
	require.NoError(t, err)

	// 验证锁已释放（通过尝试获取写锁）
	for _, pageID := range pageIDs {
		err := lm.AcquireLock(ctx, 2, pageID, WriteLock)
		require.NoError(t, err)
		err = lm.ReleaseLock(2, pageID)
		require.NoError(t, err)
	}
}

// TestPageLockManagerConcurrency 测试锁管理器并发性能
func TestPageLockManagerConcurrency(t *testing.T) {
	options := &BTreeOptions{
		LockTimeout:    time.Second,
		DeadlockDetect: false,
	}

	lm := NewPageLockManager(options)
	defer lm.Close()

	const numGoroutines = 10
	const numOperations = 100
	const numPages = 10

	var wg sync.WaitGroup
	errors := make(chan error, numGoroutines*numOperations)

	// 启动多个goroutine进行并发锁操作
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()
			ctx := context.Background()

			for j := 0; j < numOperations; j++ {
				txnID := uint64(goroutineID*numOperations + j)
				pageID := uint64(j % numPages) // 循环使用页面

				// 获取锁
				err := lm.AcquireLock(ctx, txnID, pageID, ReadLock)
				if err != nil {
					errors <- err
					return
				}

				// 模拟一些工作
				time.Sleep(time.Microsecond)

				// 释放锁
				err = lm.ReleaseLock(txnID, pageID)
				if err != nil {
					errors <- err
					return
				}
			}
		}(i)
	}

	wg.Wait()
	close(errors)

	// 检查错误
	for err := range errors {
		t.Errorf("Concurrent lock operation failed: %v", err)
	}

	// 验证最终状态
	stats := lm.GetStats()
	expectedRequests := int64(numGoroutines * numOperations)
	assert.Equal(t, expectedRequests, stats["lock_requests"])
	assert.Equal(t, expectedRequests, stats["lock_grants"])
	t.Logf("Lock manager stats: %+v", stats)
}

// TestPageLockManagerDeadlockDetection 测试死锁检测
func TestPageLockManagerDeadlockDetection(t *testing.T) {
	options := &BTreeOptions{
		LockTimeout:    5 * time.Second, // 长超时时间，让死锁检测先触发
		DeadlockDetect: true,
	}

	lm := NewPageLockManager(options)
	defer lm.Close()

	ctx := context.Background()
	page1 := uint64(100)
	page2 := uint64(101)

	// 事务1获取page1的写锁
	err := lm.AcquireLock(ctx, 1, page1, WriteLock)
	require.NoError(t, err)

	// 事务2获取page2的写锁
	err = lm.AcquireLock(ctx, 2, page2, WriteLock)
	require.NoError(t, err)

	// 创建死锁情况
	var wg sync.WaitGroup
	errors := make(chan error, 2)

	// 事务1尝试获取page2的锁
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := lm.AcquireLock(ctx, 1, page2, WriteLock)
		errors <- err
	}()

	// 事务2尝试获取page1的锁
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := lm.AcquireLock(ctx, 2, page1, WriteLock)
		errors <- err
	}()

	// 等待死锁检测和解决
	wg.Wait()
	close(errors)

	// 检查结果
	errorCount := 0
	for err := range errors {
		if err != nil {
			errorCount++
		}
	}

	// 至少有一个事务应该成功（死锁被解决）
	assert.True(t, errorCount < 2, "Deadlock should be resolved")

	// 验证死锁统计
	stats := lm.GetStats()
	t.Logf("Deadlock detection stats: %+v", stats)

	// 清理剩余的锁
	lm.ReleaseAllLocks(1)
	lm.ReleaseAllLocks(2)
}
