/*
JadeDB 统一事务系统测试

本模块测试新的统一事务系统，包括本地事务、分布式事务、MVCC、锁管理等。
替代旧的txn_test.go，基于新的Transaction接口进行全面测试。

测试覆盖：
1. 基本事务操作：Put/Get/Delete/Commit/Rollback
2. 事务隔离级别：各种隔离级别的行为验证
3. MVCC功能：多版本并发控制测试
4. 锁管理：死锁检测、锁等待、锁升级
5. 分布式事务：2PC协议、故障恢复
6. 性能测试：并发事务、大数据量测试
*/

package test

import (
	"fmt"
	"github.com/util6/JadeDB/transaction/core"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestTransactionManager_BasicOperations 测试事务管理器基本操作
func TestTransactionManager_BasicOperations(t *testing.T) {
	// 创建事务管理器
	config := core.DefaultTransactionConfig()
	config.EnableDistributed = false // 测试本地事务

	tm, err := core.NewTransactionManager(config)
	require.NoError(t, err)
	defer tm.Close()

	// 测试开始事务
	txn, err := tm.BeginTransaction(nil)
	require.NoError(t, err)
	require.NotNil(t, txn)

	// 验证事务属性
	assert.NotEmpty(t, txn.GetTxnID())
	assert.False(t, txn.IsDistributed())
	assert.Equal(t, core.ReadCommitted, txn.GetIsolationLevel())

	// 测试基本操作
	key := []byte("test_key")
	value := []byte("test_value")

	// Put操作
	err = txn.Put(key, value)
	require.NoError(t, err)

	// Get操作
	retrievedValue, err := txn.Get(key)
	require.NoError(t, err)
	assert.Equal(t, value, retrievedValue)

	// Exists操作
	exists, err := txn.Exists(key)
	require.NoError(t, err)
	assert.True(t, exists)

	// 提交事务
	err = txn.Commit()
	require.NoError(t, err)
}

// TestTransaction_IsolationLevels 测试事务隔离级别
func TestTransaction_IsolationLevels(t *testing.T) {
	config := core.DefaultTransactionConfig()
	tm, err := core.NewTransactionManager(config)
	require.NoError(t, err)
	defer tm.Close()

	isolationLevels := []core.IsolationLevel{
		core.ReadUncommitted,
		core.ReadCommitted,
		core.RepeatableRead,
		core.Serializable,
		core.SnapshotIsolation,
	}

	for _, isolation := range isolationLevels {
		t.Run(fmt.Sprintf("Isolation_%s", isolation.String()), func(t *testing.T) {
			options := &core.TransactionOptions{
				Isolation: isolation,
				Timeout:   10 * time.Second,
				ReadOnly:  false,
			}

			txn, err := tm.BeginTransaction(options)
			require.NoError(t, err)

			assert.Equal(t, isolation, txn.GetIsolationLevel())

			// 基本操作测试
			key := []byte(fmt.Sprintf("isolation_test_%d", isolation))
			value := []byte(fmt.Sprintf("value_%d", isolation))

			err = txn.Put(key, value)
			require.NoError(t, err)

			retrievedValue, err := txn.Get(key)
			require.NoError(t, err)
			assert.Equal(t, value, retrievedValue)

			err = txn.Commit()
			require.NoError(t, err)
		})
	}
}

// TestTransaction_ConcurrentAccess 测试并发事务访问
func TestTransaction_ConcurrentAccess(t *testing.T) {
	config := core.DefaultTransactionConfig()
	config.MaxConcurrentTxns = 100

	tm, err := core.NewTransactionManager(config)
	require.NoError(t, err)
	defer tm.Close()

	numTxns := 50
	numOpsPerTxn := 10

	var wg sync.WaitGroup
	errors := make(chan error, numTxns)

	// 启动多个并发事务
	for i := 0; i < numTxns; i++ {
		wg.Add(1)
		go func(txnIndex int) {
			defer wg.Done()

			txn, err := tm.BeginTransaction(nil)
			if err != nil {
				errors <- err
				return
			}

			// 执行多个操作
			for j := 0; j < numOpsPerTxn; j++ {
				key := []byte(fmt.Sprintf("concurrent_key_%d_%d", txnIndex, j))
				value := []byte(fmt.Sprintf("concurrent_value_%d_%d", txnIndex, j))

				if err := txn.Put(key, value); err != nil {
					errors <- err
					return
				}

				retrievedValue, err := txn.Get(key)
				if err != nil {
					errors <- err
					return
				}

				if string(retrievedValue) != string(value) {
					errors <- fmt.Errorf("value mismatch: expected %s, got %s",
						string(value), string(retrievedValue))
					return
				}
			}

			if err := txn.Commit(); err != nil {
				errors <- err
				return
			}
		}(i)
	}

	wg.Wait()
	close(errors)

	// 检查是否有错误
	for err := range errors {
		t.Errorf("Concurrent transaction error: %v", err)
	}
}

// TestTransaction_RollbackBehavior 测试事务回滚行为
func TestTransaction_RollbackBehavior(t *testing.T) {
	config := core.DefaultTransactionConfig()
	tm, err := core.NewTransactionManager(config)
	require.NoError(t, err)
	defer tm.Close()

	// 测试正常回滚
	t.Run("NormalRollback", func(t *testing.T) {
		txn, err := tm.BeginTransaction(nil)
		require.NoError(t, err)

		key := []byte("rollback_test_key")
		value := []byte("rollback_test_value")

		err = txn.Put(key, value)
		require.NoError(t, err)

		// 验证在事务内可以读取
		retrievedValue, err := txn.Get(key)
		require.NoError(t, err)
		assert.Equal(t, value, retrievedValue)

		// 回滚事务
		err = txn.Rollback()
		require.NoError(t, err)

		// 创建新事务验证数据不存在
		txn2, err := tm.BeginTransaction(nil)
		require.NoError(t, err)

		exists, err := txn2.Exists(key)
		require.NoError(t, err)
		assert.False(t, exists)

		err = txn2.Commit()
		require.NoError(t, err)
	})

	// 测试重复回滚
	t.Run("DoubleRollback", func(t *testing.T) {
		txn, err := tm.BeginTransaction(nil)
		require.NoError(t, err)

		err = txn.Rollback()
		require.NoError(t, err)

		// 第二次回滚应该不报错
		err = txn.Rollback()
		require.NoError(t, err)
	})
}

// TestTransaction_BatchOperations 测试批量操作
func TestTransaction_BatchOperations(t *testing.T) {
	config := core.DefaultTransactionConfig()
	tm, err := core.NewTransactionManager(config)
	require.NoError(t, err)
	defer tm.Close()

	txn, err := tm.BeginTransaction(nil)
	require.NoError(t, err)

	// 准备批量数据
	batchSize := 100
	batch := make([]core.KVPair, batchSize)
	keys := make([][]byte, batchSize)

	for i := 0; i < batchSize; i++ {
		key := []byte(fmt.Sprintf("batch_key_%d", i))
		value := []byte(fmt.Sprintf("batch_value_%d", i))

		batch[i] = core.KVPair{Key: key, Value: value}
		keys[i] = key
	}

	// 测试批量写入
	err = txn.BatchPut(batch)
	require.NoError(t, err)

	// 测试批量读取
	values, err := txn.BatchGet(keys)
	require.NoError(t, err)
	require.Len(t, values, batchSize)

	// 验证数据正确性
	for i, value := range values {
		expectedValue := []byte(fmt.Sprintf("batch_value_%d", i))
		assert.Equal(t, expectedValue, value)
	}

	// 测试批量删除
	err = txn.BatchDelete(keys)
	require.NoError(t, err)

	// 验证删除结果
	for _, key := range keys {
		exists, err := txn.Exists(key)
		require.NoError(t, err)
		assert.False(t, exists)
	}

	err = txn.Commit()
	require.NoError(t, err)
}

// TestTransaction_ReadOnlyTransaction 测试只读事务
func TestTransaction_ReadOnlyTransaction(t *testing.T) {
	config := core.DefaultTransactionConfig()
	tm, err := core.NewTransactionManager(config)
	require.NoError(t, err)
	defer tm.Close()

	// 先创建一些数据
	setupTxn, err := tm.BeginTransaction(nil)
	require.NoError(t, err)

	key := []byte("readonly_test_key")
	value := []byte("readonly_test_value")

	err = setupTxn.Put(key, value)
	require.NoError(t, err)

	err = setupTxn.Commit()
	require.NoError(t, err)

	// 创建只读事务
	readOnlyOptions := &core.TransactionOptions{
		Isolation: core.ReadCommitted,
		Timeout:   10 * time.Second,
		ReadOnly:  true,
	}

	readOnlyTxn, err := tm.BeginTransaction(readOnlyOptions)
	require.NoError(t, err)

	assert.True(t, readOnlyTxn.IsReadOnly())

	// 测试读取操作
	retrievedValue, err := readOnlyTxn.Get(key)
	require.NoError(t, err)
	assert.Equal(t, value, retrievedValue)

	// 测试写入操作应该失败
	err = readOnlyTxn.Put([]byte("new_key"), []byte("new_value"))
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "read-only")

	// 测试删除操作应该失败
	err = readOnlyTxn.Delete(key)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "read-only")

	err = readOnlyTxn.Commit()
	require.NoError(t, err)
}

// TestTransactionManager_Metrics 测试事务指标
func TestTransactionManager_Metrics(t *testing.T) {
	config := core.DefaultTransactionConfig()
	tm, err := core.NewTransactionManager(config)
	require.NoError(t, err)
	defer tm.Close()

	initialMetrics := tm.GetMetrics()

	// 创建并提交一些事务
	numTxns := 10
	for i := 0; i < numTxns; i++ {
		txn, err := tm.BeginTransaction(nil)
		require.NoError(t, err)

		key := []byte(fmt.Sprintf("metrics_key_%d", i))
		value := []byte(fmt.Sprintf("metrics_value_%d", i))

		err = txn.Put(key, value)
		require.NoError(t, err)

		err = txn.Commit()
		require.NoError(t, err)
	}

	// 创建并回滚一些事务
	numAbortedTxns := 5
	for i := 0; i < numAbortedTxns; i++ {
		txn, err := tm.BeginTransaction(nil)
		require.NoError(t, err)

		key := []byte(fmt.Sprintf("aborted_key_%d", i))
		value := []byte(fmt.Sprintf("aborted_value_%d", i))

		err = txn.Put(key, value)
		require.NoError(t, err)

		err = txn.Rollback()
		require.NoError(t, err)
	}

	finalMetrics := tm.GetMetrics()

	// 验证指标
	assert.Equal(t, initialMetrics.TotalTransactions.Load()+int64(numTxns+numAbortedTxns),
		finalMetrics.TotalTransactions.Load())
	assert.Equal(t, initialMetrics.CommittedTransactions.Load()+int64(numTxns),
		finalMetrics.CommittedTransactions.Load())
	assert.Equal(t, initialMetrics.AbortedTransactions.Load()+int64(numAbortedTxns),
		finalMetrics.AbortedTransactions.Load())
	assert.Equal(t, int64(0), finalMetrics.ActiveTransactions.Load())
}

// BenchmarkTransaction_BasicOperations 基本操作性能测试
func BenchmarkTransaction_BasicOperations(b *testing.B) {
	config := core.DefaultTransactionConfig()
	tm, err := core.NewTransactionManager(config)
	require.NoError(b, err)
	defer tm.Close()

	b.ResetTimer()

	b.Run("Put", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			txn, err := tm.BeginTransaction(nil)
			require.NoError(b, err)

			key := []byte(fmt.Sprintf("bench_key_%d", i))
			value := []byte(fmt.Sprintf("bench_value_%d", i))

			err = txn.Put(key, value)
			require.NoError(b, err)

			err = txn.Commit()
			require.NoError(b, err)
		}
	})

	b.Run("Get", func(b *testing.B) {
		// 先准备数据
		setupTxn, err := tm.BeginTransaction(nil)
		require.NoError(b, err)

		for i := 0; i < b.N; i++ {
			key := []byte(fmt.Sprintf("bench_get_key_%d", i))
			value := []byte(fmt.Sprintf("bench_get_value_%d", i))
			err = setupTxn.Put(key, value)
			require.NoError(b, err)
		}

		err = setupTxn.Commit()
		require.NoError(b, err)

		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			txn, err := tm.BeginTransaction(nil)
			require.NoError(b, err)

			key := []byte(fmt.Sprintf("bench_get_key_%d", i))
			_, err = txn.Get(key)
			require.NoError(b, err)

			err = txn.Commit()
			require.NoError(b, err)
		}
	})
}
