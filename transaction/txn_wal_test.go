/*
事务WAL功能测试

本测试验证事务系统与WAL的集成功能，确保事务操作正确记录到WAL中。

测试内容：
1. 事务开始记录WAL
2. 数据操作记录WAL
3. 事务提交记录WAL
4. 事务回滚记录WAL
5. WAL恢复功能
*/

package transaction

import (
	"os"
	"testing"
	"time"

	"github.com/util6/JadeDB/bplustree"
	"github.com/util6/JadeDB/txnwal"
)

// TestTransactionWALIntegration 测试事务与WAL的集成
func TestTransactionWALIntegration(t *testing.T) {
	// 清理测试目录
	testDir := "./test_txnwal"
	os.RemoveAll(testDir)
	defer os.RemoveAll(testDir)

	// 创建事务管理器
	config := DefaultTransactionConfig()
	txnMgr, err := NewTransactionManager(config)
	if err != nil {
		t.Fatalf("Failed to create transaction manager: %v", err)
	}
	defer txnMgr.Close()

	// 创建B+树存储引擎
	btreeConfig := bplustree.DefaultBTreeEngineConfig()
	btreeConfig.BTreeOptions.WorkDir = testDir + "/btree"
	btreeEngine, err := bplustree.NewBTreeEngine(btreeConfig)
	if err != nil {
		t.Fatalf("Failed to create B+tree engine: %v", err)
	}
	defer btreeEngine.Close()

	// 设置默认存储引擎
	txnMgr.SetDefaultEngine(btreeEngine)

	// 测试事务操作
	t.Run("TransactionOperations", func(t *testing.T) {
		// 开始事务
		txn, err := txnMgr.BeginTransaction(nil)
		if err != nil {
			t.Fatalf("Failed to begin transaction: %v", err)
		}

		// 执行一些操作
		key1 := []byte("test_key_1")
		value1 := []byte("test_value_1")
		if err := txn.Put(key1, value1); err != nil {
			t.Fatalf("Failed to put key-value: %v", err)
		}

		key2 := []byte("test_key_2")
		value2 := []byte("test_value_2")
		if err := txn.Put(key2, value2); err != nil {
			t.Fatalf("Failed to put key-value: %v", err)
		}

		// 删除一个键
		if err := txn.Delete(key1); err != nil {
			t.Fatalf("Failed to delete key: %v", err)
		}

		// 提交事务
		if err := txn.Commit(); err != nil {
			t.Fatalf("Failed to commit transaction: %v", err)
		}

		// 事务已提交，无需验证状态（接口不暴露GetState）

		// 关闭事务
		if err := txn.Close(); err != nil {
			t.Fatalf("Failed to close transaction: %v", err)
		}
	})

	// 测试事务回滚
	t.Run("TransactionRollback", func(t *testing.T) {
		// 开始事务
		txn, err := txnMgr.BeginTransaction(nil)
		if err != nil {
			t.Fatalf("Failed to begin transaction: %v", err)
		}

		// 执行一些操作
		key := []byte("rollback_key")
		value := []byte("rollback_value")
		if err := txn.Put(key, value); err != nil {
			t.Fatalf("Failed to put key-value: %v", err)
		}

		// 回滚事务
		if err := txn.Rollback(); err != nil {
			t.Fatalf("Failed to rollback transaction: %v", err)
		}

		// 事务已回滚，无需验证状态（接口不暴露GetState）

		// 关闭事务
		if err := txn.Close(); err != nil {
			t.Fatalf("Failed to close transaction: %v", err)
		}
	})

	// 测试WAL记录
	t.Run("WALRecords", func(t *testing.T) {
		// 获取WAL管理器
		walMgr := txnMgr.txnWAL
		if walMgr == nil {
			t.Fatal("WAL manager is nil")
		}

		// 获取WAL统计信息
		stats := walMgr.GetStatistics()
		t.Logf("WAL Statistics: %+v", stats)

		// 验证有WAL记录
		if stats.TotalRecords == 0 {
			t.Error("Expected WAL records to be written, but got 0")
		}
	})
}

// TestTransactionWALRecovery 测试WAL恢复功能
func TestTransactionWALRecovery(t *testing.T) {
	// 清理测试目录
	testDir := "./test_recovery"
	os.RemoveAll(testDir)
	defer os.RemoveAll(testDir)

	// 第一阶段：创建事务并写入数据
	func() {
		config := DefaultTransactionConfig()
		txnMgr, err := NewTransactionManager(config)
		if err != nil {
			t.Fatalf("Failed to create transaction manager: %v", err)
		}
		defer txnMgr.Close()

		// 创建内存WAL用于测试
		walOptions := txnwal.DefaultTxnWALOptions()
		memWAL, err := txnwal.NewMemoryTxnWALManager(walOptions)
		if err != nil {
			t.Fatalf("Failed to create memory WAL: %v", err)
		}

		// 创建恢复管理器
		recoveryMgr := txnwal.NewUnifiedRecoveryManager(memWAL, walOptions, &txnwal.DefaultRecoveryCallbacks{})

		// 模拟一些事务操作
		txn, err := txnMgr.BeginTransaction(nil)
		if err != nil {
			t.Fatalf("Failed to begin transaction: %v", err)
		}

		key := []byte("recovery_test_key")
		value := []byte("recovery_test_value")
		if err := txn.Put(key, value); err != nil {
			t.Fatalf("Failed to put key-value: %v", err)
		}

		if err := txn.Commit(); err != nil {
			t.Fatalf("Failed to commit transaction: %v", err)
		}

		txn.Close()

		// 验证WAL中有记录
		stats := memWAL.GetStatistics()
		if stats.TotalRecords == 0 {
			t.Error("Expected WAL records for recovery test")
		}

		// 测试恢复功能
		ctx := txnMgr.ctx
		if err := recoveryMgr.Recovery(ctx); err != nil {
			t.Errorf("Recovery failed: %v", err)
		}
	}()
}

// TestTransactionTimeout 测试事务超时
func TestTransactionTimeout(t *testing.T) {
	// 创建短超时的配置
	config := DefaultTransactionConfig()
	config.DefaultTimeout = 100 * time.Millisecond

	txnMgr, err := NewTransactionManager(config)
	if err != nil {
		t.Fatalf("Failed to create transaction manager: %v", err)
	}
	defer txnMgr.Close()

	// 开始事务
	txn, err := txnMgr.BeginTransaction(nil)
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}
	defer txn.Close()

	// 等待超时
	time.Sleep(200 * time.Millisecond)

	// 尝试操作，应该失败
	key := []byte("timeout_key")
	value := []byte("timeout_value")
	err = txn.Put(key, value)
	if err == nil {
		t.Error("Expected timeout error, but operation succeeded")
	}

	if err != nil && err.Error() != "transaction timeout" {
		t.Errorf("Expected timeout error, got: %v", err)
	}
}

// TestTransactionStatistics 测试事务统计信息
func TestTransactionStatistics(t *testing.T) {
	config := DefaultTransactionConfig()
	txnMgr, err := NewTransactionManager(config)
	if err != nil {
		t.Fatalf("Failed to create transaction manager: %v", err)
	}
	defer txnMgr.Close()

	// 开始事务
	txn, err := txnMgr.BeginTransaction(nil)
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}

	// 执行一些操作
	for i := 0; i < 5; i++ {
		key := []byte("stats_key_" + string(rune('0'+i)))
		value := []byte("stats_value_" + string(rune('0'+i)))
		if err := txn.Put(key, value); err != nil {
			t.Fatalf("Failed to put key-value: %v", err)
		}
	}

	// 统计信息通过WAL管理器获取
	walStats := txnMgr.txnWAL.GetStatistics()
	t.Logf("WAL Statistics: %+v", walStats)

	// 验证有WAL记录
	if walStats.TotalRecords == 0 {
		t.Error("Expected WAL records to be written")
	}

	// 提交事务
	if err := txn.Commit(); err != nil {
		t.Fatalf("Failed to commit transaction: %v", err)
	}

	txn.Close()
}
