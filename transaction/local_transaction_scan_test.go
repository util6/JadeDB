/*
JadeDB LocalTransaction 范围查询功能测试

本测试文件验证LocalTransaction的Scan和NewIterator方法实现，
确保事务级别的范围查询功能正常工作。

测试覆盖：
1. 基本的Scan功能
2. 带范围限制的Scan
3. 事务写集合的Scan
4. Iterator的基本功能
5. Iterator的定位和遍历
*/

package transaction

import (
	"testing"
	"time"
)

// TestLocalTransactionScan 测试LocalTransaction的Scan功能
func TestLocalTransactionScan(t *testing.T) {
	// 创建事务管理器
	config := DefaultTransactionConfig()
	config.DefaultTimeout = 5 * time.Second

	tm, err := NewTransactionManager(config)
	if err != nil {
		t.Fatalf("Failed to create transaction manager: %v", err)
	}
	defer tm.Close()

	// 开始事务
	txn, err := tm.BeginTransaction(nil)
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}
	defer txn.Rollback()

	// 测试空数据库的Scan
	t.Run("EmptyDatabaseScan", func(t *testing.T) {
		results, err := txn.Scan(nil, nil, 10)
		if err != nil {
			t.Errorf("Scan failed: %v", err)
		}
		if len(results) != 0 {
			t.Errorf("Expected 0 results, got %d", len(results))
		}
	})

	// 添加一些测试数据
	testData := map[string]string{
		"key1": "value1",
		"key2": "value2",
		"key3": "value3",
		"key5": "value5",
		"key7": "value7",
	}

	for key, value := range testData {
		if err := txn.Put([]byte(key), []byte(value)); err != nil {
			t.Fatalf("Failed to put %s: %v", key, err)
		}
	}

	// 测试全范围Scan
	t.Run("FullRangeScan", func(t *testing.T) {
		results, err := txn.Scan(nil, nil, 10)
		if err != nil {
			t.Errorf("Scan failed: %v", err)
		}
		if len(results) != len(testData) {
			t.Errorf("Expected %d results, got %d", len(testData), len(results))
		}

		// 验证结果是否按键排序
		for i := 1; i < len(results); i++ {
			if string(results[i-1].Key) > string(results[i].Key) {
				t.Errorf("Results not sorted: %s > %s", string(results[i-1].Key), string(results[i].Key))
			}
		}
	})

	// 测试范围限制的Scan
	t.Run("RangeLimitedScan", func(t *testing.T) {
		results, err := txn.Scan([]byte("key2"), []byte("key5"), 10)
		if err != nil {
			t.Errorf("Scan failed: %v", err)
		}

		// 应该包含key2, key3, key5
		expectedKeys := []string{"key2", "key3", "key5"}
		if len(results) != len(expectedKeys) {
			t.Errorf("Expected %d results, got %d", len(expectedKeys), len(results))
		}

		for i, expected := range expectedKeys {
			if i >= len(results) || string(results[i].Key) != expected {
				t.Errorf("Expected key %s at position %d, got %s", expected, i, string(results[i].Key))
			}
		}
	})

	// 测试数量限制
	t.Run("CountLimitedScan", func(t *testing.T) {
		results, err := txn.Scan(nil, nil, 2)
		if err != nil {
			t.Errorf("Scan failed: %v", err)
		}
		if len(results) != 2 {
			t.Errorf("Expected 2 results, got %d", len(results))
		}
	})

	// 测试删除操作的影响
	t.Run("ScanWithDeletes", func(t *testing.T) {
		// 删除一个键
		if err := txn.Delete([]byte("key2")); err != nil {
			t.Fatalf("Failed to delete key2: %v", err)
		}

		results, err := txn.Scan(nil, nil, 10)
		if err != nil {
			t.Errorf("Scan failed: %v", err)
		}

		// 应该少一个结果
		if len(results) != len(testData)-1 {
			t.Errorf("Expected %d results after delete, got %d", len(testData)-1, len(results))
		}

		// 确保key2不在结果中
		for _, kv := range results {
			if string(kv.Key) == "key2" {
				t.Errorf("Deleted key2 should not appear in scan results")
			}
		}
	})
}

// TestLocalTransactionIterator 测试LocalTransaction的Iterator功能
func TestLocalTransactionIterator(t *testing.T) {
	// 创建事务管理器
	config := DefaultTransactionConfig()
	config.DefaultTimeout = 5 * time.Second

	tm, err := NewTransactionManager(config)
	if err != nil {
		t.Fatalf("Failed to create transaction manager: %v", err)
	}
	defer tm.Close()

	// 开始事务
	txn, err := tm.BeginTransaction(nil)
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}
	defer txn.Rollback()

	// 添加测试数据
	testData := map[string]string{
		"apple":  "fruit1",
		"banana": "fruit2",
		"cherry": "fruit3",
		"date":   "fruit4",
	}

	for key, value := range testData {
		if err := txn.Put([]byte(key), []byte(value)); err != nil {
			t.Fatalf("Failed to put %s: %v", key, err)
		}
	}

	// 测试基本迭代器功能
	t.Run("BasicIterator", func(t *testing.T) {
		iter, err := txn.NewIterator(nil)
		if err != nil {
			t.Fatalf("Failed to create iterator: %v", err)
		}
		defer iter.Close()

		count := 0
		for iter.Valid() {
			key := iter.Key()
			value := iter.Value()

			if key == nil || value == nil {
				t.Errorf("Iterator returned nil key or value")
			}

			count++
			iter.Next()
		}

		if count != len(testData) {
			t.Errorf("Expected to iterate over %d items, got %d", len(testData), count)
		}
	})

	// 测试SeekToFirst和SeekToLast
	t.Run("SeekOperations", func(t *testing.T) {
		iter, err := txn.NewIterator(nil)
		if err != nil {
			t.Fatalf("Failed to create iterator: %v", err)
		}
		defer iter.Close()

		// 测试SeekToFirst
		iter.SeekToFirst()
		if !iter.Valid() {
			t.Errorf("SeekToFirst should position iterator at first element")
		}
		firstKey := string(iter.Key())
		if firstKey != "apple" { // 按字典序排序，apple应该是第一个
			t.Errorf("Expected first key to be 'apple', got '%s'", firstKey)
		}

		// 测试SeekToLast
		iter.SeekToLast()
		if !iter.Valid() {
			t.Errorf("SeekToLast should position iterator at last element")
		}
		lastKey := string(iter.Key())
		if lastKey != "date" { // 按字典序排序，date应该是最后一个
			t.Errorf("Expected last key to be 'date', got '%s'", lastKey)
		}
	})

	// 测试Seek定位
	t.Run("SeekToKey", func(t *testing.T) {
		iter, err := txn.NewIterator(nil)
		if err != nil {
			t.Fatalf("Failed to create iterator: %v", err)
		}
		defer iter.Close()

		// 定位到存在的键
		iter.Seek([]byte("cherry"))
		if !iter.Valid() {
			t.Errorf("Seek to existing key should be valid")
		}
		if string(iter.Key()) != "cherry" {
			t.Errorf("Expected key 'cherry', got '%s'", string(iter.Key()))
		}

		// 定位到不存在但在范围内的键
		iter.Seek([]byte("coconut")) // 应该定位到date
		if !iter.Valid() {
			t.Errorf("Seek to non-existing key should position at next key")
		}
		if string(iter.Key()) != "date" {
			t.Errorf("Expected key 'date', got '%s'", string(iter.Key()))
		}
	})

	// 测试前缀迭代
	t.Run("PrefixIterator", func(t *testing.T) {
		// 添加更多有前缀的数据
		prefixData := map[string]string{
			"test1": "value1",
			"test2": "value2",
			"other": "value3",
		}

		for key, value := range prefixData {
			if err := txn.Put([]byte(key), []byte(value)); err != nil {
				t.Fatalf("Failed to put %s: %v", key, err)
			}
		}

		options := &IteratorOptions{
			Prefix: []byte("test"),
		}

		iter, err := txn.NewIterator(options)
		if err != nil {
			t.Fatalf("Failed to create prefix iterator: %v", err)
		}
		defer iter.Close()

		count := 0
		for iter.Valid() {
			key := string(iter.Key())
			if key != "test1" && key != "test2" {
				t.Errorf("Prefix iterator returned unexpected key: %s", key)
			}
			count++
			iter.Next()
		}

		if count != 2 {
			t.Errorf("Expected 2 keys with prefix 'test', got %d", count)
		}
	})
}
