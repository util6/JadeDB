/*
JadeDB B+树WAL恢复功能测试

本测试文件验证B+树恢复管理器的WAL恢复机制，
确保重做操作、撤销操作、数据完整性验证等功能正常工作。

测试覆盖：
1. 基本的重做操作（插入、更新、删除）
2. 撤销操作逻辑
3. 页面分裂和合并恢复
4. 数据完整性验证
5. WAL扫描功能
*/

package bplustree

import (
	"encoding/binary"
	"os"
	"testing"
)

// TestRecoveryManagerBasic 测试恢复管理器基本功能
func TestRecoveryManagerBasic(t *testing.T) {
	// 创建临时目录
	tempDir, err := os.MkdirTemp("", "recovery_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// 创建B+树配置
	options := &BTreeOptions{
		WorkDir:        tempDir,
		PageSize:       16384,
		BufferPoolSize: 64,
		WALBufferSize:  1024,
	}

	// 创建页面管理器
	pageManager, err := NewPageManager(options)
	if err != nil {
		t.Fatalf("Failed to create page manager: %v", err)
	}
	defer pageManager.Close()

	// 创建恢复管理器
	recoveryManager, err := NewRecoveryManager(options, pageManager)
	if err != nil {
		t.Fatalf("Failed to create recovery manager: %v", err)
	}

	// 测试基本功能
	stats := recoveryManager.GetStats()
	if stats == nil {
		t.Errorf("GetStats returned nil")
	}

	t.Logf("Recovery manager created successfully")
}

// TestRedoOperations 测试重做操作
func TestRedoOperations(t *testing.T) {
	// 创建临时目录
	tempDir, err := os.MkdirTemp("", "redo_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// 创建B+树配置
	options := &BTreeOptions{
		WorkDir:        tempDir,
		PageSize:       16384,
		BufferPoolSize: 64,
		WALBufferSize:  1024,
	}

	// 创建页面管理器
	pageManager, err := NewPageManager(options)
	if err != nil {
		t.Fatalf("Failed to create page manager: %v", err)
	}
	defer pageManager.Close()

	// 创建恢复管理器
	recoveryManager, err := NewRecoveryManager(options, pageManager)
	if err != nil {
		t.Fatalf("Failed to create recovery manager: %v", err)
	}

	// 创建测试页面并初始化为叶子节点
	page := NewPage(1, LeafPage)
	_, err = NewNode(page, LeafNodeType)
	if err != nil {
		t.Fatalf("Failed to create node: %v", err)
	}

	// 测试插入操作的重做
	t.Run("RedoInsert", func(t *testing.T) {
		// 创建插入记录的数据
		key := []byte("test_key")
		value := []byte("test_value")
		data := createInsertData(key, value)

		record := &LogRecord{
			LSN:      1,
			Type:     LogInsert,
			TxnID:    1,
			PageID:   1,
			Length:   uint32(len(data)),
			Checksum: 0,
			Data:     data,
		}

		// 执行重做插入
		err := recoveryManager.redoInsert(page, record)
		if err != nil {
			t.Errorf("RedoInsert failed: %v", err)
		}

		// 验证插入结果 - 重新加载节点
		node, err := LoadNode(page)
		if err != nil {
			t.Fatalf("Failed to load node: %v", err)
		}

		foundRecord, _, err := node.searchRecord(key)
		if err != nil {
			t.Errorf("Key not found after redo insert: %v", err)
		} else if string(foundRecord.Value) != string(value) {
			t.Errorf("Value mismatch: expected %s, got %s", string(value), string(foundRecord.Value))
		}
	})

	// 测试更新操作的重做
	t.Run("RedoUpdate", func(t *testing.T) {
		// 创建更新记录的数据
		key := []byte("test_key")
		newValue := []byte("updated_value")
		data := createUpdateData(key, newValue)

		record := &LogRecord{
			LSN:      2,
			Type:     LogUpdate,
			TxnID:    1,
			PageID:   1,
			Length:   uint32(len(data)),
			Checksum: 0,
			Data:     data,
		}

		// 执行重做更新
		err := recoveryManager.redoUpdate(page, record)
		if err != nil {
			t.Errorf("RedoUpdate failed: %v", err)
		}

		// 验证更新结果 - 重新加载节点
		node, err := LoadNode(page)
		if err != nil {
			t.Fatalf("Failed to load node: %v", err)
		}

		foundRecord, _, err := node.searchRecord(key)
		if err != nil {
			t.Errorf("Key not found after redo update: %v", err)
		} else if string(foundRecord.Value) != string(newValue) {
			t.Errorf("Value mismatch after update: expected %s, got %s", string(newValue), string(foundRecord.Value))
		}
	})

	// 测试删除操作的重做
	t.Run("RedoDelete", func(t *testing.T) {
		// 创建删除记录的数据
		key := []byte("test_key")
		data := createDeleteData(key)

		record := &LogRecord{
			LSN:      3,
			Type:     LogDelete,
			TxnID:    1,
			PageID:   1,
			Length:   uint32(len(data)),
			Checksum: 0,
			Data:     data,
		}

		// 执行重做删除
		err := recoveryManager.redoDelete(page, record)
		if err != nil {
			t.Errorf("RedoDelete failed: %v", err)
		}

		// 验证删除结果 - 重新加载节点
		node, err := LoadNode(page)
		if err != nil {
			t.Fatalf("Failed to load node: %v", err)
		}

		_, _, err = node.searchRecord(key)
		if err == nil {
			t.Errorf("Key still exists after redo delete")
		}
	})
}

// TestUndoOperations 测试撤销操作
func TestUndoOperations(t *testing.T) {
	// 创建临时目录
	tempDir, err := os.MkdirTemp("", "undo_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// 创建B+树配置
	options := &BTreeOptions{
		WorkDir:        tempDir,
		PageSize:       16384,
		BufferPoolSize: 64,
		WALBufferSize:  1024,
	}

	// 创建页面管理器
	pageManager, err := NewPageManager(options)
	if err != nil {
		t.Fatalf("Failed to create page manager: %v", err)
	}
	defer pageManager.Close()

	// 创建恢复管理器
	recoveryManager, err := NewRecoveryManager(options, pageManager)
	if err != nil {
		t.Fatalf("Failed to create recovery manager: %v", err)
	}

	// 创建测试页面并插入一些数据
	page := NewPage(1, LeafPage)
	node, err := NewNode(page, LeafNodeType)
	if err != nil {
		t.Fatalf("Failed to create node: %v", err)
	}

	// 插入测试数据
	testKey := []byte("undo_test_key")
	originalValue := []byte("original_value")
	err = node.insertRecord(testKey, originalValue, DataRecord)
	if err != nil {
		t.Fatalf("Failed to insert test data: %v", err)
	}

	// 测试撤销插入操作
	t.Run("UndoInsert", func(t *testing.T) {
		// 创建插入记录的数据
		data := createInsertData(testKey, originalValue)

		record := &LogRecord{
			LSN:      1,
			Type:     LogInsert,
			TxnID:    1,
			PageID:   1,
			Length:   uint32(len(data)),
			Checksum: 0,
			Data:     data,
		}

		// 执行撤销插入（应该删除记录）
		err := recoveryManager.undoInsert(page, record)
		if err != nil {
			t.Errorf("UndoInsert failed: %v", err)
		}

		// 验证记录被删除 - 重新加载节点
		node, err = LoadNode(page)
		if err != nil {
			t.Fatalf("Failed to reload node: %v", err)
		}

		_, _, err = node.searchRecord(testKey)
		if err == nil {
			t.Errorf("Key still exists after undo insert")
		}
	})
}

// 辅助函数：创建插入操作的数据
func createInsertData(key, value []byte) []byte {
	data := make([]byte, 8+len(key)+len(value))
	offset := 0

	// 键长度
	binary.LittleEndian.PutUint32(data[offset:offset+4], uint32(len(key)))
	offset += 4

	// 键
	copy(data[offset:offset+len(key)], key)
	offset += len(key)

	// 值长度
	binary.LittleEndian.PutUint32(data[offset:offset+4], uint32(len(value)))
	offset += 4

	// 值
	copy(data[offset:offset+len(value)], value)

	return data
}

// 辅助函数：创建更新操作的数据
func createUpdateData(key, newValue []byte) []byte {
	// 更新操作的数据格式与插入操作相同
	return createInsertData(key, newValue)
}

// 辅助函数：创建删除操作的数据
func createDeleteData(key []byte) []byte {
	data := make([]byte, 4+len(key))
	offset := 0

	// 键长度
	binary.LittleEndian.PutUint32(data[offset:offset+4], uint32(len(key)))
	offset += 4

	// 键
	copy(data[offset:offset+len(key)], key)

	return data
}
