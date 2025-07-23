/*
JadeDB B+树核心功能测试

本模块测试B+树的核心CRUD操作，包括：
1. 节点基本操作测试
2. B+树插入、查找、删除测试
3. 页面分裂和合并测试
4. 范围查询测试
5. 并发操作测试

测试覆盖：
- 单条记录操作
- 批量数据操作
- 边界条件测试
- 错误处理测试
- 性能基准测试
*/

package bplustree

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// setupBTreeTest 设置B+树测试环境
func setupBTreeTest(t *testing.T) (*BPlusTree, string) {
	testDir := filepath.Join(os.TempDir(), "btree_test", t.Name())
	err := os.MkdirAll(testDir, 0755)
	require.NoError(t, err)

	options := &BTreeOptions{
		WorkDir:            testDir,
		PageSize:           PageSize,
		BufferPoolSize:     100,
		WALBufferSize:      1024 * 1024,
		CheckpointInterval: time.Minute,
		LockTimeout:        time.Second,
		DeadlockDetect:     true,
		EnableAdaptiveHash: true,
		EnablePrefetch:     true,
		PrefetchSize:       8,
	}

	btree, err := NewBPlusTree(options)
	require.NoError(t, err)

	return btree, testDir
}

// cleanupBTreeTest 清理测试环境
func cleanupBTreeTest(btree *BPlusTree, testDir string) {
	if btree != nil {
		btree.Close()
	}
	os.RemoveAll(testDir)
}

// TestNodeBasicOperations 测试节点基本操作
func TestNodeBasicOperations(t *testing.T) {
	btree, testDir := setupBTreeTest(t)
	defer cleanupBTreeTest(btree, testDir)

	// 分配页面
	page, err := btree.pageManager.AllocatePage(LeafPage)
	require.NoError(t, err)

	// 创建叶子节点
	node, err := NewNode(page, LeafNodeType)
	require.NoError(t, err)

	// 测试节点属性
	assert.True(t, node.IsLeaf())
	assert.False(t, node.IsInternal())
	assert.False(t, node.IsRoot())
	assert.Equal(t, uint16(0), node.GetRecordCount())
	assert.Greater(t, node.GetFreeSpace(), uint16(0))

	// 测试插入记录
	key1 := []byte("key1")
	value1 := []byte("value1")
	err = node.insertRecord(key1, value1, DataRecord)
	require.NoError(t, err)

	assert.Equal(t, uint16(1), node.GetRecordCount())

	// 测试查找记录
	record, index, err := node.searchRecord(key1)
	require.NoError(t, err)
	assert.Equal(t, key1, record.Key)
	assert.Equal(t, value1, record.Value)
	assert.Equal(t, 0, index)

	// 测试插入多条记录（保持有序）
	key2 := []byte("key2")
	value2 := []byte("value2")
	err = node.insertRecord(key2, value2, DataRecord)
	require.NoError(t, err)

	key0 := []byte("key0")
	value0 := []byte("value0")
	err = node.insertRecord(key0, value0, DataRecord)
	require.NoError(t, err)

	assert.Equal(t, uint16(3), node.GetRecordCount())

	// 验证记录顺序
	record0, err := node.getRecord(0)
	require.NoError(t, err)
	assert.Equal(t, key0, record0.Key)

	record1, err := node.getRecord(1)
	require.NoError(t, err)
	assert.Equal(t, key1, record1.Key)

	record2, err := node.getRecord(2)
	require.NoError(t, err)
	assert.Equal(t, key2, record2.Key)
}

// TestBTreeBasicOperations 测试B+树基本操作
func TestBTreeBasicOperations(t *testing.T) {
	btree, testDir := setupBTreeTest(t)
	defer cleanupBTreeTest(btree, testDir)

	// 测试插入
	key1 := []byte("key1")
	value1 := []byte("value1")
	err := btree.Put(key1, value1)
	require.NoError(t, err)

	// 测试查找
	result, err := btree.Get(key1)
	require.NoError(t, err)
	assert.Equal(t, value1, result)

	// 测试不存在的键
	_, err = btree.Get([]byte("nonexistent"))
	assert.Error(t, err)

	// 测试更新
	newValue1 := []byte("new_value1")
	err = btree.Put(key1, newValue1)
	require.NoError(t, err)

	result, err = btree.Get(key1)
	require.NoError(t, err)
	assert.Equal(t, newValue1, result)

	// 测试删除
	err = btree.Delete(key1)
	require.NoError(t, err)

	_, err = btree.Get(key1)
	assert.Error(t, err)
}

// TestBTreeMultipleRecords 测试多条记录操作
func TestBTreeMultipleRecords(t *testing.T) {
	btree, testDir := setupBTreeTest(t)
	defer cleanupBTreeTest(btree, testDir)

	// 插入多条记录
	recordCount := 100
	for i := 0; i < recordCount; i++ {
		key := []byte(fmt.Sprintf("key_%04d", i))
		value := []byte(fmt.Sprintf("value_%04d", i))
		err := btree.Put(key, value)
		require.NoError(t, err)
	}

	// 验证所有记录
	for i := 0; i < recordCount; i++ {
		key := []byte(fmt.Sprintf("key_%04d", i))
		expectedValue := []byte(fmt.Sprintf("value_%04d", i))

		value, err := btree.Get(key)
		require.NoError(t, err)
		assert.Equal(t, expectedValue, value)
	}

	// 测试统计信息
	stats := btree.GetStats()
	assert.Equal(t, int64(recordCount), stats.Inserts.Load())
	assert.Equal(t, int64(recordCount), stats.Selects.Load())
}

// TestBTreeRangeQuery 测试范围查询
func TestBTreeRangeQuery(t *testing.T) {
	btree, testDir := setupBTreeTest(t)
	defer cleanupBTreeTest(btree, testDir)

	// 插入有序数据
	recordCount := 50
	for i := 0; i < recordCount; i++ {
		key := []byte(fmt.Sprintf("key_%04d", i))
		value := []byte(fmt.Sprintf("value_%04d", i))
		err := btree.Put(key, value)
		require.NoError(t, err)
	}

	// 测试范围查询
	startKey := []byte("key_0010")
	endKey := []byte("key_0020")
	results, err := btree.Scan(startKey, endKey, 20)
	require.NoError(t, err)

	// 验证结果数量和顺序
	assert.True(t, len(results) > 0)
	assert.True(t, len(results) <= 20)

	// 验证结果在范围内
	for _, record := range results {
		assert.True(t, string(record.Key) >= string(startKey))
		assert.True(t, string(record.Key) <= string(endKey))
	}

	// 验证结果有序
	for i := 1; i < len(results); i++ {
		assert.True(t, string(results[i-1].Key) <= string(results[i].Key))
	}
}

// TestBTreePageSplit 测试页面分裂
func TestBTreePageSplit(t *testing.T) {
	btree, testDir := setupBTreeTest(t)
	defer cleanupBTreeTest(btree, testDir)

	// 插入足够多的数据触发分裂
	// 每条记录大约20字节，16KB页面可以容纳约800条记录
	recordCount := 1000
	for i := 0; i < recordCount; i++ {
		key := []byte(fmt.Sprintf("key_%06d", i))
		value := []byte(fmt.Sprintf("value_%06d", i))
		err := btree.Put(key, value)
		require.NoError(t, err)
	}

	// 验证所有数据仍然可以查找到
	for i := 0; i < recordCount; i++ {
		key := []byte(fmt.Sprintf("key_%06d", i))
		expectedValue := []byte(fmt.Sprintf("value_%06d", i))

		value, err := btree.Get(key)
		require.NoError(t, err)
		assert.Equal(t, expectedValue, value)
	}

	// 检查树高度是否增加
	stats := btree.GetStats()
	assert.True(t, stats.TreeHeight.Load() > 1)
}

// TestBTreeDeletion 测试删除操作
func TestBTreeDeletion(t *testing.T) {
	btree, testDir := setupBTreeTest(t)
	defer cleanupBTreeTest(btree, testDir)

	// 插入数据
	recordCount := 100
	for i := 0; i < recordCount; i++ {
		key := []byte(fmt.Sprintf("key_%04d", i))
		value := []byte(fmt.Sprintf("value_%04d", i))
		err := btree.Put(key, value)
		require.NoError(t, err)
	}

	// 删除一半数据
	for i := 0; i < recordCount/2; i++ {
		key := []byte(fmt.Sprintf("key_%04d", i))
		err := btree.Delete(key)
		require.NoError(t, err)
	}

	// 验证删除的数据不存在
	for i := 0; i < recordCount/2; i++ {
		key := []byte(fmt.Sprintf("key_%04d", i))
		_, err := btree.Get(key)
		assert.Error(t, err)
	}

	// 验证剩余数据仍然存在
	for i := recordCount / 2; i < recordCount; i++ {
		key := []byte(fmt.Sprintf("key_%04d", i))
		expectedValue := []byte(fmt.Sprintf("value_%04d", i))

		value, err := btree.Get(key)
		require.NoError(t, err)
		assert.Equal(t, expectedValue, value)
	}
}

// TestBTreeErrorHandling 测试错误处理
func TestBTreeErrorHandling(t *testing.T) {
	btree, testDir := setupBTreeTest(t)
	defer cleanupBTreeTest(btree, testDir)

	// 测试空键
	err := btree.Put([]byte{}, []byte("value"))
	assert.Error(t, err)

	err = btree.Put(nil, []byte("value"))
	assert.Error(t, err)

	_, err = btree.Get([]byte{})
	assert.Error(t, err)

	_, err = btree.Get(nil)
	assert.Error(t, err)

	err = btree.Delete([]byte{})
	assert.Error(t, err)

	err = btree.Delete(nil)
	assert.Error(t, err)

	// 测试删除不存在的键
	err = btree.Delete([]byte("nonexistent"))
	assert.Error(t, err)
}

// BenchmarkBTreeInsert B+树插入性能测试
func BenchmarkBTreeInsert(b *testing.B) {
	testDir := filepath.Join(os.TempDir(), "btree_bench")
	os.MkdirAll(testDir, 0755)
	defer os.RemoveAll(testDir)

	options := &BTreeOptions{
		WorkDir:        testDir,
		BufferPoolSize: 1000,
	}

	btree, err := NewBPlusTree(options)
	if err != nil {
		b.Fatal(err)
	}
	defer btree.Close()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			key := []byte(fmt.Sprintf("key_%010d", i))
			value := []byte(fmt.Sprintf("value_%010d", i))
			btree.Put(key, value)
			i++
		}
	})
}

// BenchmarkBTreeGet B+树查找性能测试
func BenchmarkBTreeGet(b *testing.B) {
	testDir := filepath.Join(os.TempDir(), "btree_bench")
	os.MkdirAll(testDir, 0755)
	defer os.RemoveAll(testDir)

	options := &BTreeOptions{
		WorkDir:        testDir,
		BufferPoolSize: 1000,
	}

	btree, err := NewBPlusTree(options)
	if err != nil {
		b.Fatal(err)
	}
	defer btree.Close()

	// 预先插入数据
	recordCount := 10000
	for i := 0; i < recordCount; i++ {
		key := []byte(fmt.Sprintf("key_%010d", i))
		value := []byte(fmt.Sprintf("value_%010d", i))
		btree.Put(key, value)
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			key := []byte(fmt.Sprintf("key_%010d", i%recordCount))
			btree.Get(key)
			i++
		}
	})
}
