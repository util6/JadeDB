/*
JadeDB B+树存储引擎测试模块

本模块包含B+树存储引擎的单元测试和集成测试。
测试覆盖页面管理、缓冲池、WAL、恢复等核心功能。

测试类别：
1. 单元测试：测试各个组件的基本功能
2. 集成测试：测试组件间的协作
3. 性能测试：测试性能指标
4. 压力测试：测试极限情况
5. 恢复测试：测试崩溃恢复功能
*/

package bplustree

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// 测试配置
var testOptions = &BTreeOptions{
	WorkDir:            "./test_data",
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

// setupTestDir 设置测试目录
func setupTestDir(t *testing.T) string {
	testDir := filepath.Join(os.TempDir(), "bplustree_test", t.Name())
	err := os.MkdirAll(testDir, 0755)
	require.NoError(t, err)
	return testDir
}

// cleanupTestDir 清理测试目录
func cleanupTestDir(testDir string) {
	os.RemoveAll(testDir)
}

// TestPageBasicOperations 测试页面基本操作
func TestPageBasicOperations(t *testing.T) {
	// 创建新页面
	page := NewPage(1, LeafPage)
	assert.NotNil(t, page)
	assert.Equal(t, uint64(1), page.ID)
	assert.Equal(t, LeafPage, page.Type)
	assert.True(t, page.Dirty)

	// 测试页面头部操作
	header := page.GetHeader()
	assert.Equal(t, uint64(1), header.PageID)
	assert.Equal(t, LeafPage, header.PageType)

	// 测试校验和
	page.UpdateChecksum()
	assert.True(t, page.VerifyChecksum())

	// 测试引用计数
	assert.False(t, page.IsPinned())
	page.Pin()
	assert.True(t, page.IsPinned())
	page.Unpin()
	assert.False(t, page.IsPinned())
}

// TestPageManager 测试页面管理器
func TestPageManager(t *testing.T) {
	testDir := setupTestDir(t)
	defer cleanupTestDir(testDir)

	options := *testOptions
	options.WorkDir = testDir

	// 创建页面管理器
	pm, err := NewPageManager(&options)
	require.NoError(t, err)
	defer pm.Close()

	// 测试页面分配
	page1, err := pm.AllocatePage(LeafPage)
	require.NoError(t, err)
	assert.NotNil(t, page1)
	assert.Equal(t, LeafPage, page1.Type)

	page2, err := pm.AllocatePage(InternalPage)
	require.NoError(t, err)
	assert.NotNil(t, page2)
	assert.Equal(t, InternalPage, page2.Type)
	assert.NotEqual(t, page1.ID, page2.ID)

	// 测试页面写入和读取
	err = pm.WritePage(page1)
	require.NoError(t, err)

	readPage, err := pm.ReadPage(page1.ID)
	require.NoError(t, err)
	assert.Equal(t, page1.ID, readPage.ID)
	assert.Equal(t, page1.Type, readPage.Type)

	// 测试页面释放
	err = pm.FreePage(page1.ID)
	require.NoError(t, err)

	// 测试统计信息
	stats := pm.GetStats()
	assert.NotNil(t, stats)
}

// TestBufferPool 测试缓冲池
func TestBufferPool(t *testing.T) {
	testDir := setupTestDir(t)
	defer cleanupTestDir(testDir)

	options := *testOptions
	options.WorkDir = testDir

	// 创建页面管理器和缓冲池
	pm, err := NewPageManager(&options)
	require.NoError(t, err)
	defer pm.Close()

	bp, err := NewBufferPool(10, pm)
	require.NoError(t, err)
	defer bp.Close()

	// 分配并写入一些页面
	var pages []*Page
	for i := 0; i < 5; i++ {
		page, err := pm.AllocatePage(LeafPage)
		require.NoError(t, err)
		err = pm.WritePage(page)
		require.NoError(t, err)
		pages = append(pages, page)
	}

	// 测试缓冲池获取页面
	for _, page := range pages {
		cachedPage, err := bp.GetPage(page.ID)
		require.NoError(t, err)
		assert.Equal(t, page.ID, cachedPage.ID)
		bp.PutPage(cachedPage)
	}

	// 测试缓存命中率
	stats := bp.GetStats()
	assert.NotNil(t, stats)
	hitRatio := stats["hit_ratio"].(float64)
	assert.True(t, hitRatio >= 0.0 && hitRatio <= 1.0)
}

// TestWALManager 测试WAL管理器
func TestWALManager(t *testing.T) {
	testDir := setupTestDir(t)
	defer cleanupTestDir(testDir)

	options := *testOptions
	options.WorkDir = testDir

	// 创建WAL管理器
	wm, err := NewWALManager(&options)
	require.NoError(t, err)
	defer wm.Close()

	// 测试写入日志记录
	record := &LogRecord{
		Type:   LogInsert,
		TxnID:  1,
		PageID: 100,
		Data:   []byte("test data"),
	}

	lsn, err := wm.WriteRecord(record)
	require.NoError(t, err)
	assert.Greater(t, lsn, uint64(0))

	// 测试刷新缓冲区
	err = wm.FlushBuffer()
	require.NoError(t, err)

	// 测试同步
	err = wm.Sync()
	require.NoError(t, err)

	// 测试检查点
	err = wm.CreateCheckpoint()
	require.NoError(t, err)

	// 测试统计信息
	stats := wm.GetStats()
	assert.NotNil(t, stats)
	assert.Greater(t, stats["total_records"].(int64), int64(0))
}

// TestRecoveryManager 测试恢复管理器
func TestRecoveryManager(t *testing.T) {
	testDir := setupTestDir(t)
	defer cleanupTestDir(testDir)

	options := *testOptions
	options.WorkDir = testDir

	// 创建页面管理器和恢复管理器
	pm, err := NewPageManager(&options)
	require.NoError(t, err)
	defer pm.Close()

	rm, err := NewRecoveryManager(&options, pm)
	require.NoError(t, err)

	// 创建WAL管理器并设置到恢复管理器
	wm, err := NewWALManager(&options)
	require.NoError(t, err)
	defer wm.Close()

	rm.SetWALManager(wm)

	// 测试恢复过程
	err = rm.Recovery()
	require.NoError(t, err)

	// 测试统计信息
	stats := rm.GetStats()
	assert.NotNil(t, stats)
}

// TestBPlusTreeBasic 测试B+树基本功能
func TestBPlusTreeBasic(t *testing.T) {
	testDir := setupTestDir(t)
	defer cleanupTestDir(testDir)

	options := *testOptions
	options.WorkDir = testDir

	// 创建B+树
	btree, err := NewBPlusTree(&options)
	require.NoError(t, err)
	defer btree.Close()

	// 测试统计信息
	stats := btree.GetStats()
	assert.NotNil(t, stats)
	assert.Equal(t, int32(1), stats.TreeHeight.Load())
}

// TestAdaptiveHashIndex 测试自适应哈希索引
func TestAdaptiveHashIndex(t *testing.T) {
	ahi := NewAdaptiveHashIndex()
	defer ahi.Close()

	key := []byte("test_key")
	pageID := uint64(100)
	offset := uint16(200)

	// 模拟多次访问使其成为热点
	for i := 0; i < HotKeyThreshold+1; i++ {
		_, _, found := ahi.Lookup(key)
		assert.False(t, found) // 初始时不存在
	}

	// 插入索引条目
	ahi.Insert(key, pageID, offset)

	// 查找应该命中
	foundPageID, foundOffset, found := ahi.Lookup(key)
	assert.True(t, found)
	assert.Equal(t, pageID, foundPageID)
	assert.Equal(t, offset, foundOffset)

	// 测试统计信息
	stats := ahi.GetStats()
	assert.NotNil(t, stats)
	assert.Greater(t, stats["total_lookups"].(int64), int64(0))
}

// TestPrefetcher 测试预读器
func TestPrefetcher(t *testing.T) {
	testDir := setupTestDir(t)
	defer cleanupTestDir(testDir)

	options := *testOptions
	options.WorkDir = testDir

	// 创建页面管理器和缓冲池
	pm, err := NewPageManager(&options)
	require.NoError(t, err)
	defer pm.Close()

	bp, err := NewBufferPool(50, pm)
	require.NoError(t, err)
	defer bp.Close()

	// 创建预读器
	pf := NewPrefetcher(8, bp)
	defer pf.Close()

	// 分配一些页面
	for i := 0; i < 10; i++ {
		page, err := pm.AllocatePage(LeafPage)
		require.NoError(t, err)
		err = pm.WritePage(page)
		require.NoError(t, err)
	}

	// 模拟顺序访问
	for i := uint64(1); i <= 5; i++ {
		pf.OnPageAccess(i)
	}

	// 等待预读完成
	time.Sleep(100 * time.Millisecond)

	// 测试统计信息
	stats := pf.GetStats()
	assert.NotNil(t, stats)
}

// BenchmarkPageOperations 页面操作性能测试
func BenchmarkPageOperations(b *testing.B) {
	page := NewPage(1, LeafPage)

	b.ResetTimer()
	b.Run("UpdateChecksum", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			page.UpdateChecksum()
		}
	})

	b.Run("VerifyChecksum", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			page.VerifyChecksum()
		}
	})
}

// BenchmarkBufferPool 缓冲池性能测试
func BenchmarkBufferPool(b *testing.B) {
	testDir := setupTestDir(&testing.T{})
	defer cleanupTestDir(testDir)

	options := *testOptions
	options.WorkDir = testDir

	pm, _ := NewPageManager(&options)
	defer pm.Close()

	bp, _ := NewBufferPool(1000, pm)
	defer bp.Close()

	// 预先分配一些页面
	var pageIDs []uint64
	for i := 0; i < 100; i++ {
		page, _ := pm.AllocatePage(LeafPage)
		pm.WritePage(page)
		pageIDs = append(pageIDs, page.ID)
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			pageID := pageIDs[i%len(pageIDs)]
			page, _ := bp.GetPage(pageID)
			bp.PutPage(page)
			i++
		}
	})
}
