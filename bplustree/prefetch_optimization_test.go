/*
JadeDB 缓存预取优化功能测试

本测试文件验证B+树预读器的优化功能，
确保智能相关性算法和BufferPool.Contains方法正确工作。

测试覆盖：
1. BufferPool.Contains方法
2. 智能相关性算法
3. 访问计数维护
4. 预读任务处理优化
5. 时间局部性和空间局部性
*/

package bplustree

import (
	"os"
	"testing"
	"time"

	"github.com/util6/JadeDB/utils"
)

// TestBufferPoolContains 测试BufferPool.Contains方法
func TestBufferPoolContains(t *testing.T) {
	// 创建临时目录
	tempDir, err := os.MkdirTemp("", "buffer_pool_contains_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// 创建页面管理器
	options := &BTreeOptions{
		WorkDir:        tempDir,
		PageSize:       16384,
		BufferPoolSize: 64,
	}
	pageManager, err := NewPageManager(options)
	if err != nil {
		t.Fatalf("Failed to create page manager: %v", err)
	}
	defer pageManager.Close()

	// 创建缓冲池
	bufferPool, err := NewBufferPool(64, pageManager) // 64页缓存
	if err != nil {
		t.Fatalf("Failed to create buffer pool: %v", err)
	}
	defer bufferPool.Close()

	// 测试不存在的页面
	t.Run("NonExistentPage", func(t *testing.T) {
		if bufferPool.Contains(999) {
			t.Errorf("Contains should return false for non-existent page")
		}
	})

	// 创建一个页面并加载到缓冲池
	testPageID := uint64(1)

	// 先创建一个有效的页面
	page := NewPage(testPageID, LeafPage)
	err = pageManager.WritePage(page)
	if err != nil {
		t.Fatalf("Failed to write page: %v", err)
	}

	// 从缓冲池获取页面
	page, err = bufferPool.GetPage(testPageID)
	if err != nil {
		t.Fatalf("Failed to get page: %v", err)
	}

	// 测试存在的页面
	t.Run("ExistentPage", func(t *testing.T) {
		if !bufferPool.Contains(testPageID) {
			t.Errorf("Contains should return true for existent page")
		}
	})

	// 释放页面
	bufferPool.PutPage(page)

	// 页面应该仍在缓存中
	t.Run("CachedPage", func(t *testing.T) {
		if !bufferPool.Contains(testPageID) {
			t.Errorf("Contains should return true for cached page")
		}
	})
}

// TestAccessHistoryRecordAccess 测试访问历史记录功能
func TestAccessHistoryRecordAccess(t *testing.T) {
	history := NewAccessHistory(10)

	// 测试新页面访问
	t.Run("NewPageAccess", func(t *testing.T) {
		history.RecordAccess(1)

		// 检查记录是否被添加
		if history.recentPages.Len() != 1 {
			t.Errorf("Expected 1 record, got %d", history.recentPages.Len())
		}

		// 检查记录内容
		record := history.recentPages.Front().Value.(*AccessRecord)
		if record.PageID != 1 {
			t.Errorf("Expected PageID 1, got %d", record.PageID)
		}
		if record.AccessCount != 1 {
			t.Errorf("Expected AccessCount 1, got %d", record.AccessCount)
		}
	})

	// 测试重复页面访问
	t.Run("RepeatedPageAccess", func(t *testing.T) {
		history.RecordAccess(1) // 再次访问页面1

		// 记录数量应该保持不变
		if history.recentPages.Len() != 1 {
			t.Errorf("Expected 1 record, got %d", history.recentPages.Len())
		}

		// 访问计数应该增加
		record := history.recentPages.Front().Value.(*AccessRecord)
		if record.AccessCount != 2 {
			t.Errorf("Expected AccessCount 2, got %d", record.AccessCount)
		}
	})

	// 测试多个页面访问
	t.Run("MultiplePageAccess", func(t *testing.T) {
		for i := uint64(2); i <= 5; i++ {
			history.RecordAccess(i)
		}

		// 应该有5条记录
		if history.recentPages.Len() != 5 {
			t.Errorf("Expected 5 records, got %d", history.recentPages.Len())
		}
	})
}

// TestSmartRelatednessAlgorithm 测试智能相关性算法
func TestSmartRelatednessAlgorithm(t *testing.T) {
	history := NewAccessHistory(100)

	// 模拟访问模式：顺序访问页面1-10
	for i := uint64(1); i <= 10; i++ {
		history.RecordAccess(i)
		time.Sleep(1 * time.Millisecond) // 确保时间戳不同
	}

	// 模拟频繁访问页面5
	for i := 0; i < 5; i++ {
		history.RecordAccess(5)
		time.Sleep(1 * time.Millisecond)
	}

	// 测试相关页面获取
	t.Run("SpatialLocality", func(t *testing.T) {
		// 获取与页面6相关的页面
		related := history.GetRelatedPages(6, 5)

		if len(related) == 0 {
			t.Errorf("Expected related pages, got none")
		}

		// 页面5应该有较高的相关性（空间局部性 + 高频访问）
		found := false
		for _, pageID := range related {
			if pageID == 5 {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("Expected page 5 to be in related pages due to spatial locality and frequency")
		}
	})

	// 测试时间局部性
	t.Run("TemporalLocality", func(t *testing.T) {
		// 最近访问页面20
		history.RecordAccess(20)

		// 获取与页面21相关的页面
		related := history.GetRelatedPages(21, 3)

		// 页面20应该在相关页面中（时间局部性）
		found := false
		for _, pageID := range related {
			if pageID == 20 {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("Expected page 20 to be in related pages due to temporal locality")
		}
	})
}

// TestPrefetchTaskProcessing 测试预读任务处理优化
func TestPrefetchTaskProcessing(t *testing.T) {
	// 创建临时目录
	tempDir, err := os.MkdirTemp("", "prefetch_task_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// 创建页面管理器
	options := &BTreeOptions{
		WorkDir:        tempDir,
		PageSize:       16384,
		BufferPoolSize: 64,
	}
	pageManager, err := NewPageManager(options)
	if err != nil {
		t.Fatalf("Failed to create page manager: %v", err)
	}
	defer pageManager.Close()

	// 创建缓冲池
	bufferPool, err := NewBufferPool(64, pageManager)
	if err != nil {
		t.Fatalf("Failed to create buffer pool: %v", err)
	}
	defer bufferPool.Close()

	// 创建预读器
	prefetcher := NewPrefetcher(8, bufferPool)
	defer prefetcher.Close()

	// 预先加载一个页面到缓冲池
	testPageID := uint64(1)

	// 先创建一个有效的页面
	page := NewPage(testPageID, LeafPage)
	err = pageManager.WritePage(page)
	if err != nil {
		t.Fatalf("Failed to write page: %v", err)
	}

	// 从缓冲池获取页面
	page, err = bufferPool.GetPage(testPageID)
	if err != nil {
		t.Fatalf("Failed to get page: %v", err)
	}
	bufferPool.PutPage(page)

	// 创建预读工作线程
	worker := &PrefetchWorker{
		id:         0,
		prefetcher: prefetcher,
		closer:     utils.NewCloser(),
	}

	// 测试已缓存页面的预读任务
	t.Run("CachedPageTask", func(t *testing.T) {
		task := &PrefetchTask{
			PageID:    testPageID,
			Priority:  1,
			Timestamp: time.Now(),
		}

		// 处理任务（应该跳过，因为页面已在缓存中）
		worker.processPrefetchTask(task)

		// 验证预读统计没有变化（因为跳过了）
		// 这里我们主要验证不会出现错误
	})

	// 测试未缓存页面的预读任务
	t.Run("UncachedPageTask", func(t *testing.T) {
		task := &PrefetchTask{
			PageID:    999, // 不存在的页面
			Priority:  1,
			Timestamp: time.Now(),
		}

		// 处理任务
		worker.processPrefetchTask(task)

		// 验证预读失败统计增加
		missedCount := prefetcher.missedPrefetch.Load()
		if missedCount == 0 {
			t.Errorf("Expected missed prefetch count to increase")
		}
	})
}

// TestPrefetcherIntegration 集成测试：验证整个预读流程
func TestPrefetcherIntegration(t *testing.T) {
	// 创建临时目录
	tempDir, err := os.MkdirTemp("", "prefetch_integration_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// 创建页面管理器
	options := &BTreeOptions{
		WorkDir:        tempDir,
		PageSize:       16384,
		BufferPoolSize: 64,
	}
	pageManager, err := NewPageManager(options)
	if err != nil {
		t.Fatalf("Failed to create page manager: %v", err)
	}
	defer pageManager.Close()

	// 创建缓冲池
	bufferPool, err := NewBufferPool(64, pageManager)
	if err != nil {
		t.Fatalf("Failed to create buffer pool: %v", err)
	}
	defer bufferPool.Close()

	// 创建预读器
	prefetcher := NewPrefetcher(4, bufferPool)
	defer prefetcher.Close()

	// 模拟顺序访问模式
	t.Run("SequentialAccess", func(t *testing.T) {
		// 连续访问页面1-5
		for i := uint64(1); i <= 5; i++ {
			prefetcher.OnPageAccess(i)
			time.Sleep(10 * time.Millisecond) // 给预读器时间处理
		}

		// 验证预读统计
		totalPrefetch := prefetcher.totalPrefetch.Load()
		if totalPrefetch == 0 {
			t.Errorf("Expected some prefetch operations, got %d", totalPrefetch)
		}
	})

	// 验证预读器状态
	t.Run("PrefetcherStats", func(t *testing.T) {
		stats := prefetcher.GetStats()

		// 验证统计信息结构
		if _, ok := stats["total_prefetch"]; !ok {
			t.Errorf("Expected total_prefetch in stats")
		}
		if _, ok := stats["hit_prefetch"]; !ok {
			t.Errorf("Expected hit_prefetch in stats")
		}
		if _, ok := stats["missed_prefetch"]; !ok {
			t.Errorf("Expected missed_prefetch in stats")
		}
	})
}
