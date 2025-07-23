/*
JadeDB B+树缓冲池模块

缓冲池是B+树存储引擎的核心组件，负责页面的内存缓存管理。
参考InnoDB的缓冲池设计，实现高效的页面缓存机制。

核心功能：
1. 页面缓存：将热点页面缓存在内存中
2. LRU管理：使用LRU算法管理页面替换
3. 脏页管理：跟踪和刷新修改过的页面
4. 预读机制：预测性地加载相关页面
5. 并发控制：支持多线程并发访问

设计原理：
- LRU链表：最近最少使用的页面优先被替换
- 哈希表：O(1)时间复杂度的页面查找
- 脏页列表：跟踪需要写回磁盘的页面
- 分区设计：减少锁竞争，提高并发性能
- 预读缓冲：异步预读相关页面

性能优化：
- 分区锁：将缓冲池分为多个分区，减少锁竞争
- 批量刷新：批量写入脏页，提高I/O效率
- 自适应预读：根据访问模式调整预读策略
- 内存预分配：预先分配页面内存，减少分配开销
*/

package bplustree

import (
	"container/list"
	"sync"
	"sync/atomic"
	"time"

	"github.com/util6/JadeDB/utils"
)

// 缓冲池常量
const (
	// DefaultBufferPoolSize 默认缓冲池大小（页面数量）
	DefaultBufferPoolSize = 1024

	// MinBufferPoolSize 最小缓冲池大小
	MinBufferPoolSize = 64

	// MaxBufferPoolSize 最大缓冲池大小
	MaxBufferPoolSize = 1024 * 1024

	// PartitionCount 缓冲池分区数量
	// 分区设计减少锁竞争，提高并发性能
	PartitionCount = 16

	// FlushThreshold 脏页刷新阈值
	// 当脏页比例超过此阈值时触发刷新
	FlushThreshold = 0.8

	// FlushBatchSize 批量刷新大小
	FlushBatchSize = 64
)

// BufferPool 缓冲池管理器
// 负责页面的内存缓存、LRU管理和脏页刷新
type BufferPool struct {
	// 基本配置
	size        int          // 缓冲池大小（页面数量）
	pageManager *PageManager // 页面管理器

	// 分区管理
	partitions []*BufferPartition // 缓冲池分区
	partMask   uint64             // 分区掩码，用于快速计算分区索引

	// 全局统计
	hitCount   atomic.Int64 // 缓存命中次数
	missCount  atomic.Int64 // 缓存未命中次数
	dirtyCount atomic.Int64 // 脏页数量
	totalCount atomic.Int64 // 总页面数量

	// 后台服务
	flushChan chan *Page    // 脏页刷新通道
	closer    *utils.Closer // 优雅关闭

	// 性能优化
	prefetcher *Prefetcher // 预读器
}

// BufferPartition 缓冲池分区
// 将缓冲池分为多个分区，减少锁竞争
type BufferPartition struct {
	// 并发控制
	mutex sync.RWMutex // 分区锁

	// 页面管理
	pages     map[uint64]*Page // 页面哈希表
	lruList   *list.List       // LRU链表
	dirtyList *list.List       // 脏页链表

	// 统计信息
	size       int // 分区大小
	capacity   int // 分区容量
	dirtyCount int // 脏页数量
}

// BufferLRUNode LRU链表节点（缓冲池专用）
type BufferLRUNode struct {
	page    *Page         // 页面指针
	element *list.Element // 链表元素
}

// NewBufferPool 创建新的缓冲池
func NewBufferPool(size int, pageManager *PageManager) (*BufferPool, error) {
	// 参数验证
	if size < MinBufferPoolSize {
		size = MinBufferPoolSize
	}
	if size > MaxBufferPoolSize {
		size = MaxBufferPoolSize
	}

	bp := &BufferPool{
		size:        size,
		pageManager: pageManager,
		partitions:  make([]*BufferPartition, PartitionCount),
		partMask:    PartitionCount - 1,
		flushChan:   make(chan *Page, FlushBatchSize),
		closer:      utils.NewCloser(),
	}

	// 初始化分区
	partitionSize := size / PartitionCount
	for i := 0; i < PartitionCount; i++ {
		bp.partitions[i] = &BufferPartition{
			pages:     make(map[uint64]*Page),
			lruList:   list.New(),
			dirtyList: list.New(),
			capacity:  partitionSize,
		}
	}

	// 启动后台服务
	bp.startBackgroundServices()

	return bp, nil
}

// GetPage 获取页面（优先从缓存获取）
func (bp *BufferPool) GetPage(pageID uint64) (*Page, error) {
	// 计算分区索引
	partIndex := bp.getPartitionIndex(pageID)
	partition := bp.partitions[partIndex]

	partition.mutex.Lock()
	defer partition.mutex.Unlock()

	// 检查页面是否在缓存中
	if page, exists := partition.pages[pageID]; exists {
		// 缓存命中
		bp.hitCount.Add(1)

		// 更新LRU位置
		bp.moveToFront(partition, page)

		// 增加引用计数
		page.Pin()

		return page, nil
	}

	// 缓存未命中，从磁盘读取
	bp.missCount.Add(1)

	page, err := bp.pageManager.ReadPage(pageID)
	if err != nil {
		return nil, err
	}

	// 将页面加入缓存
	if err := bp.addToCache(partition, page); err != nil {
		return nil, err
	}

	// 增加引用计数
	page.Pin()

	return page, nil
}

// PutPage 将页面放回缓冲池
func (bp *BufferPool) PutPage(page *Page) {
	// 减少引用计数
	page.Unpin()

	// 如果页面被修改，标记为脏页
	if page.Dirty {
		bp.markDirty(page)
	}
}

// FlushPage 刷新单个页面到磁盘
func (bp *BufferPool) FlushPage(page *Page) error {
	if !page.Dirty {
		return nil
	}

	// 写入磁盘
	if err := bp.pageManager.WritePage(page); err != nil {
		return err
	}

	// 从脏页列表中移除
	bp.removeDirty(page)

	return nil
}

// FlushAll 刷新所有脏页到磁盘
func (bp *BufferPool) FlushAll() error {
	for _, partition := range bp.partitions {
		partition.mutex.Lock()

		// 收集所有脏页
		dirtyPages := make([]*Page, 0, partition.dirtyCount)
		for e := partition.dirtyList.Front(); e != nil; e = e.Next() {
			page := e.Value.(*Page)
			dirtyPages = append(dirtyPages, page)
		}

		partition.mutex.Unlock()

		// 批量刷新脏页
		for _, page := range dirtyPages {
			if err := bp.FlushPage(page); err != nil {
				return err
			}
		}
	}

	return nil
}

// getPartitionIndex 计算页面所属的分区索引
func (bp *BufferPool) getPartitionIndex(pageID uint64) int {
	// 使用页面ID的低位作为分区索引
	return int(pageID & bp.partMask)
}

// addToCache 将页面添加到缓存
func (bp *BufferPool) addToCache(partition *BufferPartition, page *Page) error {
	// 检查是否需要淘汰页面
	if partition.size >= partition.capacity {
		if err := bp.evictLRU(partition); err != nil {
			return err
		}
	}

	// 添加到哈希表
	partition.pages[page.ID] = page

	// 添加到LRU链表头部
	element := partition.lruList.PushFront(page)
	page.lruNode = &BufferLRUNode{
		page:    page,
		element: element,
	}

	// 更新统计
	partition.size++
	bp.totalCount.Add(1)
	page.inBuffer = true

	return nil
}

// evictLRU 淘汰LRU页面
func (bp *BufferPool) evictLRU(partition *BufferPartition) error {
	// 从LRU链表尾部获取最久未使用的页面
	element := partition.lruList.Back()
	if element == nil {
		return utils.ErrBufferPoolFull
	}

	page := element.Value.(*Page)

	// 检查页面是否被引用
	if page.IsPinned() {
		// 页面被引用，无法淘汰
		return utils.ErrPagePinned
	}

	// 如果是脏页，先刷新到磁盘
	if page.Dirty {
		if err := bp.pageManager.WritePage(page); err != nil {
			return err
		}
		bp.removeDirtyUnsafe(partition, page)
	}

	// 从缓存中移除
	delete(partition.pages, page.ID)
	partition.lruList.Remove(element)

	// 更新统计
	partition.size--
	bp.totalCount.Add(-1)
	page.inBuffer = false

	return nil
}

// moveToFront 将页面移动到LRU链表头部
func (bp *BufferPool) moveToFront(partition *BufferPartition, page *Page) {
	if page.lruNode != nil {
		if bufferNode, ok := page.lruNode.(*BufferLRUNode); ok && bufferNode.element != nil {
			partition.lruList.MoveToFront(bufferNode.element)
		}
	}
	page.LastUsed = time.Now()
}

// markDirty 标记页面为脏页
func (bp *BufferPool) markDirty(page *Page) {
	partIndex := bp.getPartitionIndex(page.ID)
	partition := bp.partitions[partIndex]

	partition.mutex.Lock()
	defer partition.mutex.Unlock()

	// 如果页面不在脏页列表中，添加进去
	if !bp.isInDirtyList(partition, page) {
		partition.dirtyList.PushBack(page)
		partition.dirtyCount++
		bp.dirtyCount.Add(1)
	}
}

// removeDirty 从脏页列表中移除页面
func (bp *BufferPool) removeDirty(page *Page) {
	partIndex := bp.getPartitionIndex(page.ID)
	partition := bp.partitions[partIndex]

	partition.mutex.Lock()
	defer partition.mutex.Unlock()

	bp.removeDirtyUnsafe(partition, page)
}

// removeDirtyUnsafe 从脏页列表中移除页面（不加锁版本）
func (bp *BufferPool) removeDirtyUnsafe(partition *BufferPartition, page *Page) {
	// 遍历脏页列表，找到并移除页面
	for e := partition.dirtyList.Front(); e != nil; e = e.Next() {
		if e.Value.(*Page).ID == page.ID {
			partition.dirtyList.Remove(e)
			partition.dirtyCount--
			bp.dirtyCount.Add(-1)
			page.Dirty = false
			break
		}
	}
}

// isInDirtyList 检查页面是否在脏页列表中
func (bp *BufferPool) isInDirtyList(partition *BufferPartition, page *Page) bool {
	for e := partition.dirtyList.Front(); e != nil; e = e.Next() {
		if e.Value.(*Page).ID == page.ID {
			return true
		}
	}
	return false
}

// startBackgroundServices 启动后台服务
func (bp *BufferPool) startBackgroundServices() {
	// 启动脏页刷新服务
	bp.closer.Add(1)
	go bp.flushService()

	// 启动统计收集服务
	bp.closer.Add(1)
	go bp.statsService()
}

// flushService 脏页刷新服务
func (bp *BufferPool) flushService() {
	defer bp.closer.Done()

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			// 检查脏页比例
			dirtyRatio := float64(bp.dirtyCount.Load()) / float64(bp.totalCount.Load())
			if dirtyRatio > FlushThreshold {
				bp.flushDirtyPages()
			}

		case <-bp.closer.CloseSignal:
			return
		}
	}
}

// flushDirtyPages 批量刷新脏页
func (bp *BufferPool) flushDirtyPages() {
	for _, partition := range bp.partitions {
		partition.mutex.Lock()

		// 收集一批脏页
		batch := make([]*Page, 0, FlushBatchSize)
		count := 0
		for e := partition.dirtyList.Front(); e != nil && count < FlushBatchSize; e = e.Next() {
			page := e.Value.(*Page)
			if !page.IsPinned() {
				batch = append(batch, page)
				count++
			}
		}

		partition.mutex.Unlock()

		// 批量刷新
		for _, page := range batch {
			bp.FlushPage(page)
		}
	}
}

// statsService 统计收集服务
func (bp *BufferPool) statsService() {
	defer bp.closer.Done()

	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			// 收集统计信息
			bp.collectStats()

		case <-bp.closer.CloseSignal:
			return
		}
	}
}

// collectStats 收集统计信息
func (bp *BufferPool) collectStats() {
	// 这里可以实现统计信息的收集和报告
	// 例如：缓存命中率、脏页比例、页面分布等
}

// GetStats 获取缓冲池统计信息
func (bp *BufferPool) GetStats() map[string]interface{} {
	hitCount := bp.hitCount.Load()
	missCount := bp.missCount.Load()
	totalAccess := hitCount + missCount

	var hitRatio float64
	if totalAccess > 0 {
		hitRatio = float64(hitCount) / float64(totalAccess)
	}

	return map[string]interface{}{
		"size":        bp.size,
		"total_pages": bp.totalCount.Load(),
		"dirty_pages": bp.dirtyCount.Load(),
		"hit_count":   hitCount,
		"miss_count":  missCount,
		"hit_ratio":   hitRatio,
	}
}

// Close 关闭缓冲池
func (bp *BufferPool) Close() error {
	// 停止后台服务
	bp.closer.Close()

	// 刷新所有脏页
	return bp.FlushAll()
}
