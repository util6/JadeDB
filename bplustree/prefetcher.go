/*
JadeDB B+树预读器模块

预读器负责预测性地加载可能访问的页面，提高查询性能。
参考InnoDB的预读机制，实现智能的页面预读策略。

核心功能：
1. 顺序预读：检测顺序访问模式，预读后续页面
2. 随机预读：基于访问历史，预读相关页面
3. 自适应调整：根据命中率动态调整预读策略
4. 异步加载：后台异步加载页面，不阻塞主线程
5. 内存控制：控制预读页面数量，避免内存浪费

设计原理：
- 访问模式检测：分析页面访问序列，识别访问模式
- 预测算法：基于历史数据预测下一个访问的页面
- 异步加载：使用独立线程异步加载预读页面
- 命中率统计：跟踪预读命中率，优化预读策略
- 内存管理：限制预读页面数量，防止内存溢出

性能优化：
- 批量预读：一次预读多个连续页面
- 优先级队列：根据预测概率排序预读任务
- 缓存感知：避免预读已在缓存中的页面
- 自适应阈值：动态调整预读触发条件
*/

package bplustree

import (
	"container/list"
	"sync"
	"sync/atomic"
	"time"

	"github.com/util6/JadeDB/utils"
)

// 预读常量
const (
	// DefaultPrefetchSize 默认预读页面数量
	DefaultPrefetchSize = 8

	// MaxPrefetchSize 最大预读页面数量
	MaxPrefetchSize = 64

	// PrefetchThreshold 预读触发阈值
	// 当连续访问页面数量达到此阈值时触发预读
	PrefetchThreshold = 3

	// PrefetchQueueSize 预读队列大小
	PrefetchQueueSize = 256

	// PrefetchWorkers 预读工作线程数量
	PrefetchWorkers = 4
)

// Prefetcher 预读器
// 负责预测性地加载可能访问的页面
type Prefetcher struct {
	// 配置参数
	prefetchSize int         // 预读页面数量
	bufferPool   *BufferPool // 缓冲池引用

	// 访问模式跟踪
	accessHistory   *AccessHistory   // 访问历史记录
	patternDetector *PatternDetector // 模式检测器

	// 预读队列
	prefetchQueue chan *PrefetchTask // 预读任务队列
	workers       []*PrefetchWorker  // 预读工作线程

	// 统计信息
	totalPrefetch  atomic.Int64 // 总预读次数
	hitPrefetch    atomic.Int64 // 预读命中次数
	missedPrefetch atomic.Int64 // 预读未命中次数

	// 生命周期
	closer *utils.Closer // 优雅关闭
}

// PrefetchTask 预读任务
type PrefetchTask struct {
	PageID    uint64    // 要预读的页面ID
	Priority  int       // 优先级（越高越优先）
	Timestamp time.Time // 创建时间
}

// PrefetchWorker 预读工作线程
type PrefetchWorker struct {
	id         int           // 工作线程ID
	prefetcher *Prefetcher   // 预读器引用
	closer     *utils.Closer // 优雅关闭
}

// AccessHistory 访问历史记录
type AccessHistory struct {
	mutex       sync.RWMutex // 并发保护
	recentPages *list.List   // 最近访问的页面
	maxSize     int          // 最大记录数量
}

// AccessRecord 访问记录
type AccessRecord struct {
	PageID      uint64    // 页面ID
	Timestamp   time.Time // 访问时间
	SeqNum      uint64    // 序列号
	AccessCount int       // 访问次数
}

// PatternDetector 模式检测器
type PatternDetector struct {
	mutex          sync.RWMutex   // 并发保护
	sequentialRuns map[uint64]int // 顺序访问计数
	lastPageID     uint64         // 最后访问的页面ID
	sequenceLength int            // 当前序列长度
}

// NewPrefetcher 创建新的预读器
func NewPrefetcher(prefetchSize int, bufferPool *BufferPool) *Prefetcher {
	if prefetchSize <= 0 {
		prefetchSize = DefaultPrefetchSize
	}
	if prefetchSize > MaxPrefetchSize {
		prefetchSize = MaxPrefetchSize
	}

	pf := &Prefetcher{
		prefetchSize:    prefetchSize,
		bufferPool:      bufferPool,
		accessHistory:   NewAccessHistory(1000),
		patternDetector: NewPatternDetector(),
		prefetchQueue:   make(chan *PrefetchTask, PrefetchQueueSize),
		workers:         make([]*PrefetchWorker, PrefetchWorkers),
		closer:          utils.NewCloser(),
	}

	// 启动预读工作线程
	for i := 0; i < PrefetchWorkers; i++ {
		pf.workers[i] = &PrefetchWorker{
			id:         i,
			prefetcher: pf,
			closer:     utils.NewCloser(),
		}
		pf.closer.Add(1)
		go pf.workers[i].run()
	}

	return pf
}

// NewAccessHistory 创建访问历史记录
func NewAccessHistory(maxSize int) *AccessHistory {
	return &AccessHistory{
		recentPages: list.New(),
		maxSize:     maxSize,
	}
}

// NewPatternDetector 创建模式检测器
func NewPatternDetector() *PatternDetector {
	return &PatternDetector{
		sequentialRuns: make(map[uint64]int),
	}
}

// OnPageAccess 页面访问事件处理
func (pf *Prefetcher) OnPageAccess(pageID uint64) {
	// 记录访问历史
	pf.accessHistory.RecordAccess(pageID)

	// 检测访问模式
	pattern := pf.patternDetector.DetectPattern(pageID)

	// 根据模式触发预读
	switch pattern {
	case SequentialPattern:
		pf.triggerSequentialPrefetch(pageID)
	case RandomPattern:
		pf.triggerRandomPrefetch(pageID)
	}
}

// triggerSequentialPrefetch 触发顺序预读
func (pf *Prefetcher) triggerSequentialPrefetch(pageID uint64) {
	// 预读后续连续页面
	for i := 1; i <= pf.prefetchSize; i++ {
		nextPageID := pageID + uint64(i)
		task := &PrefetchTask{
			PageID:    nextPageID,
			Priority:  pf.prefetchSize - i + 1, // 越近的页面优先级越高
			Timestamp: time.Now(),
		}

		select {
		case pf.prefetchQueue <- task:
			pf.totalPrefetch.Add(1)
		default:
			// 队列满，跳过
		}
	}
}

// triggerRandomPrefetch 触发随机预读
func (pf *Prefetcher) triggerRandomPrefetch(pageID uint64) {
	// 基于访问历史预测可能访问的页面
	candidates := pf.accessHistory.GetRelatedPages(pageID, pf.prefetchSize)

	for i, candidateID := range candidates {
		task := &PrefetchTask{
			PageID:    candidateID,
			Priority:  len(candidates) - i, // 相关性越高优先级越高
			Timestamp: time.Now(),
		}

		select {
		case pf.prefetchQueue <- task:
			pf.totalPrefetch.Add(1)
		default:
			// 队列满，跳过
		}
	}
}

// RecordAccess 记录页面访问
func (ah *AccessHistory) RecordAccess(pageID uint64) {
	ah.mutex.Lock()
	defer ah.mutex.Unlock()

	// 查找是否已存在该页面的记录
	for e := ah.recentPages.Front(); e != nil; e = e.Next() {
		record := e.Value.(*AccessRecord)
		if record.PageID == pageID {
			// 更新现有记录
			record.Timestamp = time.Now()
			record.AccessCount++

			// 将记录移到前面（LRU策略）
			ah.recentPages.MoveToFront(e)
			return
		}
	}

	// 添加新的访问记录
	record := &AccessRecord{
		PageID:      pageID,
		Timestamp:   time.Now(),
		SeqNum:      uint64(ah.recentPages.Len()),
		AccessCount: 1,
	}

	ah.recentPages.PushFront(record)

	// 限制历史记录大小
	if ah.recentPages.Len() > ah.maxSize {
		ah.recentPages.Remove(ah.recentPages.Back())
	}
}

// GetRelatedPages 获取相关页面
// 实现智能的相关性算法，基于时间局部性、空间局部性和访问频率
func (ah *AccessHistory) GetRelatedPages(pageID uint64, maxCount int) []uint64 {
	ah.mutex.RLock()
	defer ah.mutex.RUnlock()

	// 候选页面及其相关性分数
	candidates := make(map[uint64]float64)

	// 当前时间，用于计算时间衰减
	now := time.Now()

	// 遍历访问历史，计算相关性分数
	for e := ah.recentPages.Front(); e != nil; e = e.Next() {
		record := e.Value.(*AccessRecord)
		if record.PageID == pageID {
			continue // 跳过自己
		}

		// 计算相关性分数
		score := ah.calculateRelatedness(pageID, record, now)
		if score > 0 {
			candidates[record.PageID] = score
		}
	}

	// 按分数排序并选择前maxCount个
	return ah.selectTopCandidates(candidates, maxCount)
}

// calculateRelatedness 计算页面相关性分数
func (ah *AccessHistory) calculateRelatedness(targetPageID uint64, record *AccessRecord, now time.Time) float64 {
	var score float64

	// 1. 时间局部性：最近访问的页面分数更高
	timeDiff := now.Sub(record.Timestamp)
	timeScore := 1.0 / (1.0 + timeDiff.Seconds()/3600.0) // 1小时衰减

	// 2. 空间局部性：相邻页面分数更高
	distance := int64(targetPageID) - int64(record.PageID)
	if distance < 0 {
		distance = -distance
	}
	spatialScore := 1.0 / (1.0 + float64(distance)/10.0) // 距离10个页面内有较高分数

	// 3. 访问频率：频繁访问的页面分数更高
	frequencyScore := float64(record.AccessCount) / 10.0 // 访问次数归一化
	if frequencyScore > 1.0 {
		frequencyScore = 1.0
	}

	// 综合分数：加权平均
	score = 0.4*timeScore + 0.4*spatialScore + 0.2*frequencyScore

	return score
}

// selectTopCandidates 选择分数最高的候选页面
func (ah *AccessHistory) selectTopCandidates(candidates map[uint64]float64, maxCount int) []uint64 {
	// 创建分数-页面ID对的切片
	type candidate struct {
		pageID uint64
		score  float64
	}

	var candidateList []candidate
	for pageID, score := range candidates {
		candidateList = append(candidateList, candidate{pageID: pageID, score: score})
	}

	// 按分数降序排序
	for i := 0; i < len(candidateList)-1; i++ {
		for j := i + 1; j < len(candidateList); j++ {
			if candidateList[i].score < candidateList[j].score {
				candidateList[i], candidateList[j] = candidateList[j], candidateList[i]
			}
		}
	}

	// 选择前maxCount个
	var result []uint64
	for i := 0; i < len(candidateList) && i < maxCount; i++ {
		result = append(result, candidateList[i].pageID)
	}

	return result
}

// AccessPattern 访问模式枚举
type AccessPattern int

const (
	UnknownPattern AccessPattern = iota
	SequentialPattern
	RandomPattern
)

// DetectPattern 检测访问模式
func (pd *PatternDetector) DetectPattern(pageID uint64) AccessPattern {
	pd.mutex.Lock()
	defer pd.mutex.Unlock()

	// 检查是否为顺序访问
	if pd.lastPageID != 0 && pageID == pd.lastPageID+1 {
		pd.sequenceLength++
		if pd.sequenceLength >= PrefetchThreshold {
			pd.lastPageID = pageID
			return SequentialPattern
		}
	} else {
		pd.sequenceLength = 1
	}

	pd.lastPageID = pageID
	return RandomPattern
}

// run 预读工作线程主循环
func (pw *PrefetchWorker) run() {
	defer pw.prefetcher.closer.Done()

	for {
		select {
		case task := <-pw.prefetcher.prefetchQueue:
			pw.processPrefetchTask(task)

		case <-pw.prefetcher.closer.CloseSignal:
			return
		}
	}
}

// processPrefetchTask 处理预读任务
func (pw *PrefetchWorker) processPrefetchTask(task *PrefetchTask) {
	// 检查页面是否已在缓冲池中
	// 使用非阻塞的Contains方法避免重复加载
	if pw.prefetcher.bufferPool.Contains(task.PageID) {
		// 页面已在缓存中，跳过预读
		return
	}

	// 异步加载页面到缓冲池
	_, err := pw.prefetcher.bufferPool.GetPage(task.PageID)
	if err != nil {
		// 预读失败
		pw.prefetcher.missedPrefetch.Add(1)
	} else {
		// 预读成功
		pw.prefetcher.hitPrefetch.Add(1)
	}
}

// GetStats 获取预读统计信息
func (pf *Prefetcher) GetStats() map[string]interface{} {
	total := pf.totalPrefetch.Load()
	hit := pf.hitPrefetch.Load()

	var hitRatio float64
	if total > 0 {
		hitRatio = float64(hit) / float64(total)
	}

	return map[string]interface{}{
		"total_prefetch":  total,
		"hit_prefetch":    hit,
		"missed_prefetch": pf.missedPrefetch.Load(),
		"hit_ratio":       hitRatio,
		"queue_size":      len(pf.prefetchQueue),
	}
}

// Close 关闭预读器
func (pf *Prefetcher) Close() error {
	// 停止所有工作线程
	pf.closer.Close()

	// 关闭队列
	close(pf.prefetchQueue)

	return nil
}
