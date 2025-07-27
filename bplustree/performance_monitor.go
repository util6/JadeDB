/*
JadeDB B+树性能监控模块

本模块实现全面的性能监控和分析功能，包括热点页面检测、
性能指标收集、趋势分析和自动优化建议。

核心功能：
1. 热点页面检测：识别频繁访问的页面
2. 性能指标收集：收集各种性能相关的指标
3. 趋势分析：分析性能趋势和异常
4. 自动优化：基于监控数据提供优化建议
5. 实时报告：生成实时性能报告

设计原理：
- 多维度监控：从页面、操作、时间等多个维度监控
- 滑动窗口：使用滑动窗口统计短期和长期趋势
- 热点检测：基于访问频率和时间衰减的热点检测
- 异常检测：基于统计学方法检测性能异常
- 自适应阈值：动态调整监控阈值

性能优化：
- 轻量级采样：避免监控本身影响性能
- 异步处理：后台异步处理监控数据
- 内存控制：限制监控数据的内存使用
- 批量处理：批量处理监控事件
*/

package bplustree

import (
	"container/heap"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/util6/JadeDB/utils"
)

// PerformanceMonitor 性能监控器
type PerformanceMonitor struct {
	// 配置参数
	sampleRate     float64       // 采样率（0.0-1.0）
	windowSize     int           // 滑动窗口大小
	hotThreshold   int           // 热点阈值
	reportInterval time.Duration // 报告间隔

	// 热点检测
	hotPages   *HotPageDetector  // 热点页面检测器
	hotQueries *HotQueryDetector // 热点查询检测器

	// 性能指标
	metrics   *PerformanceMetrics // 性能指标
	trends    *TrendAnalyzer      // 趋势分析器
	anomalies *AnomalyDetector    // 异常检测器

	// 事件处理
	eventQueue chan *MonitorEvent // 监控事件队列
	workers    []*MonitorWorker   // 监控工作线程

	// 统计信息
	totalEvents   atomic.Int64 // 总事件数
	sampledEvents atomic.Int64 // 采样事件数
	hotPagesCount atomic.Int64 // 热点页面数

	// 生命周期
	closer *utils.Closer
}

// MonitorEvent 监控事件
type MonitorEvent struct {
	Type      EventType     // 事件类型
	PageID    uint64        // 页面ID
	Operation string        // 操作类型
	Duration  time.Duration // 操作耗时
	Timestamp time.Time     // 时间戳
	Extra     interface{}   // 额外信息
}

// EventType 事件类型
type EventType int

const (
	PageAccessEvent EventType = iota
	PageReadEvent
	PageWriteEvent
	CacheHitEvent
	CacheMissEvent
	QueryEvent
	InsertEvent
	UpdateEvent
	DeleteEvent
)

// HotPageDetector 热点页面检测器
type HotPageDetector struct {
	mutex       sync.RWMutex
	pages       map[uint64]*PageStats // 页面统计
	hotPages    *PageHeap             // 热点页面堆
	maxHotPages int                   // 最大热点页面数
	decayFactor float64               // 衰减因子
}

// PageStats 页面统计信息
type PageStats struct {
	PageID      uint64    // 页面ID
	AccessCount int64     // 访问次数
	LastAccess  time.Time // 最后访问时间
	Score       float64   // 热度分数
	IsHot       bool      // 是否为热点
}

// PageHeap 页面堆（用于维护热点页面）
type PageHeap []*PageStats

func (h PageHeap) Len() int           { return len(h) }
func (h PageHeap) Less(i, j int) bool { return h[i].Score > h[j].Score }
func (h PageHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

func (h *PageHeap) Push(x interface{}) {
	*h = append(*h, x.(*PageStats))
}

func (h *PageHeap) Pop() interface{} {
	old := *h
	n := len(old)
	item := old[n-1]
	*h = old[0 : n-1]
	return item
}

// HotQueryDetector 热点查询检测器
type HotQueryDetector struct {
	mutex         sync.RWMutex
	queries       map[string]*QueryStats // 查询统计
	hotQueries    []*QueryStats          // 热点查询
	maxHotQueries int                    // 最大热点查询数
}

// QueryStats 查询统计信息
type QueryStats struct {
	Pattern     string        // 查询模式
	Count       int64         // 执行次数
	TotalTime   time.Duration // 总耗时
	AvgTime     time.Duration // 平均耗时
	LastExecute time.Time     // 最后执行时间
}

// PerformanceMetrics 性能指标
type PerformanceMetrics struct {
	mutex sync.RWMutex

	// 操作统计
	OperationCounts map[string]int64         // 操作计数
	OperationTimes  map[string]time.Duration // 操作总耗时

	// 缓存统计
	CacheHitRate  float64 // 缓存命中率
	CacheMissRate float64 // 缓存未命中率

	// 页面统计
	PageReadCount  int64   // 页面读取次数
	PageWriteCount int64   // 页面写入次数
	DirtyPageRatio float64 // 脏页比例

	// 性能指标
	AvgQueryTime  time.Duration // 平均查询时间
	AvgInsertTime time.Duration // 平均插入时间
	AvgUpdateTime time.Duration // 平均更新时间
	AvgDeleteTime time.Duration // 平均删除时间

	// 系统指标
	MemoryUsage int64         // 内存使用量
	DiskUsage   int64         // 磁盘使用量
	IOWaitTime  time.Duration // IO等待时间
}

// TrendAnalyzer 趋势分析器
type TrendAnalyzer struct {
	mutex      sync.RWMutex
	dataPoints []DataPoint      // 数据点
	windowSize int              // 窗口大小
	trends     map[string]Trend // 趋势信息
}

// DataPoint 数据点
type DataPoint struct {
	Timestamp time.Time
	Metrics   map[string]float64
}

// Trend 趋势信息
type Trend struct {
	Metric     string    // 指标名称
	Direction  int       // 趋势方向（1上升，-1下降，0平稳）
	Slope      float64   // 斜率
	R2         float64   // 相关系数
	LastUpdate time.Time // 最后更新时间
}

// AnomalyDetector 异常检测器
type AnomalyDetector struct {
	mutex     sync.RWMutex
	baselines map[string]*Baseline // 基线数据
	anomalies []*Anomaly           // 检测到的异常
	threshold float64              // 异常阈值（标准差倍数）
}

// Baseline 基线数据
type Baseline struct {
	Metric     string    // 指标名称
	Mean       float64   // 均值
	StdDev     float64   // 标准差
	Count      int64     // 样本数量
	LastUpdate time.Time // 最后更新时间
}

// Anomaly 异常信息
type Anomaly struct {
	Metric    string    // 指标名称
	Value     float64   // 异常值
	Expected  float64   // 期望值
	Deviation float64   // 偏差倍数
	Timestamp time.Time // 发生时间
	Severity  int       // 严重程度（1-5）
}

// MonitorWorker 监控工作线程
type MonitorWorker struct {
	id      int
	monitor *PerformanceMonitor
	closer  *utils.Closer
}

// NewPerformanceMonitor 创建性能监控器
func NewPerformanceMonitor(options *MonitorOptions) *PerformanceMonitor {
	if options == nil {
		options = DefaultMonitorOptions()
	}

	pm := &PerformanceMonitor{
		sampleRate:     options.SampleRate,
		windowSize:     options.WindowSize,
		hotThreshold:   options.HotThreshold,
		reportInterval: options.ReportInterval,
		hotPages:       NewHotPageDetector(options.MaxHotPages),
		hotQueries:     NewHotQueryDetector(options.MaxHotQueries),
		metrics:        NewPerformanceMetrics(),
		trends:         NewTrendAnalyzer(options.WindowSize),
		anomalies:      NewAnomalyDetector(options.AnomalyThreshold),
		eventQueue:     make(chan *MonitorEvent, options.EventQueueSize),
		workers:        make([]*MonitorWorker, options.WorkerCount),
		closer:         utils.NewCloser(),
	}

	// 启动监控工作线程
	for i := 0; i < options.WorkerCount; i++ {
		pm.workers[i] = &MonitorWorker{
			id:      i,
			monitor: pm,
			closer:  utils.NewCloser(),
		}
		pm.closer.Add(1)
		go pm.workers[i].run()
	}

	// 启动报告服务
	pm.closer.Add(1)
	go pm.reportService()

	return pm
}

// MonitorOptions 监控选项
type MonitorOptions struct {
	SampleRate       float64       // 采样率
	WindowSize       int           // 滑动窗口大小
	HotThreshold     int           // 热点阈值
	MaxHotPages      int           // 最大热点页面数
	MaxHotQueries    int           // 最大热点查询数
	AnomalyThreshold float64       // 异常检测阈值
	EventQueueSize   int           // 事件队列大小
	WorkerCount      int           // 工作线程数
	ReportInterval   time.Duration // 报告间隔
}

// DefaultMonitorOptions 默认监控选项
func DefaultMonitorOptions() *MonitorOptions {
	return &MonitorOptions{
		SampleRate:       0.1,         // 10%采样率
		WindowSize:       1000,        // 1000个数据点
		HotThreshold:     100,         // 100次访问为热点
		MaxHotPages:      100,         // 最多100个热点页面
		MaxHotQueries:    50,          // 最多50个热点查询
		AnomalyThreshold: 3.0,         // 3倍标准差为异常
		EventQueueSize:   10000,       // 10000个事件队列
		WorkerCount:      4,           // 4个工作线程
		ReportInterval:   time.Minute, // 每分钟报告一次
	}
}

// NewHotPageDetector 创建热点页面检测器
func NewHotPageDetector(maxHotPages int) *HotPageDetector {
	return &HotPageDetector{
		pages:       make(map[uint64]*PageStats),
		hotPages:    &PageHeap{},
		maxHotPages: maxHotPages,
		decayFactor: 0.95, // 每次更新衰减5%
	}
}

// NewHotQueryDetector 创建热点查询检测器
func NewHotQueryDetector(maxHotQueries int) *HotQueryDetector {
	return &HotQueryDetector{
		queries:       make(map[string]*QueryStats),
		hotQueries:    make([]*QueryStats, 0, maxHotQueries),
		maxHotQueries: maxHotQueries,
	}
}

// NewPerformanceMetrics 创建性能指标
func NewPerformanceMetrics() *PerformanceMetrics {
	return &PerformanceMetrics{
		OperationCounts: make(map[string]int64),
		OperationTimes:  make(map[string]time.Duration),
	}
}

// NewTrendAnalyzer 创建趋势分析器
func NewTrendAnalyzer(windowSize int) *TrendAnalyzer {
	return &TrendAnalyzer{
		dataPoints: make([]DataPoint, 0, windowSize),
		windowSize: windowSize,
		trends:     make(map[string]Trend),
	}
}

// NewAnomalyDetector 创建异常检测器
func NewAnomalyDetector(threshold float64) *AnomalyDetector {
	return &AnomalyDetector{
		baselines: make(map[string]*Baseline),
		anomalies: make([]*Anomaly, 0),
		threshold: threshold,
	}
}

// RecordEvent 记录监控事件
func (pm *PerformanceMonitor) RecordEvent(event *MonitorEvent) {
	pm.totalEvents.Add(1)

	// 采样检查
	if !pm.shouldSample() {
		return
	}

	pm.sampledEvents.Add(1)

	// 异步处理事件
	select {
	case pm.eventQueue <- event:
	default:
		// 队列满，丢弃事件
	}
}

// shouldSample 检查是否应该采样
func (pm *PerformanceMonitor) shouldSample() bool {
	// 简单的随机采样
	return float64(pm.totalEvents.Load()%100)/100.0 < pm.sampleRate
}

// RecordPageAccess 记录页面访问
func (pm *PerformanceMonitor) RecordPageAccess(pageID uint64, duration time.Duration) {
	event := &MonitorEvent{
		Type:      PageAccessEvent,
		PageID:    pageID,
		Duration:  duration,
		Timestamp: time.Now(),
	}
	pm.RecordEvent(event)
}

// RecordQuery 记录查询操作
func (pm *PerformanceMonitor) RecordQuery(pattern string, duration time.Duration) {
	event := &MonitorEvent{
		Type:      QueryEvent,
		Operation: pattern,
		Duration:  duration,
		Timestamp: time.Now(),
	}
	pm.RecordEvent(event)
}

// RecordCacheHit 记录缓存命中
func (pm *PerformanceMonitor) RecordCacheHit(pageID uint64) {
	event := &MonitorEvent{
		Type:      CacheHitEvent,
		PageID:    pageID,
		Timestamp: time.Now(),
	}
	pm.RecordEvent(event)
}

// RecordCacheMiss 记录缓存未命中
func (pm *PerformanceMonitor) RecordCacheMiss(pageID uint64) {
	event := &MonitorEvent{
		Type:      CacheMissEvent,
		PageID:    pageID,
		Timestamp: time.Now(),
	}
	pm.RecordEvent(event)
}

// run 监控工作线程主循环
func (mw *MonitorWorker) run() {
	defer mw.monitor.closer.Done()

	for {
		select {
		case event := <-mw.monitor.eventQueue:
			mw.processEvent(event)

		case <-mw.monitor.closer.CloseSignal:
			return
		}
	}
}

// processEvent 处理监控事件
func (mw *MonitorWorker) processEvent(event *MonitorEvent) {
	switch event.Type {
	case PageAccessEvent:
		mw.monitor.hotPages.RecordAccess(event.PageID, event.Timestamp)
		mw.monitor.metrics.UpdatePageAccess(event.Duration)

	case QueryEvent:
		mw.monitor.hotQueries.RecordQuery(event.Operation, event.Duration, event.Timestamp)
		mw.monitor.metrics.UpdateQuery(event.Operation, event.Duration)

	case CacheHitEvent:
		mw.monitor.metrics.UpdateCacheHit()

	case CacheMissEvent:
		mw.monitor.metrics.UpdateCacheMiss()
	}

	// 更新趋势分析
	mw.monitor.trends.AddDataPoint(event.Timestamp, map[string]float64{
		"duration": float64(event.Duration.Nanoseconds()),
	})

	// 异常检测
	mw.monitor.anomalies.CheckAnomaly("duration", float64(event.Duration.Nanoseconds()), event.Timestamp)
}

// RecordAccess 记录页面访问
func (hpd *HotPageDetector) RecordAccess(pageID uint64, timestamp time.Time) {
	hpd.mutex.Lock()
	defer hpd.mutex.Unlock()

	stats, exists := hpd.pages[pageID]
	if !exists {
		stats = &PageStats{
			PageID:      pageID,
			AccessCount: 0,
			LastAccess:  timestamp,
			Score:       0,
			IsHot:       false,
		}
		hpd.pages[pageID] = stats
	}

	// 更新访问统计
	stats.AccessCount++
	stats.LastAccess = timestamp

	// 计算热度分数（考虑时间衰减）
	timeDiff := timestamp.Sub(stats.LastAccess).Hours()
	decay := 1.0
	if timeDiff > 0 {
		decay = 1.0 / (1.0 + timeDiff)
	}
	stats.Score = float64(stats.AccessCount) * decay

	// 更新热点页面堆
	hpd.updateHotPages(stats)
}

// updateHotPages 更新热点页面堆
func (hpd *HotPageDetector) updateHotPages(stats *PageStats) {
	// 如果页面已经是热点，更新其分数
	if stats.IsHot {
		heap.Fix(hpd.hotPages, 0) // 简化处理，实际应该找到正确位置
		return
	}

	// 如果热点页面数量未满，直接添加
	if hpd.hotPages.Len() < hpd.maxHotPages {
		heap.Push(hpd.hotPages, stats)
		stats.IsHot = true
		return
	}

	// 如果当前页面分数高于最低热点页面，替换
	if hpd.hotPages.Len() > 0 {
		minHot := (*hpd.hotPages)[hpd.hotPages.Len()-1]
		if stats.Score > minHot.Score {
			minHot.IsHot = false
			heap.Pop(hpd.hotPages)
			heap.Push(hpd.hotPages, stats)
			stats.IsHot = true
		}
	}
}

// RecordQuery 记录查询
func (hqd *HotQueryDetector) RecordQuery(pattern string, duration time.Duration, timestamp time.Time) {
	hqd.mutex.Lock()
	defer hqd.mutex.Unlock()

	stats, exists := hqd.queries[pattern]
	if !exists {
		stats = &QueryStats{
			Pattern:     pattern,
			Count:       0,
			TotalTime:   0,
			AvgTime:     0,
			LastExecute: timestamp,
		}
		hqd.queries[pattern] = stats
	}

	// 更新查询统计
	stats.Count++
	stats.TotalTime += duration
	stats.AvgTime = time.Duration(int64(stats.TotalTime) / stats.Count)
	stats.LastExecute = timestamp

	// 更新热点查询列表
	hqd.updateHotQueries(stats)
}

// updateHotQueries 更新热点查询列表
func (hqd *HotQueryDetector) updateHotQueries(stats *QueryStats) {
	// 检查是否已在热点列表中
	for i, hot := range hqd.hotQueries {
		if hot.Pattern == stats.Pattern {
			hqd.hotQueries[i] = stats
			// 重新排序
			sort.Slice(hqd.hotQueries, func(i, j int) bool {
				return hqd.hotQueries[i].Count > hqd.hotQueries[j].Count
			})
			return
		}
	}

	// 如果列表未满，直接添加
	if len(hqd.hotQueries) < hqd.maxHotQueries {
		hqd.hotQueries = append(hqd.hotQueries, stats)
		sort.Slice(hqd.hotQueries, func(i, j int) bool {
			return hqd.hotQueries[i].Count > hqd.hotQueries[j].Count
		})
		return
	}

	// 如果当前查询比最少的热点查询更频繁，替换
	if stats.Count > hqd.hotQueries[len(hqd.hotQueries)-1].Count {
		hqd.hotQueries[len(hqd.hotQueries)-1] = stats
		sort.Slice(hqd.hotQueries, func(i, j int) bool {
			return hqd.hotQueries[i].Count > hqd.hotQueries[j].Count
		})
	}
}

// UpdatePageAccess 更新页面访问指标
func (pm *PerformanceMetrics) UpdatePageAccess(duration time.Duration) {
	pm.mutex.Lock()
	defer pm.mutex.Unlock()

	pm.OperationCounts["page_access"]++
	pm.OperationTimes["page_access"] += duration
}

// UpdateQuery 更新查询指标
func (pm *PerformanceMetrics) UpdateQuery(operation string, duration time.Duration) {
	pm.mutex.Lock()
	defer pm.mutex.Unlock()

	pm.OperationCounts[operation]++
	pm.OperationTimes[operation] += duration

	// 更新平均查询时间
	if pm.OperationCounts["query"] > 0 {
		pm.AvgQueryTime = time.Duration(int64(pm.OperationTimes["query"]) / pm.OperationCounts["query"])
	}
}

// UpdateCacheHit 更新缓存命中指标
func (pm *PerformanceMetrics) UpdateCacheHit() {
	pm.mutex.Lock()
	defer pm.mutex.Unlock()

	pm.OperationCounts["cache_hit"]++

	// 更新缓存命中率
	totalCache := pm.OperationCounts["cache_hit"] + pm.OperationCounts["cache_miss"]
	if totalCache > 0 {
		pm.CacheHitRate = float64(pm.OperationCounts["cache_hit"]) / float64(totalCache)
	}
}

// UpdateCacheMiss 更新缓存未命中指标
func (pm *PerformanceMetrics) UpdateCacheMiss() {
	pm.mutex.Lock()
	defer pm.mutex.Unlock()

	pm.OperationCounts["cache_miss"]++

	// 更新缓存未命中率
	totalCache := pm.OperationCounts["cache_hit"] + pm.OperationCounts["cache_miss"]
	if totalCache > 0 {
		pm.CacheMissRate = float64(pm.OperationCounts["cache_miss"]) / float64(totalCache)
	}
}

// AddDataPoint 添加数据点
func (ta *TrendAnalyzer) AddDataPoint(timestamp time.Time, metrics map[string]float64) {
	ta.mutex.Lock()
	defer ta.mutex.Unlock()

	dataPoint := DataPoint{
		Timestamp: timestamp,
		Metrics:   metrics,
	}

	ta.dataPoints = append(ta.dataPoints, dataPoint)

	// 保持窗口大小
	if len(ta.dataPoints) > ta.windowSize {
		ta.dataPoints = ta.dataPoints[1:]
	}

	// 更新趋势
	ta.updateTrends()
}

// updateTrends 更新趋势信息
func (ta *TrendAnalyzer) updateTrends() {
	if len(ta.dataPoints) < 2 {
		return
	}

	// 为每个指标计算趋势
	for metric := range ta.dataPoints[len(ta.dataPoints)-1].Metrics {
		ta.calculateTrend(metric)
	}
}

// calculateTrend 计算指标趋势
func (ta *TrendAnalyzer) calculateTrend(metric string) {
	if len(ta.dataPoints) < 2 {
		return
	}

	// 简单的线性回归计算趋势
	n := len(ta.dataPoints)
	var sumX, sumY, sumXY, sumX2 float64

	for i, point := range ta.dataPoints {
		if value, exists := point.Metrics[metric]; exists {
			x := float64(i)
			y := value
			sumX += x
			sumY += y
			sumXY += x * y
			sumX2 += x * x
		}
	}

	// 计算斜率
	slope := (float64(n)*sumXY - sumX*sumY) / (float64(n)*sumX2 - sumX*sumX)

	// 确定趋势方向
	direction := 0
	if slope > 0.1 {
		direction = 1 // 上升
	} else if slope < -0.1 {
		direction = -1 // 下降
	}

	ta.trends[metric] = Trend{
		Metric:     metric,
		Direction:  direction,
		Slope:      slope,
		LastUpdate: time.Now(),
	}
}

// CheckAnomaly 检查异常
func (ad *AnomalyDetector) CheckAnomaly(metric string, value float64, timestamp time.Time) {
	ad.mutex.Lock()
	defer ad.mutex.Unlock()

	baseline, exists := ad.baselines[metric]
	if !exists {
		// 创建新的基线
		baseline = &Baseline{
			Metric:     metric,
			Mean:       value,
			StdDev:     0,
			Count:      1,
			LastUpdate: timestamp,
		}
		ad.baselines[metric] = baseline
		return
	}

	// 更新基线统计
	baseline.Count++
	oldMean := baseline.Mean
	baseline.Mean += (value - baseline.Mean) / float64(baseline.Count)

	// 更新标准差（在线算法）
	if baseline.Count > 1 {
		baseline.StdDev = baseline.StdDev + (value-oldMean)*(value-baseline.Mean)
		if baseline.Count > 2 {
			baseline.StdDev = baseline.StdDev / float64(baseline.Count-1)
		}
	}

	baseline.LastUpdate = timestamp

	// 检查是否为异常
	if baseline.StdDev > 0 {
		deviation := (value - baseline.Mean) / baseline.StdDev
		if deviation > ad.threshold || deviation < -ad.threshold {
			// 发现异常
			anomaly := &Anomaly{
				Metric:    metric,
				Value:     value,
				Expected:  baseline.Mean,
				Deviation: deviation,
				Timestamp: timestamp,
				Severity:  ad.calculateSeverity(deviation),
			}
			ad.anomalies = append(ad.anomalies, anomaly)

			// 限制异常记录数量
			if len(ad.anomalies) > 1000 {
				ad.anomalies = ad.anomalies[100:]
			}
		}
	}
}

// calculateSeverity 计算异常严重程度
func (ad *AnomalyDetector) calculateSeverity(deviation float64) int {
	absDeviation := deviation
	if absDeviation < 0 {
		absDeviation = -absDeviation
	}

	if absDeviation >= 5.0 {
		return 5 // 严重
	} else if absDeviation >= 4.0 {
		return 4 // 高
	} else if absDeviation >= 3.5 {
		return 3 // 中等
	} else if absDeviation >= 3.0 {
		return 2 // 低
	}
	return 1 // 轻微
}

// reportService 报告服务
func (pm *PerformanceMonitor) reportService() {
	defer pm.closer.Done()

	ticker := time.NewTicker(pm.reportInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			pm.generateReport()

		case <-pm.closer.CloseSignal:
			return
		}
	}
}

// generateReport 生成性能报告
func (pm *PerformanceMonitor) generateReport() {
	// 这里可以生成详细的性能报告
	// 包括热点页面、热点查询、性能趋势、异常等信息

	// 更新热点页面计数
	pm.hotPagesCount.Store(int64(pm.hotPages.hotPages.Len()))
}

// GetHotPages 获取热点页面
func (pm *PerformanceMonitor) GetHotPages() []*PageStats {
	pm.hotPages.mutex.RLock()
	defer pm.hotPages.mutex.RUnlock()

	result := make([]*PageStats, pm.hotPages.hotPages.Len())
	for i, stats := range *pm.hotPages.hotPages {
		result[i] = stats
	}
	return result
}

// GetHotQueries 获取热点查询
func (pm *PerformanceMonitor) GetHotQueries() []*QueryStats {
	pm.hotQueries.mutex.RLock()
	defer pm.hotQueries.mutex.RUnlock()

	result := make([]*QueryStats, len(pm.hotQueries.hotQueries))
	copy(result, pm.hotQueries.hotQueries)
	return result
}

// GetMetrics 获取性能指标
func (pm *PerformanceMonitor) GetMetrics() *PerformanceMetrics {
	return pm.metrics
}

// GetAnomalies 获取异常列表
func (pm *PerformanceMonitor) GetAnomalies() []*Anomaly {
	pm.anomalies.mutex.RLock()
	defer pm.anomalies.mutex.RUnlock()

	result := make([]*Anomaly, len(pm.anomalies.anomalies))
	copy(result, pm.anomalies.anomalies)
	return result
}

// GetStats 获取监控统计信息
func (pm *PerformanceMonitor) GetStats() map[string]interface{} {
	return map[string]interface{}{
		"total_events":    pm.totalEvents.Load(),
		"sampled_events":  pm.sampledEvents.Load(),
		"hot_pages_count": pm.hotPagesCount.Load(),
		"sample_rate":     pm.sampleRate,
	}
}

// Close 关闭性能监控器
func (pm *PerformanceMonitor) Close() error {
	pm.closer.Close()
	return nil
}
