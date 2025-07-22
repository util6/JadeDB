/*
JadeDB 对象池管理模块

对象池是一种重要的性能优化技术，通过重用对象来减少内存分配和垃圾回收的开销。
在 JadeDB 中，对象池主要用于管理频繁创建和销毁的对象。

核心优势：
1. 减少内存分配：重用已有对象，避免频繁的 new 操作
2. 降低 GC 压力：减少垃圾对象的产生，降低 GC 频率和延迟
3. 提高性能：避免内存分配的系统调用开销
4. 内存局部性：重用的对象可能仍在 CPU 缓存中

设计原理：
- 基于 sync.Pool：利用 Go 标准库的高效实现
- 统计监控：收集使用统计，便于性能分析
- 类型安全：为不同类型提供专门的池
- 自动清理：Go 运行时会自动清理未使用的对象

适用场景：
- 频繁创建的临时对象（如 Entry、Buffer）
- 大小相对固定的对象
- 生命周期短暂的对象
- 对性能敏感的热点路径

使用注意：
- 对象重用前需要重置状态
- 不要长期持有池中的对象
- 避免在池中存储过大的对象
- 注意并发安全性

性能监控：
- 获取次数：跟踪对象池的使用频率
- 放回次数：监控对象的回收情况
- 创建次数：了解新对象的分配情况
- 池大小：监控池中对象的数量变化
*/

package utils

import (
	"sync"
	"sync/atomic"
)

// ObjectPool 提供通用的对象池功能。
// 它封装了 sync.Pool，并添加了统计和监控功能。
//
// 设计特点：
// 1. 泛型支持：可以存储任意类型的对象
// 2. 统计监控：收集详细的使用统计信息
// 3. 并发安全：基于 sync.Pool 的线程安全实现
// 4. 自动管理：Go 运行时自动管理池的大小
//
// 工作原理：
// - Get()：从池中获取对象，如果池为空则创建新对象
// - Put()：将对象放回池中，供后续重用
// - 统计信息通过原子操作更新，保证并发安全
//
// 内存管理：
// - sync.Pool 会在 GC 时清理未使用的对象
// - 不需要手动管理池的大小
// - 对象的生命周期由 Go 运行时控制
type ObjectPool struct {
	// 底层对象池

	// pool 是 Go 标准库提供的对象池。
	// 提供高效的对象存储和检索功能。
	// 支持并发访问，内部使用无锁算法优化。
	pool sync.Pool

	// 池标识

	// name 是对象池的名称，用于标识和调试。
	// 在日志记录和统计报告中使用。
	// 有助于区分不同用途的对象池。
	name string

	// 统计信息（使用原子操作）

	// stats 包含对象池的详细使用统计。
	// 所有字段都使用原子操作更新，保证并发安全。
	// 用于性能监控、调优和问题诊断。
	stats struct {
		// gets 记录从池中获取对象的总次数。
		// 反映对象池的使用频率。
		gets int64

		// puts 记录向池中放回对象的总次数。
		// 理想情况下应该接近 gets 的数量。
		puts int64

		// news 记录创建新对象的总次数。
		// 当池为空时会创建新对象。
		news int64

		// size 记录当前池中对象的估计数量。
		// 由于 sync.Pool 的特性，这只是一个近似值。
		size int64

		// maxSize 记录池中对象数量的历史最大值。
		// 用于了解池的峰值使用情况。
		maxSize int64
	}
}

// NewObjectPool 创建一个新的对象池实例。
// 这是对象池的构造函数，设置基本配置和统计监控。
//
// 参数说明：
// name: 对象池的名称，用于标识和调试
// newFunc: 创建新对象的工厂函数
//
// 返回值：
// 配置完成的对象池实例
//
// 实现细节：
// 1. 包装原始的工厂函数，添加统计功能
// 2. 使用原子操作更新统计计数器
// 3. 实现最大大小的无锁更新算法
//
// 统计功能：
// - 跟踪新对象的创建次数
// - 维护池大小的估计值
// - 记录历史最大池大小
func NewObjectPool(name string, newFunc func() interface{}) *ObjectPool {
	op := &ObjectPool{
		name: name,
		pool: sync.Pool{New: newFunc},
	}

	// 包装原始工厂函数，添加统计功能
	originalNew := newFunc
	op.pool.New = func() interface{} {
		// 原子递增新对象创建计数
		atomic.AddInt64(&op.stats.news, 1)
		atomic.AddInt64(&op.stats.size, 1)

		// 使用 CAS 循环更新最大大小记录
		// 这是一个无锁算法，避免使用互斥锁的开销
		for {
			current := atomic.LoadInt64(&op.stats.maxSize)
			newSize := atomic.LoadInt64(&op.stats.size)
			if newSize <= current || atomic.CompareAndSwapInt64(&op.stats.maxSize, current, newSize) {
				break
			}
		}

		return originalNew()
	}

	return op
}

// Get 从对象池中获取一个对象。
// 如果池中有可用对象则直接返回，否则创建新对象。
//
// 返回值：
// 从池中获取的对象，类型为 interface{}
//
// 性能特点：
// - 优先返回池中的现有对象
// - 池为空时自动创建新对象
// - 使用原子操作更新统计信息
// - 操作是线程安全的
//
// 使用注意：
// - 返回的对象可能包含之前使用的状态
// - 使用前应该重置对象状态
// - 不要假设对象的初始状态
func (op *ObjectPool) Get() interface{} {
	atomic.AddInt64(&op.stats.gets, 1)
	return op.pool.Get()
}

// Put 将对象放回对象池中供后续重用。
// 对象会被存储在池中，等待下次 Get() 调用时返回。
//
// 参数说明：
// obj: 要放回池中的对象
//
// 性能考虑：
// - 放回的对象应该处于可重用状态
// - 避免放回过大或包含敏感数据的对象
// - 使用原子操作更新统计信息
//
// 最佳实践：
// - 在使用完对象后及时放回池中
// - 确保对象不再被其他地方引用
// - 可以在 defer 语句中调用以确保执行
func (op *ObjectPool) Put(obj interface{}) {
	atomic.AddInt64(&op.stats.puts, 1)
	op.pool.Put(obj)
}

// GetStats 获取对象池的详细统计信息。
// 返回包含各种使用指标的映射表。
//
// 返回值：
// 包含统计信息的映射表
//
// 统计指标：
// - name: 对象池名称
// - gets: 获取对象的总次数
// - puts: 放回对象的总次数
// - news: 创建新对象的总次数
// - size: 当前池中对象的估计数量
// - max_size: 历史最大池大小
//
// 使用场景：
// - 性能监控和调优
// - 问题诊断和分析
// - 资源使用情况报告
// - 系统健康检查
func (op *ObjectPool) GetStats() map[string]interface{} {
	return map[string]interface{}{
		"name":     op.name,
		"gets":     atomic.LoadInt64(&op.stats.gets),
		"puts":     atomic.LoadInt64(&op.stats.puts),
		"news":     atomic.LoadInt64(&op.stats.news),
		"size":     atomic.LoadInt64(&op.stats.size),
		"max_size": atomic.LoadInt64(&op.stats.maxSize),
	}
}

// EntryPool Entry对象池
var EntryPool = NewObjectPool("entry", func() interface{} {
	return &Entry{}
})

// GetEntry 从池中获取Entry对象
func GetEntry() *Entry {
	entry := EntryPool.Get().(*Entry)
	// 重置Entry状态
	entry.Key = entry.Key[:0]
	entry.Value = entry.Value[:0]
	entry.Meta = 0
	entry.Version = 0
	entry.ExpiresAt = 0
	return entry
}

// PutEntry 将Entry对象放回池中
func PutEntry(entry *Entry) {
	if entry != nil {
		EntryPool.Put(entry)
	}
}

// BufferPool 字节缓冲区池
var BufferPool = NewObjectPool("buffer", func() interface{} {
	return make([]byte, 0, 4096) // 4KB初始容量
})

// GetBuffer 从池中获取字节缓冲区
func GetBuffer() []byte {
	buf := BufferPool.Get().([]byte)
	return buf[:0] // 重置长度但保留容量
}

// PutBuffer 将字节缓冲区放回池中
func PutBuffer(buf []byte) {
	if buf != nil && cap(buf) <= 64*1024 { // 只回收小于64KB的缓冲区
		BufferPool.Put(buf)
	}
}
