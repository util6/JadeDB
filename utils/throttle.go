/*
JadeDB 限流器（Throttle）模块

限流器是一种重要的流量控制机制，用于限制操作的执行频率，防止系统过载。
在 JadeDB 中，限流器主要用于控制压缩、垃圾回收等后台操作的频率。

核心功能：
1. 频率控制：限制操作的执行频率，防止资源过度消耗
2. 平滑限流：使用令牌桶算法，提供平滑的流量控制
3. 非阻塞检查：支持非阻塞的限流检查
4. 资源保护：保护系统免受突发流量冲击

设计原理：
- 令牌桶算法：定期向桶中添加令牌，操作需要消耗令牌
- 时间驱动：使用定时器定期补充令牌
- 通道通信：使用缓冲通道实现令牌的存储和分发
- 优雅降级：当限流激活时，操作会等待而不是失败

适用场景：
- 后台压缩操作的频率控制
- 垃圾回收操作的速率限制
- 磁盘 I/O 操作的流量控制
- 网络请求的频率限制

性能考虑：
- 低开销：使用轻量级的通道和定时器
- 无锁设计：避免使用互斥锁，减少竞争
- 内存友好：使用固定大小的缓冲区
- CPU 友好：定时器由系统调度，不占用 CPU

使用模式：
1. 创建限流器并指定速率
2. 在需要限流的操作前调用 Do()
3. 执行实际操作
4. 可选择性调用完成方法
5. 关闭时调用 Close() 清理资源
*/

package utils

import (
	"time"
)

// Throttle 实现基于令牌桶算法的限流器。
// 它控制操作的执行频率，防止系统过载和资源耗尽。
//
// 工作原理：
// 1. 定时器定期向通道中发送令牌
// 2. 操作执行前需要从通道中获取令牌
// 3. 如果没有令牌可用，操作会被阻塞
// 4. 通道缓冲区大小为1，实现简单的令牌桶
//
// 算法特点：
// - 平滑限流：避免突发流量对系统的冲击
// - 简单高效：使用 Go 的原生通道和定时器
// - 自适应：可以处理不同的负载模式
// - 公平性：按照先来先服务的原则分配令牌
type Throttle struct {
	// 定时器组件

	// ticker 定期生成令牌的定时器。
	// 根据配置的速率计算时间间隔。
	// nil 表示不限流（无限速率）。
	ticker *time.Ticker

	// 令牌通道

	// ch 存储和分发令牌的缓冲通道。
	// 缓冲区大小为1，实现简单的令牌桶。
	// 空结构体作为令牌，不占用额外内存。
	ch chan struct{}
}

// NewThrottle 创建一个新的限流器实例。
// 根据指定的速率配置令牌生成频率。
//
// 参数说明：
// rate: 每秒允许的操作次数，0或负数表示不限流
//
// 返回值：
// 配置完成的限流器实例
//
// 实现细节：
// 1. rate <= 0 时创建无限流限流器
// 2. 计算令牌生成间隔：1秒 / rate
// 3. 启动后台 goroutine 定期生成令牌
// 4. 初始化时提供一个令牌，允许立即执行第一个操作
//
// 后台 goroutine：
// - 监听定时器事件
// - 尝试向通道发送令牌
// - 使用 select default 避免阻塞
// - 如果通道已满，丢弃多余的令牌
func NewThrottle(rate int) *Throttle {
	// 处理无限流情况
	if rate <= 0 {
		return &Throttle{
			ch: make(chan struct{}, 1),
		}
	}

	// 计算令牌生成间隔
	interval := time.Second / time.Duration(rate)
	ticker := time.NewTicker(interval)
	ch := make(chan struct{}, 1)

	// 初始化时提供一个令牌，允许立即执行
	ch <- struct{}{}

	// 启动令牌生成器 goroutine
	go func() {
		for range ticker.C {
			select {
			case ch <- struct{}{}:
				// 成功添加令牌
			default:
				// 通道已满，丢弃令牌
				// 这实现了令牌桶的"溢出"行为
			}
		}
	}()

	return &Throttle{
		ticker: ticker,
		ch:     ch,
	}
}

// Do 执行一个操作，如果需要限流则等待，返回一个完成函数
func (t *Throttle) Do() func() {
	if t.ticker == nil {
		return func() {}
	}
	<-t.ch
	return func() {} // 返回一个空的完成函数
}

// Done 标记操作完成（兼容性方法）
func (t *Throttle) Done() {
	// 空实现，用于兼容性
}

// Finish 完成所有操作（兼容性方法）
func (t *Throttle) Finish() {
	// 空实现，用于兼容性
}

// Close 关闭限流器
func (t *Throttle) Close() {
	if t.ticker != nil {
		t.ticker.Stop()
	}
}

// Copy 复制字节切片
func Copy(src []byte) []byte {
	if src == nil {
		return nil
	}
	dst := make([]byte, len(src))
	copy(dst, src)
	return dst
}
