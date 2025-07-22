/*
JadeDB 资源关闭管理模块

Closer 是 JadeDB 中用于优雅关闭和资源管理的核心组件。
它提供了一种标准化的方式来协调多个 goroutine 的关闭过程。

核心功能：
1. 信号传播：向所有相关 goroutine 发送关闭信号
2. 同步等待：等待所有 goroutine 完成清理工作
3. 资源管理：确保资源的正确释放和清理
4. 优雅关闭：避免强制终止导致的数据不一致

设计原理：
- 两阶段关闭：先发送信号，再等待完成
- 引用计数：使用 WaitGroup 跟踪活跃的 goroutine
- 上下文传播：使用 context 传递取消信号
- 通道通信：使用 channel 进行异步通知

使用模式：
1. 创建 Closer 实例
2. 在启动 goroutine 前调用 Add()
3. 在 goroutine 中监听关闭信号
4. 在 goroutine 结束时调用 Done()
5. 主线程调用 Close() 等待所有 goroutine 结束

应用场景：
- 数据库关闭时的资源清理
- 后台服务的优雅停止
- 长时间运行任务的中断处理
- 系统组件的生命周期管理

并发安全：
所有方法都是并发安全的，可以在多个 goroutine 中同时调用。
*/

package utils

import (
	"context"
	"sync"
)

var (
	// dummyCloserChan 是一个永远不会关闭的通道。
	// 用于 nil Closer 的安全处理，避免空指针异常。
	dummyCloserChan <-chan struct{}
)

// Closer 提供优雅关闭和资源管理的功能。
// 它协调多个 goroutine 的关闭过程，确保所有资源都被正确清理。
//
// 工作流程：
// 1. 初始化：创建 Closer 并设置初始计数
// 2. 启动：启动需要管理的 goroutine
// 3. 监听：goroutine 监听关闭信号
// 4. 信号：主线程发送关闭信号
// 5. 清理：各 goroutine 执行清理工作
// 6. 完成：所有 goroutine 报告完成
// 7. 退出：主线程继续执行
//
// 两种使用模式：
// 1. 简单模式：使用 CloseSignal 通道
// 2. 上下文模式：使用 context 取消机制
type Closer struct {
	// 同步原语

	// waiting 用于跟踪需要等待完成的 goroutine 数量。
	// 每个需要管理的 goroutine 在启动前调用 Add(1)。
	// 在 goroutine 完成清理工作后调用 Done()。
	// 主线程通过 Wait() 等待所有 goroutine 完成。
	waiting sync.WaitGroup

	// 上下文管理（新版本接口）

	// ctx 是用于传播取消信号的上下文。
	// 当调用 Signal() 时，这个上下文会被取消。
	// goroutine 可以通过 ctx.Done() 监听取消信号。
	ctx context.Context

	// cancel 是取消上下文的函数。
	// 调用这个函数会关闭 ctx.Done() 通道。
	// 用于实现基于上下文的关闭机制。
	cancel context.CancelFunc

	// 通道管理（兼容接口）

	// CloseSignal 是用于发送关闭信号的通道。
	// 当调用 Close() 时，这个通道会被关闭。
	// goroutine 可以通过监听这个通道来接收关闭信号。
	// 这是为了保持向后兼容性而保留的接口。
	CloseSignal chan struct{}
}

// NewCloser 创建一个新的 Closer 实例（简单模式）。
// 这个版本使用传统的通道机制进行关闭信号传播。
//
// 返回值：
// 新创建的 Closer 实例
//
// 特点：
// - 使用 CloseSignal 通道进行信号传播
// - 兼容旧版本的使用方式
// - 适合简单的关闭场景
//
// 使用示例：
// closer := NewCloser()
// closer.Add(1)
//
//	go func() {
//	    defer closer.Done()
//	    select {
//	    case <-closer.CloseSignal:
//	        // 执行清理工作
//	        return
//	    }
//	}()
//
// closer.Close() // 等待 goroutine 完成
func NewCloser() *Closer {
	// 创建 Closer 实例并初始化 WaitGroup
	closer := &Closer{waiting: sync.WaitGroup{}}

	// 创建关闭信号通道
	closer.CloseSignal = make(chan struct{})

	return closer
}

// NewCloserInitial 创建一个新的 Closer 实例（上下文模式）。
// 这个版本使用 context 机制进行关闭信号传播，并预设初始计数。
//
// 参数说明：
// initial: 初始的 goroutine 计数
//
// 返回值：
// 新创建的 Closer 实例
//
// 特点：
// - 使用 context 进行信号传播
// - 支持取消传播和超时控制
// - 预设初始计数，适合已知 goroutine 数量的场景
//
// 使用示例：
// closer := NewCloserInitial(2) // 预设2个goroutine
//
//	go func() {
//	    defer closer.Done()
//	    select {
//	    case <-closer.HasBeenClosed():
//	        // 执行清理工作
//	        return
//	    }
//	}()
//
// closer.SignalAndWait() // 发送信号并等待完成
func NewCloserInitial(initial int) *Closer {
	ret := &Closer{}

	// 创建可取消的上下文
	ret.ctx, ret.cancel = context.WithCancel(context.Background())

	// 设置初始的等待计数
	ret.waiting.Add(initial)

	return ret
}

// Close 发送关闭信号并等待所有 goroutine 完成（简单模式）。
// 这是一个阻塞操作，会等待所有注册的 goroutine 完成清理工作。
//
// 工作流程：
// 1. 关闭 CloseSignal 通道，通知所有监听的 goroutine
// 2. 等待所有 goroutine 调用 Done() 完成清理
// 3. 返回，表示所有资源已被清理
//
// 使用场景：
// - 数据库关闭时等待所有后台任务完成
// - 服务停止时确保所有请求处理完毕
// - 系统退出前的资源清理
//
// 注意事项：
// - 这是一个阻塞操作，可能需要较长时间
// - 确保所有 goroutine 都能响应关闭信号
// - 避免死锁，确保 Done() 会被调用
func (c *Closer) Close() {
	// 关闭信号通道，通知所有监听的 goroutine
	close(c.CloseSignal)

	// 等待所有 goroutine 完成清理工作
	c.waiting.Wait()
}

// Done 标记一个 goroutine 已完成清理工作。
// 每个被管理的 goroutine 在完成清理后必须调用这个方法。
//
// 使用场景：
// - goroutine 完成资源清理后调用
// - 通常在 defer 语句中调用以确保执行
// - 与 Add() 方法配对使用
//
// 注意事项：
// - 必须与之前的 Add() 调用配对
// - 不要重复调用，会导致计数错误
// - 建议在 defer 语句中调用
func (c *Closer) Done() {
	c.waiting.Done()
}

// Add 增加需要等待的 goroutine 计数。
// 在启动新的 goroutine 前调用，增加等待计数。
//
// 参数说明：
// n: 要增加的计数值（通常为1）
//
// 使用场景：
// - 启动新的后台 goroutine 前调用
// - 动态增加需要管理的任务数量
// - 与 Done() 方法配对使用
//
// 注意事项：
// - 必须在 goroutine 启动前调用
// - 每次 Add() 都需要对应的 Done() 调用
// - 计数不能为负数
func (c *Closer) Add(n int) {
	c.waiting.Add(n)
}

// HasBeenClosed 返回一个通道，用于检查是否已发送关闭信号。
// 这是基于上下文的关闭检查方法。
//
// 返回值：
// 一个只读通道，关闭时表示已发送关闭信号
//
// 使用场景：
// - 在 select 语句中监听关闭信号
// - 非阻塞地检查关闭状态
// - 与其他通道操作组合使用
//
// 安全性：
// - 对 nil Closer 返回安全的虚拟通道
// - 避免空指针异常
//
// 使用示例：
// select {
// case <-closer.HasBeenClosed():
//
//	// 处理关闭信号
//	return
//
// case data := <-dataChan:
//
//	    // 处理正常数据
//	}
func (c *Closer) HasBeenClosed() <-chan struct{} {
	if c == nil {
		return dummyCloserChan
	}
	return c.ctx.Done()
}

// SignalAndWait 发送关闭信号并等待所有 goroutine 完成（上下文模式）。
// 这是 Signal() 和 Wait() 的组合操作。
//
// 工作流程：
// 1. 调用 Signal() 发送关闭信号
// 2. 调用 Wait() 等待所有 goroutine 完成
//
// 使用场景：
// - 需要原子地执行信号发送和等待的场景
// - 简化关闭流程的便捷方法
// - 确保信号发送和等待的顺序性
func (c *Closer) SignalAndWait() {
	c.Signal()
	c.Wait()
}

// Signal 发送关闭信号但不等待完成（上下文模式）。
// 通过取消上下文来通知所有监听的 goroutine。
//
// 特点：
// - 非阻塞操作，立即返回
// - 通过上下文传播取消信号
// - 可以与 Wait() 分开调用
//
// 使用场景：
// - 需要立即发送关闭信号的场景
// - 与 Wait() 分离，实现更灵活的控制
// - 在超时或错误情况下强制关闭
func (c *Closer) Signal() {
	c.cancel()
}

// Wait 等待所有 goroutine 完成，但不发送关闭信号。
// 这是一个纯等待操作，不会主动触发关闭。
//
// 特点：
// - 阻塞操作，直到所有 goroutine 完成
// - 不发送关闭信号，需要其他方式触发
// - 可以与 Signal() 分开调用
//
// 使用场景：
// - 已经通过其他方式发送关闭信号
// - 需要等待特定的完成条件
// - 实现更复杂的关闭逻辑
func (c *Closer) Wait() {
	c.waiting.Wait()
}
