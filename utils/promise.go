/*
JadeDB Promise 异步编程模块

Promise 实现了 Future/Promise 异步编程模式，用于处理异步操作的结果。
这种模式在数据库系统中特别有用，可以优雅地处理异步 I/O 操作。

核心概念：
1. 异步执行：操作在后台异步执行，不阻塞调用者
2. 结果承诺：Promise 承诺在未来某个时刻提供操作结果
3. 状态管理：跟踪操作的执行状态（进行中、已完成、已失败）
4. 同步等待：提供同步等待异步结果的机制

设计模式：
- Future/Promise：将异步操作的执行和结果获取分离
- 一次性设置：结果只能设置一次，确保一致性
- 线程安全：支持多线程环境下的安全使用
- 错误传播：统一的错误处理机制

使用场景：
- 异步磁盘 I/O 操作
- 后台压缩和垃圾回收
- 批量操作的协调
- 事务提交的异步处理

优势：
- 非阻塞：调用者可以继续执行其他任务
- 组合性：多个 Promise 可以组合使用
- 错误处理：统一的错误处理机制
- 资源效率：避免线程阻塞，提高资源利用率

实现特点：
- 轻量级：使用 WaitGroup 和原子操作，开销很小
- 并发安全：所有操作都是线程安全的
- 一次性：结果只能设置一次，防止竞态条件
- 简单易用：API 简洁明了，易于理解和使用

典型用法：
```go
// 创建 Promise
promise := NewPromise()

// 异步执行操作
go func() {
    err := doSomeAsyncWork()
    promise.Resolve(err)
}()

// 等待结果
err := promise.Wait()
if err != nil {
    // 处理错误
}
```
*/

package utils

import (
	"sync"
	"sync/atomic"
)

// Promise 实现异步操作的 Future/Promise 模式。
// 它允许异步操作的执行者和结果消费者解耦，提供优雅的异步编程体验。
//
// 工作原理：
// 1. 创建 Promise 时，内部计数器设置为1
// 2. 异步操作完成时调用 Resolve() 设置结果
// 3. 调用者通过 Wait() 等待操作完成并获取结果
// 4. 使用原子操作确保结果只能设置一次
//
// 状态转换：
// 初始状态 -> 执行中 -> 已完成（成功或失败）
//
// 线程安全：
// 所有方法都是线程安全的，可以在多个 goroutine 中安全使用。
type Promise struct {
	// 同步原语

	// wg 用于同步等待异步操作完成。
	// 初始化时计数为1，Resolve时调用Done()。
	// Wait()方法通过wg.Wait()阻塞直到操作完成。
	wg sync.WaitGroup

	// 结果存储

	// err 存储异步操作的结果。
	// nil 表示操作成功，非nil表示操作失败。
	// 只有在done标志设置后，这个值才是有效的。
	err error

	// 状态标志（原子操作）

	// done 标记 Promise 是否已经完成。
	// 0表示未完成，1表示已完成。
	// 使用原子操作确保并发安全和一次性设置。
	done int32
}

// NewPromise 创建一个新的 Promise 实例。
// 返回的 Promise 处于未完成状态，等待异步操作的结果。
//
// 返回值：
// 新创建的 Promise 实例
//
// 初始化过程：
// 1. 创建 Promise 结构体
// 2. 设置 WaitGroup 计数为1
// 3. done 标志初始化为0（未完成）
//
// 使用模式：
// 通常在启动异步操作前创建 Promise，然后将其传递给异步操作的执行者。
func NewPromise() *Promise {
	p := &Promise{}
	p.wg.Add(1) // 设置等待计数为1
	return p
}

// Resolve 解决 Promise，设置异步操作的结果。
// 这个方法只能被调用一次，后续调用会被忽略。
//
// 参数说明：
// err: 异步操作的结果，nil表示成功，非nil表示失败
//
// 工作原理：
// 1. 使用原子操作检查并设置done标志
// 2. 如果是第一次调用，存储结果并通知等待者
// 3. 如果已经被调用过，忽略本次调用
//
// 并发安全：
// 使用 CompareAndSwap 原子操作确保只有第一次调用生效。
// 这防止了竞态条件和重复设置结果的问题。
//
// 使用场景：
// 在异步操作完成时调用，无论操作成功还是失败。
func (p *Promise) Resolve(err error) {
	// 原子地检查并设置done标志，确保只执行一次
	if atomic.CompareAndSwapInt32(&p.done, 0, 1) {
		p.err = err // 存储操作结果
		p.wg.Done() // 通知等待的goroutine
	}
	// 如果已经完成，忽略本次调用
}

// Wait 等待 Promise 完成并返回结果。
// 这是一个阻塞操作，会一直等待直到 Resolve() 被调用。
//
// 返回值：
// 异步操作的结果，nil表示成功，非nil表示失败
//
// 阻塞行为：
// - 如果 Promise 已经完成，立即返回结果
// - 如果 Promise 未完成，阻塞直到 Resolve() 被调用
//
// 使用场景：
// - 需要同步等待异步操作结果的地方
// - 在主线程中等待后台任务完成
// - 实现同步接口包装异步操作
//
// 注意事项：
// - 可以被多个 goroutine 同时调用
// - 所有等待者都会收到相同的结果
func (p *Promise) Wait() error {
	p.wg.Wait()  // 阻塞等待操作完成
	return p.err // 返回操作结果
}

// IsDone 检查 Promise 是否已经完成。
// 这是一个非阻塞操作，立即返回当前状态。
//
// 返回值：
// true表示已完成，false表示仍在进行中
//
// 使用场景：
// - 非阻塞地检查操作状态
// - 实现超时和轮询机制
// - 在UI或监控系统中显示进度
//
// 性能特点：
// - 非阻塞操作，立即返回
// - 使用原子操作，性能开销很小
// - 可以被频繁调用而不影响性能
func (p *Promise) IsDone() bool {
	return atomic.LoadInt32(&p.done) == 1
}
