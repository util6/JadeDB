package utils

import (
	"context"
	"sync"
)

var (
	dummyCloserChan <-chan struct{}
)

// Closer 用于资源回收的信号控制
// 它提供了一种在长时间运行的操作中安全关闭资源或服务的方法
type Closer struct {
	waiting sync.WaitGroup // waiting 用于同步所有等待完成的任务

	ctx         context.Context    // ctx 用于传递关闭信号的上下文
	CloseSignal chan struct{}      // CloseSignal 用于接收关闭信号
	cancel      context.CancelFunc // cancel 用于取消上下文，触发关闭
}

// NewCloser 创建并返回一个新的Closer实例。
// 这个函数初始化了一个带有同步WaitGroup的Closer结构体，用于跟踪关闭过程中的并发操作。
// 它还初始化了一个CloseSignal通道，用于通知关闭操作的开始。
func NewCloser() *Closer {
	// 初始化Closer结构体，并用sync.WaitGroup来跟踪并发任务。
	closer := &Closer{waiting: sync.WaitGroup{}}
	// 初始化一个用于接收关闭信号的通道。
	closer.CloseSignal = make(chan struct{})
	return closer
}

func NewCloserInitial(initial int) *Closer {
	ret := &Closer{}
	ret.ctx, ret.cancel = context.WithCancel(context.Background())
	ret.waiting.Add(initial)
	return ret
}

// Close 上游通知下游协程进行资源回收，并等待协程通知回收完毕
func (c *Closer) Close() {
	close(c.CloseSignal)
	c.waiting.Wait()
}

// Done 标示协程已经完成资源回收，通知上游正式关闭
func (c *Closer) Done() {
	c.waiting.Done()
}

// Add 添加wait 计数
func (c *Closer) Add(n int) {
	c.waiting.Add(n)
}

func (c *Closer) HasBeenClosed() <-chan struct{} {
	if c == nil {
		return dummyCloserChan
	}
	return c.ctx.Done()
}

func (c *Closer) SignalAndWait() {
	c.Signal()
	c.Wait()
}

func (c *Closer) Signal() {
	c.cancel()
}

func (c *Closer) Wait() {
	c.waiting.Wait()
}
