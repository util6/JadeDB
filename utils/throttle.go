package utils

import (
	"time"
)

// Throttle 用于限制操作频率的结构体
type Throttle struct {
	ticker *time.Ticker
	ch     chan struct{}
}

// NewThrottle 创建一个新的限流器
func NewThrottle(rate int) *Throttle {
	if rate <= 0 {
		return &Throttle{
			ch: make(chan struct{}, 1),
		}
	}

	interval := time.Second / time.Duration(rate)
	ticker := time.NewTicker(interval)
	ch := make(chan struct{}, 1)

	// 初始化时允许一次操作
	ch <- struct{}{}

	go func() {
		for range ticker.C {
			select {
			case ch <- struct{}{}:
			default:
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
