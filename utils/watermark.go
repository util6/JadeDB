package utils

import (
	"sync"
)

// WaterMark 用于跟踪事务的水位标记
type WaterMark struct {
	sync.RWMutex
	mark uint64
}

// NewWaterMark 创建一个新的水位标记
func NewWaterMark() *WaterMark {
	return &WaterMark{}
}

// Init 初始化水位标记
func (w *WaterMark) Init(mark uint64) {
	w.Lock()
	defer w.Unlock()
	w.mark = mark
}

// Begin 开始一个新的事务
func (w *WaterMark) Begin(mark uint64) {
	w.Lock()
	defer w.Unlock()
	if mark > w.mark {
		w.mark = mark
	}
}

// Done 完成一个事务
func (w *WaterMark) Done(mark uint64) {
	w.Lock()
	defer w.Unlock()
	// 简单实现，实际可能需要更复杂的逻辑
}

// DoneUntil 完成直到指定标记的所有事务
func (w *WaterMark) DoneUntil() uint64 {
	w.RLock()
	defer w.RUnlock()
	return w.mark
}

// WaitForMark 等待直到指定的标记
func (w *WaterMark) WaitForMark(mark uint64) {
	// 简单实现，实际可能需要条件变量
	for {
		w.RLock()
		current := w.mark
		w.RUnlock()
		if current >= mark {
			break
		}
	}
}
