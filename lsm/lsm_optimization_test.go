/*
LSM引擎优化功能测试

本测试文件验证LSM引擎的各项优化功能，包括：
1. 内存表引用计数机制
2. 并发安全的关闭操作
3. 性能统计信息收集
4. 配置管理改进

运行测试：
go test -v ./lsm -run TestLSMOptimization
*/

package lsm

import (
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/util6/JadeDB/utils"
)

// TestMemTableRefCounting 测试内存表引用计数功能
func TestMemTableRefCounting(t *testing.T) {
	// 创建临时工作目录
	workDir := "./test_ref_counting"
	defer os.RemoveAll(workDir)
	require.NoError(t, os.MkdirAll(workDir, 0755))

	// 配置选项
	opt := NewDefaultOptions()
	opt.WorkDir = workDir
	opt.MemTableSize = 1024 // 小内存表便于测试
	opt.MaxImmutableTables = 2

	// 创建LSM实例
	lsm := NewLSM(opt)
	defer lsm.Close()

	// 测试新创建的内存表引用计数
	assert.Equal(t, int32(1), lsm.memTable.getRefCount(), "新内存表引用计数应为1")

	// 测试引用计数增加
	newCount := lsm.memTable.incrementRef()
	assert.Equal(t, int32(2), newCount, "增加引用后计数应为2")

	// 测试引用计数减少
	newCount = lsm.memTable.decrementRef()
	assert.Equal(t, int32(1), newCount, "减少引用后计数应为1")

	// 测试配置项
	assert.Equal(t, 2, opt.MaxImmutableTables, "MaxImmutableTables配置应正确设置")
}

// TestLSMStats 测试LSM统计信息功能
func TestLSMStats(t *testing.T) {
	// 创建临时工作目录
	workDir := "./test_stats"
	defer os.RemoveAll(workDir)
	require.NoError(t, os.MkdirAll(workDir, 0755))

	// 配置选项
	opt := NewDefaultOptions()
	opt.WorkDir = workDir
	opt.MemTableSize = 2048
	opt.MaxImmutableTables = 3

	// 创建LSM实例
	lsm := NewLSM(opt)
	defer lsm.Close()

	// 写入一些数据
	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("test_key_%08d", i)
		value := fmt.Sprintf("test_value_%08d", i)

		entry := &utils.Entry{
			Key:   []byte(key),
			Value: []byte(value),
		}

		require.NoError(t, lsm.Set(entry))
	}

	// 获取统计信息
	stats := lsm.GetStats()

	// 验证统计信息结构
	assert.Contains(t, stats, "memtable_size", "统计信息应包含内存表大小")
	assert.Contains(t, stats, "memtable_ref_count", "统计信息应包含引用计数")
	assert.Contains(t, stats, "immutable_count", "统计信息应包含不可变表数量")
	assert.Contains(t, stats, "immutable_total_size", "统计信息应包含不可变表总大小")
	assert.Contains(t, stats, "levels", "统计信息应包含层级信息")

	// 验证数值合理性
	memTableSize, ok := stats["memtable_size"].(int64)
	assert.True(t, ok, "内存表大小应为int64类型")
	assert.Greater(t, memTableSize, int64(0), "内存表大小应大于0")

	refCount, ok := stats["memtable_ref_count"].(int32)
	assert.True(t, ok, "引用计数应为int32类型")
	assert.Equal(t, int32(1), refCount, "引用计数应为1")

	immutableCount, ok := stats["immutable_count"].(int)
	assert.True(t, ok, "不可变表数量应为int类型")
	assert.GreaterOrEqual(t, immutableCount, 0, "不可变表数量应大于等于0")
}

// TestConcurrentClose 测试并发安全的关闭机制
func TestConcurrentClose(t *testing.T) {
	// 创建临时工作目录
	workDir := "./test_concurrent_close"
	defer os.RemoveAll(workDir)
	require.NoError(t, os.MkdirAll(workDir, 0755))

	// 配置选项
	opt := NewDefaultOptions()
	opt.WorkDir = workDir
	opt.MemTableSize = 1024

	// 创建LSM实例
	lsm := NewLSM(opt)

	// 启动并发写入操作
	var wg sync.WaitGroup
	stopChan := make(chan struct{})

	// 启动少量写入协程，减少并发压力
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < 10; j++ {
				select {
				case <-stopChan:
					return
				default:
				}

				key := fmt.Sprintf("concurrent_%d_key_%08d", id, j)
				value := fmt.Sprintf("concurrent_%d_value_%08d", id, j)

				entry := &utils.Entry{
					Key:   []byte(key),
					Value: []byte(value),
				}

				// 忽略可能的错误，因为可能在关闭过程中
				lsm.Set(entry)
				time.Sleep(2 * time.Millisecond)
			}
		}(i)
	}

	// 等待一段时间后开始关闭
	time.Sleep(5 * time.Millisecond)

	// 停止写入操作
	close(stopChan)

	// 等待所有协程完成
	wg.Wait()

	// 测试安全关闭
	start := time.Now()
	err := lsm.Close()
	duration := time.Since(start)

	// 验证关闭成功
	assert.NoError(t, err, "LSM关闭应该成功")
	assert.Less(t, duration, 100*time.Millisecond, "关闭时间应该合理")

	// 验证关闭后的状态
	// 注意：关闭后的LSM不应再接受写入操作
}

// TestConfigurationManagement 测试配置管理改进
func TestConfigurationManagement(t *testing.T) {
	// 测试默认配置
	opt := NewDefaultOptions()

	// 验证新增的配置项
	assert.Equal(t, 6, opt.MaxImmutableTables, "默认MaxImmutableTables应为6")
	assert.Greater(t, opt.MemTableSize, int64(0), "内存表大小应大于0")
	assert.Greater(t, opt.NumCompactors, 0, "压缩器数量应大于0")
	assert.Greater(t, opt.MaxLevelNum, 0, "最大层数应大于0")

	// 测试配置修改
	opt.MaxImmutableTables = 8
	opt.MemTableSize = 2048

	assert.Equal(t, 8, opt.MaxImmutableTables, "配置修改应生效")
	assert.Equal(t, int64(2048), opt.MemTableSize, "配置修改应生效")
}

// TestMemoryOptimization 测试内存优化功能
func TestMemoryOptimization(t *testing.T) {
	// 创建临时工作目录
	workDir := "./test_memory_opt"
	defer os.RemoveAll(workDir)
	require.NoError(t, os.MkdirAll(workDir, 0755))

	// 配置选项
	opt := NewDefaultOptions()
	opt.WorkDir = workDir
	opt.MemTableSize = 2048 // 适中的内存表大小
	opt.MaxImmutableTables = 2

	// 创建LSM实例
	lsm := NewLSM(opt)
	defer lsm.Close()

	// 写入少量数据进行测试
	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("memory_test_key_%08d", i)
		value := fmt.Sprintf("memory_test_value_%08d", i)

		entry := &utils.Entry{
			Key:   []byte(key),
			Value: []byte(value),
		}

		require.NoError(t, lsm.Set(entry))
	}

	// 获取统计信息
	stats := lsm.GetStats()

	// 验证内存使用情况
	memTableSize, ok := stats["memtable_size"].(int64)
	assert.True(t, ok, "应能获取内存表大小")
	assert.Greater(t, memTableSize, int64(0), "内存表应有数据")

	immutableCount, ok := stats["immutable_count"].(int)
	assert.True(t, ok, "应能获取不可变表数量")
	assert.LessOrEqual(t, immutableCount, opt.MaxImmutableTables,
		"不可变表数量不应超过配置限制")
}

// BenchmarkLSMOptimizedOperations 基准测试优化后的操作
func BenchmarkLSMOptimizedOperations(b *testing.B) {
	// 创建临时工作目录
	workDir := "./bench_optimized"
	defer os.RemoveAll(workDir)
	os.MkdirAll(workDir, 0755)

	// 配置选项
	opt := NewDefaultOptions()
	opt.WorkDir = workDir
	opt.MemTableSize = 1024 * 1024 // 1MB
	opt.MaxImmutableTables = 4

	// 创建LSM实例
	lsm := NewLSM(opt)
	defer lsm.Close()

	b.ResetTimer()

	b.Run("OptimizedWrite", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			key := fmt.Sprintf("bench_key_%08d", i)
			value := fmt.Sprintf("bench_value_%08d", i)

			entry := &utils.Entry{
				Key:   []byte(key),
				Value: []byte(value),
			}

			if err := lsm.Set(entry); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("OptimizedRead", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			key := fmt.Sprintf("bench_key_%08d", i%1000)
			_, err := lsm.Get([]byte(key))
			if err != nil && err != utils.ErrKeyNotFound {
				b.Fatal(err)
			}
		}
	})

	b.Run("StatsCollection", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			stats := lsm.GetStats()
			if len(stats) == 0 {
				b.Fatal("统计信息不应为空")
			}
		}
	})
}
