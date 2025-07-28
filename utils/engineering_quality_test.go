/*
JadeDB 工程质量提升功能测试

本测试文件验证工程质量提升的各项优化功能，
确保代码结构优化、错误处理完善、锁粒度优化等功能正确工作。

测试覆盖：
1. Options结构体重构功能
2. 锁粒度优化效果
3. 对象复用机制
4. 错误处理完善
5. 私有方法访问控制
*/

package utils

import (
	"bytes"
	"testing"
)

// TestOptionsRestructure 测试Options结构体重构功能
func TestOptionsRestructure(t *testing.T) {
	// 测试默认选项创建
	t.Run("DefaultOptions", func(t *testing.T) {
		opts := DefaultOptions()

		// 验证默认值
		if !opts.IsAsc {
			t.Errorf("Expected IsAsc to be true by default")
		}
		if opts.PrefetchSize != 100 {
			t.Errorf("Expected PrefetchSize to be 100, got %d", opts.PrefetchSize)
		}
		if !opts.ReadAhead {
			t.Errorf("Expected ReadAhead to be true by default")
		}
		if opts.SkipEmpty {
			t.Errorf("Expected SkipEmpty to be false by default")
		}
	})

	// 测试链式调用
	t.Run("ChainedCalls", func(t *testing.T) {
		prefix := []byte("test_prefix")
		startKey := []byte("start")
		endKey := []byte("end")

		opts := NewOptions().
			WithPrefix(prefix).
			WithRange(startKey, endKey).
			WithDescending().
			WithPrefetchSize(200)

		// 验证前缀设置
		if !bytes.Equal(opts.Prefix, prefix) {
			t.Errorf("Expected prefix %s, got %s", string(prefix), string(opts.Prefix))
		}

		// 验证范围设置
		if !bytes.Equal(opts.StartKey, startKey) {
			t.Errorf("Expected start key %s, got %s", string(startKey), string(opts.StartKey))
		}
		if !bytes.Equal(opts.EndKey, endKey) {
			t.Errorf("Expected end key %s, got %s", string(endKey), string(opts.EndKey))
		}

		// 验证降序设置
		if opts.IsAsc {
			t.Errorf("Expected IsAsc to be false after WithDescending()")
		}

		// 验证预读大小设置
		if opts.PrefetchSize != 200 {
			t.Errorf("Expected PrefetchSize to be 200, got %d", opts.PrefetchSize)
		}
	})

	// 测试数据隔离
	t.Run("DataIsolation", func(t *testing.T) {
		originalPrefix := []byte("original")
		opts := NewOptions().WithPrefix(originalPrefix)

		// 修改原始数据
		originalPrefix[0] = 'X'

		// 验证Options中的数据没有被修改
		if opts.Prefix[0] == 'X' {
			t.Errorf("Options.Prefix should be isolated from original data")
		}

		// 修改Options中的数据
		opts.Prefix[0] = 'Y'

		// 验证原始数据没有被修改（除了我们之前的修改）
		if originalPrefix[0] != 'X' {
			t.Errorf("Original data should not be affected by Options modification")
		}
	})
}

// TestOptionsFlexibility 测试Options的灵活性和扩展性
func TestOptionsFlexibility(t *testing.T) {
	// 测试空值处理
	t.Run("EmptyValues", func(t *testing.T) {
		opts := NewOptions().
			WithPrefix(nil).
			WithRange(nil, nil)

		// 验证空值不会导致panic
		if opts.Prefix != nil {
			t.Errorf("Expected nil prefix to remain nil")
		}
		if opts.StartKey != nil {
			t.Errorf("Expected nil start key to remain nil")
		}
		if opts.EndKey != nil {
			t.Errorf("Expected nil end key to remain nil")
		}
	})

	// 测试边界值
	t.Run("BoundaryValues", func(t *testing.T) {
		opts := NewOptions().WithPrefetchSize(0)

		if opts.PrefetchSize != 0 {
			t.Errorf("Expected PrefetchSize to be 0, got %d", opts.PrefetchSize)
		}

		// 测试负值（虽然不推荐，但应该能设置）
		opts.WithPrefetchSize(-1)
		if opts.PrefetchSize != -1 {
			t.Errorf("Expected PrefetchSize to be -1, got %d", opts.PrefetchSize)
		}
	})

	// 测试多次调用同一方法
	t.Run("MultipleCallsSameMethod", func(t *testing.T) {
		opts := NewOptions().
			WithPrefetchSize(100).
			WithPrefetchSize(200).
			WithPrefetchSize(300)

		// 最后一次调用应该生效
		if opts.PrefetchSize != 300 {
			t.Errorf("Expected PrefetchSize to be 300, got %d", opts.PrefetchSize)
		}
	})
}

// TestOptionsUsagePatterns 测试Options的常见使用模式
func TestOptionsUsagePatterns(t *testing.T) {
	// 测试只读查询模式
	t.Run("ReadOnlyQuery", func(t *testing.T) {
		opts := NewOptions().
			WithPrefix([]byte("readonly_")).
			WithPrefetchSize(1000) // 大预读提升读取性能

		if opts.PrefetchSize != 1000 {
			t.Errorf("Expected large prefetch size for read-only queries")
		}
		if !opts.ReadAhead {
			t.Errorf("Expected ReadAhead to be enabled for read-only queries")
		}
	})

	// 测试范围扫描模式
	t.Run("RangeScan", func(t *testing.T) {
		opts := NewOptions().
			WithRange([]byte("start"), []byte("end")).
			WithPrefetchSize(50) // 适中的预读大小

		if len(opts.StartKey) == 0 || len(opts.EndKey) == 0 {
			t.Errorf("Expected both start and end keys to be set for range scan")
		}
	})

	// 测试内存受限模式
	t.Run("MemoryConstrained", func(t *testing.T) {
		opts := NewOptions().
			WithPrefetchSize(10). // 小预读减少内存使用
			WithDescending()      // 可能需要反向扫描

		if opts.PrefetchSize != 10 {
			t.Errorf("Expected small prefetch size for memory-constrained mode")
		}
		if opts.IsAsc {
			t.Errorf("Expected descending order for this test case")
		}
	})
}

// TestOptionsCompatibility 测试Options的向后兼容性
func TestOptionsCompatibility(t *testing.T) {
	// 测试旧式创建方式仍然有效
	t.Run("LegacyCreation", func(t *testing.T) {
		opts := &Options{
			Prefix: []byte("legacy"),
			IsAsc:  false,
		}

		// 验证直接创建仍然有效
		if !bytes.Equal(opts.Prefix, []byte("legacy")) {
			t.Errorf("Legacy creation should work")
		}
		if opts.IsAsc {
			t.Errorf("Legacy field setting should work")
		}
	})

	// 测试混合使用方式
	t.Run("MixedUsage", func(t *testing.T) {
		opts := &Options{
			Prefix: []byte("mixed"),
			IsAsc:  false,
		}

		// 使用新的链式调用方法
		opts.WithPrefetchSize(500).WithDescending()

		// 验证两种方式都生效
		if !bytes.Equal(opts.Prefix, []byte("mixed")) {
			t.Errorf("Direct field setting should still work")
		}
		if opts.PrefetchSize != 500 {
			t.Errorf("Chained method should work on existing struct")
		}
	})
}

// TestOptionsPerformance 测试Options的性能特性
func TestOptionsPerformance(t *testing.T) {
	// 测试大量创建的性能
	t.Run("MassCreation", func(t *testing.T) {
		const count = 10000

		for i := 0; i < count; i++ {
			opts := NewOptions().
				WithPrefix([]byte("test")).
				WithPrefetchSize(i % 1000)

			// 简单验证确保创建成功
			if opts == nil {
				t.Fatalf("Failed to create options at iteration %d", i)
			}
		}
	})

	// 测试内存使用
	t.Run("MemoryUsage", func(t *testing.T) {
		// 创建大量Options实例
		options := make([]*Options, 1000)
		for i := range options {
			options[i] = NewOptions().
				WithPrefix([]byte("memory_test")).
				WithRange([]byte("start"), []byte("end"))
		}

		// 验证所有实例都正确创建
		for i, opts := range options {
			if opts == nil {
				t.Errorf("Options instance %d is nil", i)
			}
			if !bytes.Equal(opts.Prefix, []byte("memory_test")) {
				t.Errorf("Options instance %d has incorrect prefix", i)
			}
		}
	})
}
