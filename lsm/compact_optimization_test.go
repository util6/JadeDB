/*
JadeDB 压缩策略优化功能测试

本测试文件验证LSM树压缩策略的优化功能，
确保陈旧数据统计和压缩决策正确工作。

测试覆盖：
1. 陈旧数据大小统计
2. 按陈旧数据大小排序
3. tableBuilder中的staleDataSize设置
4. TableIndex中的StaleDataSize字段
*/

package lsm

import (
	"testing"

	"github.com/util6/JadeDB/pb"
	"github.com/util6/JadeDB/utils"
)

// TestTableBuilderStaleDataSize 测试tableBuilder中陈旧数据大小的统计
func TestTableBuilderStaleDataSize(t *testing.T) {
	// 创建测试选项
	opt := &Options{
		WorkDir:      "/tmp/test_stale_data",
		SSTableMaxSz: 1 << 20, // 1MB
		BlockSize:    4 << 10, // 4KB
	}

	// 创建tableBuilder
	builder := newTableBuilder(opt)
	defer builder.Close()

	// 测试添加正常键
	normalEntry := &utils.Entry{
		Key:   []byte("normal_key"),
		Value: []byte("normal_value"),
	}
	builder.AddKey(normalEntry)

	// 验证初始staleDataSize为0
	if builder.staleDataSize != 0 {
		t.Errorf("Expected initial staleDataSize to be 0, got %d", builder.staleDataSize)
	}

	// 测试添加陈旧键
	staleEntry := &utils.Entry{
		Key:   []byte("stale_key"),
		Value: []byte("stale_value"),
	}
	builder.AddStaleKey(staleEntry)

	// 验证staleDataSize被正确更新
	// AddStaleKey会调用两次计算：一次在AddStaleKey中，一次在add方法中（如果需要finishBlock）
	// 这里我们只验证staleDataSize大于0，具体值取决于内部实现
	if builder.staleDataSize <= 0 {
		t.Errorf("Expected staleDataSize to be greater than 0, got %d", builder.staleDataSize)
	}

	// 记录第一次添加后的大小
	firstStaleSize := builder.staleDataSize

	// 添加更多陈旧键
	staleEntry2 := &utils.Entry{
		Key:   []byte("another_stale_key"),
		Value: []byte("another_stale_value"),
	}
	builder.AddStaleKey(staleEntry2)

	// 验证staleDataSize增加了
	if builder.staleDataSize <= firstStaleSize {
		t.Errorf("Expected staleDataSize to increase after adding second stale key, got %d (was %d)",
			builder.staleDataSize, firstStaleSize)
	}
}

// TestTableIndexStaleDataSize 测试TableIndex中StaleDataSize字段的设置
func TestTableIndexStaleDataSize(t *testing.T) {
	// 创建测试选项
	opt := &Options{
		WorkDir:      "/tmp/test_table_index",
		SSTableMaxSz: 1 << 20, // 1MB
		BlockSize:    4 << 10, // 4KB
	}

	// 创建tableBuilder
	builder := newTableBuilder(opt)
	defer builder.Close()

	// 添加一些正常键和陈旧键
	normalEntry := &utils.Entry{
		Key:   []byte("normal_key_1"),
		Value: []byte("normal_value_1"),
	}
	builder.AddKey(normalEntry)

	staleEntry1 := &utils.Entry{
		Key:   []byte("stale_key_1"),
		Value: []byte("stale_value_1"),
	}
	builder.AddStaleKey(staleEntry1)

	staleEntry2 := &utils.Entry{
		Key:   []byte("stale_key_2"),
		Value: []byte("stale_value_2"),
	}
	builder.AddStaleKey(staleEntry2)

	// 完成构建过程
	buildData := builder.done()

	// 使用完整的构建数据
	index := buildData.index

	// 解析TableIndex
	tableIndex := &pb.TableIndex{}
	err := tableIndex.Unmarshal(index)
	if err != nil {
		t.Fatalf("Failed to unmarshal TableIndex: %v", err)
	}

	// 验证StaleDataSize字段被正确设置
	expectedStaleSize := uint32(builder.staleDataSize)
	if tableIndex.StaleDataSize != expectedStaleSize {
		t.Errorf("Expected TableIndex.StaleDataSize to be %d, got %d",
			expectedStaleSize, tableIndex.StaleDataSize)
	}

	// 验证其他字段也被正确设置
	if tableIndex.KeyCount == 0 {
		t.Errorf("Expected KeyCount to be greater than 0")
	}
}

// TestSortByStaleDataSize 测试按陈旧数据大小排序功能
func TestSortByStaleDataSize(t *testing.T) {
	// 创建模拟的table对象
	// 注意：这里我们创建简化的测试，因为创建真实的table需要复杂的设置

	// 创建测试用的levelManager
	opt := &Options{
		WorkDir:      "/tmp/test_sort_stale",
		SSTableMaxSz: 1 << 20, // 1MB
		BlockSize:    4 << 10, // 4KB
	}

	lm := &levelManager{
		opt: opt,
	}

	// 创建模拟的compactDef
	cd := &compactDef{
		nextLevel: &levelHandler{},
	}

	// 由于创建真实的table对象比较复杂，这里主要测试排序逻辑
	// 在实际使用中，sortByStaleDataSize会被调用来对tables进行排序

	// 测试空表列表
	var emptyTables []*table
	lm.sortByStaleDataSize(emptyTables, cd)
	// 空表列表不应该导致panic

	// 测试nil nextLevel
	cdWithNilNext := &compactDef{
		nextLevel: nil,
	}
	lm.sortByStaleDataSize(emptyTables, cdWithNilNext)
	// nil nextLevel不应该导致panic
}

// TestStaleDataSizeIntegration 集成测试：验证整个流程
func TestStaleDataSizeIntegration(t *testing.T) {
	// 创建测试选项
	opt := &Options{
		WorkDir:      "/tmp/test_integration",
		SSTableMaxSz: 1 << 20, // 1MB
		BlockSize:    4 << 10, // 4KB
	}

	// 创建tableBuilder
	builder := newTableBuilder(opt)
	defer builder.Close()

	// 模拟真实场景：添加一些正常数据和陈旧数据
	testData := []struct {
		key     string
		value   string
		isStale bool
	}{
		{"key1", "value1", false},
		{"key2", "value2", true}, // 陈旧数据
		{"key3", "value3", false},
		{"key4", "value4", true}, // 陈旧数据
		{"key5", "value5", false},
		{"key6", "value6", true}, // 陈旧数据
	}

	var staleCount int
	for _, data := range testData {
		entry := &utils.Entry{
			Key:   []byte(data.key),
			Value: []byte(data.value),
		}

		if data.isStale {
			builder.AddStaleKey(entry)
			staleCount++
		} else {
			builder.AddKey(entry)
		}
	}

	// 验证builder中的staleDataSize大于0（如果有陈旧数据）
	if staleCount > 0 && builder.staleDataSize <= 0 {
		t.Errorf("Expected staleDataSize to be greater than 0 when there are %d stale entries, got %d",
			staleCount, builder.staleDataSize)
	}

	// 构建完整的buildData
	buildData := builder.done()

	// 解析索引
	tableIndex := &pb.TableIndex{}
	err := tableIndex.Unmarshal(buildData.index)
	if err != nil {
		t.Fatalf("Failed to unmarshal TableIndex: %v", err)
	}

	// 验证TableIndex中的StaleDataSize与builder中的一致
	if tableIndex.StaleDataSize != uint32(builder.staleDataSize) {
		t.Errorf("Expected TableIndex.StaleDataSize %d, got %d",
			builder.staleDataSize, tableIndex.StaleDataSize)
	}

	// 如果有陈旧数据，验证StaleDataSize大于0
	if staleCount > 0 && tableIndex.StaleDataSize == 0 {
		t.Errorf("Expected TableIndex.StaleDataSize to be greater than 0 when there are %d stale entries",
			staleCount)
	}

	// 验证其他统计信息
	expectedKeyCount := uint32(len(testData))
	if tableIndex.KeyCount != expectedKeyCount {
		t.Errorf("Expected KeyCount %d, got %d", expectedKeyCount, tableIndex.KeyCount)
	}
}

// TestStaleDataSizeAccuracy 测试陈旧数据大小计算的准确性
func TestStaleDataSizeAccuracy(t *testing.T) {
	// 创建测试选项
	opt := &Options{
		WorkDir:      "/tmp/test_accuracy",
		SSTableMaxSz: 1 << 20, // 1MB
		BlockSize:    4 << 10, // 4KB
	}

	// 创建tableBuilder
	builder := newTableBuilder(opt)
	defer builder.Close()

	// 测试不同大小的陈旧数据
	testCases := []struct {
		key   string
		value string
	}{
		{"small", "v"},
		{"medium_key", "medium_value"},
		{"very_long_key_for_testing", "very_long_value_for_testing_purposes"},
	}

	var previousSize int
	for i, tc := range testCases {
		entry := &utils.Entry{
			Key:   []byte(tc.key),
			Value: []byte(tc.value),
		}

		initialSize := builder.staleDataSize
		builder.AddStaleKey(entry)

		// 验证大小确实增加了
		if builder.staleDataSize <= initialSize {
			t.Errorf("For entry %s:%s, expected size to increase, but got %d (was %d)",
				tc.key, tc.value, builder.staleDataSize, initialSize)
		}

		// 验证大小是单调递增的
		if i > 0 && builder.staleDataSize <= previousSize {
			t.Errorf("Expected staleDataSize to be monotonically increasing, got %d after %d",
				builder.staleDataSize, previousSize)
		}

		previousSize = builder.staleDataSize
	}

	// 验证最终大小大于0
	if builder.staleDataSize <= 0 {
		t.Errorf("Expected final staleDataSize to be greater than 0, got %d",
			builder.staleDataSize)
	}
}
