/*
JadeDB 引擎切换功能测试

本测试文件验证多引擎支持和无感切换功能的正确性。
包括：
1. 传统模式和适配器模式的基本功能
2. 引擎切换的正确性
3. 数据迁移的完整性
4. API兼容性验证
*/

package JadeDB

import (
	"fmt"
	"os"
	"testing"

	"github.com/util6/JadeDB/utils"
)

// TestLegacyMode 测试传统模式
func TestLegacyMode(t *testing.T) {
	// 清理测试目录
	testDir := "./test_data/legacy"
	os.RemoveAll(testDir)
	os.MkdirAll(testDir, 0755)
	defer os.RemoveAll(testDir)

	// 创建传统模式数据库
	opt := &Options{
		WorkDir:      testDir,
		MemTableSize: 1 << 20, // 1MB
		SSTableMaxSz: 1 << 20, // 1MB
	}

	db := Open(opt)
	defer db.Close()

	// 验证引擎类型
	if db.GetEngineMode() != LegacyMode {
		t.Errorf("Expected LegacyMode, got %v", db.GetEngineMode())
	}

	if db.GetCurrentEngineType() != LSMEngine {
		t.Errorf("Expected LSMEngine, got %v", db.GetCurrentEngineType())
	}

	// 测试基本操作
	testBasicOperations(t, db)
}

// TestAdapterModeWithLSM 测试适配器模式使用LSM引擎
func TestAdapterModeWithLSM(t *testing.T) {
	// 清理测试目录
	testDir := "./test_data/adapter_lsm"
	os.RemoveAll(testDir)
	os.MkdirAll(testDir, 0755)
	defer os.RemoveAll(testDir)

	// 创建适配器模式数据库，使用LSM引擎
	opt := &Options{
		WorkDir:      testDir,
		MemTableSize: 1 << 20, // 1MB
		SSTableMaxSz: 1 << 20, // 1MB
	}

	db := OpenWithEngine(opt, AdapterMode, LSMEngine)
	defer db.Close()

	// 验证引擎类型
	if db.GetEngineMode() != AdapterMode {
		t.Errorf("Expected AdapterMode, got %v", db.GetEngineMode())
	}

	if db.GetCurrentEngineType() != LSMEngine {
		t.Errorf("Expected LSMEngine, got %v", db.GetCurrentEngineType())
	}

	// 测试基本操作
	testBasicOperations(t, db)
}

// TestAdapterModeWithBTree 测试适配器模式使用B+树引擎
func TestAdapterModeWithBTree(t *testing.T) {
	// 清理测试目录
	testDir := "./test_data/adapter_btree"
	os.RemoveAll(testDir)
	os.MkdirAll(testDir, 0755)
	defer os.RemoveAll(testDir)

	// 创建适配器模式数据库，使用B+树引擎
	opt := &Options{
		WorkDir:      testDir,
		MemTableSize: 1 << 20, // 1MB
		SSTableMaxSz: 1 << 20, // 1MB
	}

	db := OpenWithEngine(opt, AdapterMode, BTreeEngine)
	defer db.Close()

	// 验证引擎类型
	if db.GetEngineMode() != AdapterMode {
		t.Errorf("Expected AdapterMode, got %v", db.GetEngineMode())
	}

	if db.GetCurrentEngineType() != BTreeEngine {
		t.Errorf("Expected BTreeEngine, got %v", db.GetCurrentEngineType())
	}

	// 测试基本操作
	testBasicOperations(t, db)
}

// TestEngineSwitching 测试引擎切换功能
func TestEngineSwitching(t *testing.T) {
	// 清理测试目录
	testDir := "./test_data/switching"
	os.RemoveAll(testDir)
	os.MkdirAll(testDir, 0755)
	defer os.RemoveAll(testDir)

	// 创建适配器模式数据库，初始使用LSM引擎
	opt := &Options{
		WorkDir:      testDir,
		MemTableSize: 1 << 20, // 1MB
		SSTableMaxSz: 1 << 20, // 1MB
	}

	db := OpenWithEngine(opt, AdapterMode, LSMEngine)
	defer db.Close()

	// 写入测试数据
	testData := make(map[string]string)
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("key_%d", i)
		value := fmt.Sprintf("value_%d", i)
		testData[key] = value

		err := db.Set(&utils.Entry{
			Key:   []byte(key),
			Value: []byte(value),
		})
		if err != nil {
			t.Fatalf("Failed to set %s: %v", key, err)
		}
	}

	// 验证数据写入成功
	for key, expectedValue := range testData {
		entry, err := db.Get([]byte(key))
		if err != nil {
			t.Fatalf("Failed to get %s: %v", key, err)
		}
		if string(entry.Value) != expectedValue {
			t.Errorf("Expected %s, got %s for key %s", expectedValue, entry.Value, key)
		}
	}

	// 切换到B+树引擎
	err := db.SwitchEngine(BTreeEngine)
	if err != nil {
		t.Fatalf("Failed to switch to BTreeEngine: %v", err)
	}

	// 验证引擎切换成功
	if db.GetCurrentEngineType() != BTreeEngine {
		t.Errorf("Expected BTreeEngine after switch, got %v", db.GetCurrentEngineType())
	}

	// 验证数据迁移成功
	for key, expectedValue := range testData {
		entry, err := db.Get([]byte(key))
		if err != nil {
			t.Fatalf("Failed to get %s after switch: %v", key, err)
		}
		if string(entry.Value) != expectedValue {
			t.Errorf("Expected %s, got %s for key %s after switch", expectedValue, entry.Value, key)
		}
	}

	// 在新引擎上写入更多数据
	for i := 100; i < 200; i++ {
		key := fmt.Sprintf("new_key_%d", i)
		value := fmt.Sprintf("new_value_%d", i)
		testData[key] = value

		err := db.Set(&utils.Entry{
			Key:   []byte(key),
			Value: []byte(value),
		})
		if err != nil {
			t.Fatalf("Failed to set %s on new engine: %v", key, err)
		}
	}

	// 切换回LSM引擎
	err = db.SwitchEngine(LSMEngine)
	if err != nil {
		t.Fatalf("Failed to switch back to LSMEngine: %v", err)
	}

	// 验证所有数据都可访问
	for key, expectedValue := range testData {
		entry, err := db.Get([]byte(key))
		if err != nil {
			t.Fatalf("Failed to get %s after switch back: %v", key, err)
		}
		if string(entry.Value) != expectedValue {
			t.Errorf("Expected %s, got %s for key %s after switch back", expectedValue, entry.Value, key)
		}
	}
}

// TestEngineSwitchingInLegacyMode 测试传统模式下的引擎切换（应该失败）
func TestEngineSwitchingInLegacyMode(t *testing.T) {
	// 清理测试目录
	testDir := "./test_data/legacy_switch"
	os.RemoveAll(testDir)
	os.MkdirAll(testDir, 0755)
	defer os.RemoveAll(testDir)

	// 创建传统模式数据库
	opt := &Options{
		WorkDir:      testDir,
		MemTableSize: 1 << 20, // 1MB
		SSTableMaxSz: 1 << 20, // 1MB
	}

	db := Open(opt)
	defer db.Close()

	// 尝试切换引擎，应该失败
	err := db.SwitchEngine(BTreeEngine)
	if err == nil {
		t.Error("Expected error when switching engine in legacy mode, but got nil")
	}
}

// TestEngineStats 测试引擎统计信息
func TestEngineStats(t *testing.T) {
	// 清理测试目录
	testDir := "./test_data/stats"
	os.RemoveAll(testDir)
	os.MkdirAll(testDir, 0755)
	defer os.RemoveAll(testDir)

	// 创建适配器模式数据库
	opt := &Options{
		WorkDir:      testDir,
		MemTableSize: 1 << 20, // 1MB
		SSTableMaxSz: 1 << 20, // 1MB
	}

	db := OpenWithEngine(opt, AdapterMode, LSMEngine)
	defer db.Close()

	// 执行一些操作
	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("stats_key_%d", i)
		value := fmt.Sprintf("stats_value_%d", i)

		err := db.Set(&utils.Entry{
			Key:   []byte(key),
			Value: []byte(value),
		})
		if err != nil {
			t.Fatalf("Failed to set %s: %v", key, err)
		}

		_, err = db.Get([]byte(key))
		if err != nil {
			t.Fatalf("Failed to get %s: %v", key, err)
		}
	}

	// 获取统计信息
	stats := db.GetEngineStats()
	if stats == nil {
		t.Error("Expected non-nil stats")
	}

	// 验证统计信息包含预期字段
	expectedFields := []string{"engine_mode", "current_engine", "total_operations"}
	for _, field := range expectedFields {
		if _, exists := stats[field]; !exists {
			t.Errorf("Expected stats to contain field %s", field)
		}
	}
}

// TestListAvailableEngines 测试列出可用引擎
func TestListAvailableEngines(t *testing.T) {
	// 清理测试目录
	testDir := "./test_data/list_engines"
	os.RemoveAll(testDir)
	os.MkdirAll(testDir, 0755)
	defer os.RemoveAll(testDir)

	// 创建数据库实例
	opt := &Options{
		WorkDir:      testDir,
		MemTableSize: 1 << 20, // 1MB
		SSTableMaxSz: 1 << 20, // 1MB
	}

	db := OpenWithEngine(opt, AdapterMode, LSMEngine)
	defer db.Close()

	// 列出可用引擎
	engines := db.ListAvailableEngines()
	if len(engines) == 0 {
		t.Error("Expected at least one available engine")
	}

	// 验证包含预期的引擎类型
	expectedEngines := []EngineType{LSMEngine, BTreeEngine}
	for _, expected := range expectedEngines {
		found := false
		for _, engine := range engines {
			if engine == expected {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("Expected engine %v to be available", expected)
		}
	}
}

// testBasicOperations 测试基本数据库操作
func testBasicOperations(t *testing.T, db *DB) {
	// 测试Set和Get
	key := []byte("test_key")
	value := []byte("test_value")

	err := db.Set(&utils.Entry{
		Key:   key,
		Value: value,
	})
	if err != nil {
		t.Fatalf("Failed to set key: %v", err)
	}

	entry, err := db.Get(key)
	if err != nil {
		t.Fatalf("Failed to get key: %v", err)
	}

	if string(entry.Key) != string(key) {
		t.Errorf("Expected key %s, got %s", key, entry.Key)
	}

	if string(entry.Value) != string(value) {
		t.Errorf("Expected value %s, got %s", value, entry.Value)
	}

	// 测试Del
	err = db.Del(key)
	if err != nil {
		t.Fatalf("Failed to delete key: %v", err)
	}

	_, err = db.Get(key)
	if err == nil {
		t.Error("Expected error when getting deleted key, but got nil")
	}

	// 测试不存在的键
	_, err = db.Get([]byte("non_existent_key"))
	if err == nil {
		t.Error("Expected error when getting non-existent key, but got nil")
	}
}

// BenchmarkEngineSwitching 引擎切换性能基准测试
func BenchmarkEngineSwitching(b *testing.B) {
	// 清理测试目录
	testDir := "./test_data/benchmark"
	os.RemoveAll(testDir)
	os.MkdirAll(testDir, 0755)
	defer os.RemoveAll(testDir)

	// 创建适配器模式数据库
	opt := &Options{
		WorkDir:      testDir,
		MemTableSize: 1 << 20, // 1MB
		SSTableMaxSz: 1 << 20, // 1MB
	}

	db := OpenWithEngine(opt, AdapterMode, LSMEngine)
	defer db.Close()

	// 写入一些测试数据
	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("bench_key_%d", i)
		value := fmt.Sprintf("bench_value_%d", i)

		err := db.Set(&utils.Entry{
			Key:   []byte(key),
			Value: []byte(value),
		})
		if err != nil {
			b.Fatalf("Failed to set %s: %v", key, err)
		}
	}

	b.ResetTimer()

	// 基准测试引擎切换
	for i := 0; i < b.N; i++ {
		// 切换到B+树引擎
		err := db.SwitchEngine(BTreeEngine)
		if err != nil {
			b.Fatalf("Failed to switch to BTreeEngine: %v", err)
		}

		// 切换回LSM引擎
		err = db.SwitchEngine(LSMEngine)
		if err != nil {
			b.Fatalf("Failed to switch back to LSMEngine: %v", err)
		}
	}
}
