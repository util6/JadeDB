package JadeDB

import (
	"fmt"
	"os"
	"testing"

	"github.com/util6/JadeDB/storage"
	"github.com/util6/JadeDB/utils"
)

// TestFixVerification 验证修复的功能
func TestFixVerification(t *testing.T) {
	// 清理测试目录
	testDir := "./test_data/fix_verification"
	os.RemoveAll(testDir)
	os.MkdirAll(testDir, 0755)
	defer os.RemoveAll(testDir)

	// 测试传统模式基本功能
	t.Run("LegacyModeBasic", func(t *testing.T) {
		opt := &Options{
			WorkDir:        testDir + "/legacy",
			MemTableSize:   1 << 20,
			SSTableMaxSz:   1 << 20,
			ValueThreshold: 1024,
		}

		db := OpenWithEngine(opt, LegacyMode, storage.LSMTreeEngine)
		defer db.Close()

		// 测试写入和读取
		key := []byte("test_key")
		value := []byte("test_value")

		err := db.Set(&utils.Entry{Key: key, Value: value})
		if err != nil {
			t.Fatalf("Failed to set: %v", err)
		}

		entry, err := db.Get(key)
		if err != nil {
			t.Fatalf("Failed to get: %v", err)
		}

		if string(entry.Value) != string(value) {
			t.Errorf("Value mismatch: expected %s, got %s", value, entry.Value)
		}

		// 测试删除
		err = db.Del(key)
		if err != nil {
			t.Fatalf("Failed to delete: %v", err)
		}

		_, err = db.Get(key)
		if err == nil {
			t.Error("Expected error when getting deleted key")
		}

		t.Logf("Legacy mode basic test: PASSED")
	})

	// 测试大值存储（值日志）
	t.Run("LargeValueStorage", func(t *testing.T) {
		opt := &Options{
			WorkDir:        testDir + "/large",
			MemTableSize:   1 << 20,
			SSTableMaxSz:   1 << 20,
			ValueThreshold: 512, // 降低阈值
		}

		db := OpenWithEngine(opt, LegacyMode, storage.LSMTreeEngine)
		defer db.Close()

		// 测试大值
		key := []byte("large_key")
		value := make([]byte, 1024) // 1KB，大于阈值
		for i := range value {
			value[i] = byte(i % 256)
		}

		err := db.Set(&utils.Entry{Key: key, Value: value})
		if err != nil {
			t.Fatalf("Failed to set large value: %v", err)
		}

		entry, err := db.Get(key)
		if err != nil {
			t.Fatalf("Failed to get large value: %v", err)
		}

		if len(entry.Value) != len(value) {
			t.Errorf("Large value length mismatch: expected %d, got %d", len(value), len(entry.Value))
		}

		// 验证内容
		for i, b := range entry.Value {
			if b != value[i] {
				t.Errorf("Large value content mismatch at index %d: expected %d, got %d", i, value[i], b)
				break
			}
		}

		t.Logf("Large value storage test: PASSED")
	})

	// 测试引擎信息
	t.Run("EngineInfo", func(t *testing.T) {
		opt := &Options{
			WorkDir:        testDir + "/info",
			MemTableSize:   1 << 20,
			SSTableMaxSz:   1 << 20,
			ValueThreshold: 1024,
		}

		// 测试传统模式
		db := OpenWithEngine(opt, LegacyMode, storage.LSMTreeEngine)
		defer db.Close()

		if db.GetEngineMode() != LegacyMode {
			t.Errorf("Expected LegacyMode, got %v", db.GetEngineMode())
		}

		if db.GetCurrentEngineType() != storage.LSMTreeEngine {
			t.Errorf("Expected LSMTreeEngine, got %v", db.GetCurrentEngineType())
		}

		engines := db.ListAvailableEngines()
		if len(engines) == 0 {
			t.Error("Expected available engines, got empty list")
		}

		t.Logf("Engine info test: PASSED")
		t.Logf("  Mode: %v", db.GetEngineMode())
		t.Logf("  Type: %v", db.GetCurrentEngineType())
		t.Logf("  Available: %v", engines)
	})
}

// TestQuickPerformance 快速性能测试
func TestQuickPerformance(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping performance test in short mode")
	}

	testDir := "./test_data/quick_perf"
	os.RemoveAll(testDir)
	os.MkdirAll(testDir, 0755)
	defer os.RemoveAll(testDir)

	opt := &Options{
		WorkDir:        testDir,
		MemTableSize:   1 << 20,
		SSTableMaxSz:   1 << 20,
		ValueThreshold: 1024,
	}

	db := OpenWithEngine(opt, LegacyMode, storage.LSMTreeEngine)
	defer db.Close()

	// 写入100个条目
	numEntries := 100
	for i := 0; i < numEntries; i++ {
		key := []byte(fmt.Sprintf("perf_key_%03d", i))
		value := []byte(fmt.Sprintf("perf_value_%03d", i))

		err := db.Set(&utils.Entry{Key: key, Value: value})
		if err != nil {
			t.Fatalf("Failed to set entry %d: %v", i, err)
		}
	}

	// 读取所有条目
	for i := 0; i < numEntries; i++ {
		key := []byte(fmt.Sprintf("perf_key_%03d", i))
		expectedValue := fmt.Sprintf("perf_value_%03d", i)

		entry, err := db.Get(key)
		if err != nil {
			t.Fatalf("Failed to get entry %d: %v", i, err)
		}

		if string(entry.Value) != expectedValue {
			t.Errorf("Value mismatch for entry %d: expected %s, got %s", i, expectedValue, entry.Value)
		}
	}

	t.Logf("Quick performance test: PASSED (%d entries)", numEntries)
}
