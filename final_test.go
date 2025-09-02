package JadeDB

import (
	"fmt"
	"os"
	"testing"

	"github.com/util6/JadeDB/storage"
	"github.com/util6/JadeDB/utils"
)

// TestFinalIntegration 最终集成测试
func TestFinalIntegration(t *testing.T) {
	// 清理测试目录
	testDir := "./test_data/final"
	os.RemoveAll(testDir)
	os.MkdirAll(testDir, 0755)
	defer os.RemoveAll(testDir)

	// 测试传统模式的完整功能
	t.Run("LegacyModeComplete", func(t *testing.T) {
		opt := &Options{
			WorkDir:        testDir + "/legacy",
			MemTableSize:   1 << 20,
			SSTableMaxSz:   1 << 20,
			ValueThreshold: 1024,
		}

		db := OpenWithEngine(opt, LegacyMode, storage.LSMTreeEngine)
		defer db.Close()

		// 测试小值存储
		smallKey := []byte("small_key")
		smallValue := []byte("small_value")
		err := db.Set(&utils.Entry{Key: smallKey, Value: smallValue})
		if err != nil {
			t.Fatalf("Failed to set small value: %v", err)
		}

		entry, err := db.Get(smallKey)
		if err != nil {
			t.Fatalf("Failed to get small value: %v", err)
		}
		if string(entry.Value) != string(smallValue) {
			t.Errorf("Small value mismatch: expected %s, got %s", smallValue, entry.Value)
		}

		// 测试大值存储（值日志）
		largeKey := []byte("large_key")
		largeValue := make([]byte, 2048) // 2KB
		for i := range largeValue {
			largeValue[i] = byte(i % 256)
		}
		err = db.Set(&utils.Entry{Key: largeKey, Value: largeValue})
		if err != nil {
			t.Fatalf("Failed to set large value: %v", err)
		}

		entry, err = db.Get(largeKey)
		if err != nil {
			t.Fatalf("Failed to get large value: %v", err)
		}
		if len(entry.Value) != len(largeValue) {
			t.Errorf("Large value length mismatch: expected %d, got %d", len(largeValue), len(entry.Value))
		}

		// 测试删除功能
		err = db.Del(smallKey)
		if err != nil {
			t.Fatalf("Failed to delete key: %v", err)
		}

		_, err = db.Get(smallKey)
		if err == nil {
			t.Error("Expected error when getting deleted key")
		}

		// 测试引擎切换失败
		err = db.SwitchEngine(storage.BPlusTreeEngine)
		if err == nil {
			t.Error("Expected error when switching engine in legacy mode")
		}

		t.Logf("Legacy mode complete test: PASSED")
	})

	// 测试适配器模式的完整功能
	t.Run("AdapterModeComplete", func(t *testing.T) {
		opt := &Options{
			WorkDir:        testDir + "/adapter",
			MemTableSize:   1 << 20,
			SSTableMaxSz:   1 << 20,
			ValueThreshold: 1024,
		}

		db := OpenWithEngine(opt, AdapterMode, storage.LSMTreeEngine)
		defer db.Close()

		// 在LSM引擎中写入数据
		lsmKey := []byte("lsm_data")
		lsmValue := []byte("stored_in_lsm")
		err := db.Set(&utils.Entry{Key: lsmKey, Value: lsmValue})
		if err != nil {
			t.Fatalf("Failed to set in LSM: %v", err)
		}

		entry, err := db.Get(lsmKey)
		if err != nil {
			t.Fatalf("Failed to get from LSM: %v", err)
		}
		if string(entry.Value) != string(lsmValue) {
			t.Errorf("LSM value mismatch: expected %s, got %s", lsmValue, entry.Value)
		}

		// 切换到B+树引擎
		err = db.SwitchEngine(storage.BPlusTreeEngine)
		if err != nil {
			t.Fatalf("Failed to switch to BPlusTree: %v", err)
		}

		if db.GetCurrentEngineType() != storage.BPlusTreeEngine {
			t.Errorf("Expected current engine to be BPlusTree, got %v", db.GetCurrentEngineType())
		}

		// 在B+树引擎中写入数据
		btreeKey := []byte("btree_data")
		btreeValue := []byte("stored_in_btree")
		err = db.Set(&utils.Entry{Key: btreeKey, Value: btreeValue})
		if err != nil {
			t.Fatalf("Failed to set in BPlusTree: %v", err)
		}

		entry, err = db.Get(btreeKey)
		if err != nil {
			t.Fatalf("Failed to get from BPlusTree: %v", err)
		}
		if string(entry.Value) != string(btreeValue) {
			t.Errorf("BPlusTree value mismatch: expected %s, got %s", btreeValue, entry.Value)
		}

		// 测试删除功能
		err = db.Del(btreeKey)
		if err != nil {
			t.Fatalf("Failed to delete from BPlusTree: %v", err)
		}

		_, err = db.Get(btreeKey)
		if err == nil {
			t.Error("Expected error when getting deleted key from BPlusTree")
		}

		// 切换回LSM引擎
		err = db.SwitchEngine(storage.LSMTreeEngine)
		if err != nil {
			t.Fatalf("Failed to switch back to LSMTree: %v", err)
		}

		if db.GetCurrentEngineType() != storage.LSMTreeEngine {
			t.Errorf("Expected current engine to be LSMTree, got %v", db.GetCurrentEngineType())
		}

		t.Logf("Adapter mode complete test: PASSED")
	})

	// 测试混合数据类型
	t.Run("MixedDataTypes", func(t *testing.T) {
		opt := &Options{
			WorkDir:        testDir + "/mixed",
			MemTableSize:   1 << 20,
			SSTableMaxSz:   1 << 20,
			ValueThreshold: 512, // 降低阈值以测试值日志
		}

		db := OpenWithEngine(opt, LegacyMode, storage.LSMTreeEngine)
		defer db.Close()

		// 测试不同大小的数据
		testCases := []struct {
			key  string
			size int
			desc string
		}{
			{"tiny", 10, "tiny value"},
			{"small", 100, "small value"},
			{"medium", 600, "medium value (vlog)"},
			{"large", 1500, "large value (vlog)"},
		}

		for _, tc := range testCases {
			key := []byte(tc.key)
			value := make([]byte, tc.size)
			for i := range value {
				value[i] = byte(i%256 + 65) // A-Z循环
			}

			// 写入
			err := db.Set(&utils.Entry{Key: key, Value: value})
			if err != nil {
				t.Fatalf("Failed to set %s: %v", tc.desc, err)
			}

			// 读取
			entry, err := db.Get(key)
			if err != nil {
				t.Fatalf("Failed to get %s: %v", tc.desc, err)
			}

			if len(entry.Value) != len(value) {
				t.Errorf("%s length mismatch: expected %d, got %d", tc.desc, len(value), len(entry.Value))
			}

			// 验证内容
			for i, b := range entry.Value {
				if b != value[i] {
					t.Errorf("%s content mismatch at index %d: expected %d, got %d", tc.desc, i, value[i], b)
					break
				}
			}

			t.Logf("%s test: PASSED (size=%d)", tc.desc, len(entry.Value))
		}

		// 测试批量删除
		for _, tc := range testCases {
			key := []byte(tc.key)
			err := db.Del(key)
			if err != nil {
				t.Fatalf("Failed to delete %s: %v", tc.desc, err)
			}

			_, err = db.Get(key)
			if err == nil {
				t.Errorf("Expected error when getting deleted %s", tc.desc)
			}
		}

		t.Logf("Mixed data types test: PASSED")
	})
}

// TestPerformanceBasic 基本性能测试
func TestPerformanceBasic(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping performance test in short mode")
	}

	testDir := "./test_data/perf"
	os.RemoveAll(testDir)
	os.MkdirAll(testDir, 0755)
	defer os.RemoveAll(testDir)

	opt := &Options{
		WorkDir:        testDir,
		MemTableSize:   1 << 20,
		SSTableMaxSz:   1 << 20,
		ValueThreshold: 1024,
	}

	db := OpenWithEngine(opt, LegacyMode, LSMEngine)
	defer db.Close()

	// 写入性能测试
	numEntries := 1000
	t.Logf("Writing %d entries...", numEntries)

	for i := 0; i < numEntries; i++ {
		key := []byte(fmt.Sprintf("perf_key_%04d", i))
		value := []byte(fmt.Sprintf("perf_value_%04d_with_some_additional_data", i))

		err := db.Set(&utils.Entry{Key: key, Value: value})
		if err != nil {
			t.Fatalf("Failed to set entry %d: %v", i, err)
		}
	}

	// 读取性能测试
	t.Logf("Reading %d entries...", numEntries)
	for i := 0; i < numEntries; i++ {
		key := []byte(fmt.Sprintf("perf_key_%04d", i))
		expectedValue := fmt.Sprintf("perf_value_%04d_with_some_additional_data", i)

		entry, err := db.Get(key)
		if err != nil {
			t.Fatalf("Failed to get entry %d: %v", i, err)
		}

		if string(entry.Value) != expectedValue {
			t.Errorf("Value mismatch for entry %d: expected %s, got %s", i, expectedValue, entry.Value)
		}
	}

	t.Logf("Performance test: PASSED (%d entries)", numEntries)
}
