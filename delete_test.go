package JadeDB

import (
	"math"
	"os"
	"testing"

	"github.com/util6/JadeDB/lsm"
	"github.com/util6/JadeDB/utils"
)

// TestDeleteFunction 测试删除功能
func TestDeleteFunction(t *testing.T) {
	// 清理测试目录
	testDir := "./test_data/delete"
	os.RemoveAll(testDir)
	os.MkdirAll(testDir, 0755)
	defer os.RemoveAll(testDir)

	// 创建数据库配置
	opt := &Options{
		WorkDir:        testDir,
		MemTableSize:   1 << 20, // 1MB
		SSTableMaxSz:   1 << 20, // 1MB
		ValueThreshold: 1024,    // 1KB
	}

	// 手动创建数据库实例，不启动压缩器
	db := &DB{
		opt: opt,
	}

	// 初始化值日志
	db.initVLog()

	// 手动初始化LSM，但不启动压缩器
	db.lsm = lsm.NewLSM(&lsm.Options{
		WorkDir:             db.opt.WorkDir,
		MemTableSize:        db.opt.MemTableSize,
		SSTableMaxSz:        db.opt.SSTableMaxSz,
		BlockSize:           8 * 1024,
		BloomFalsePositive:  0,
		BaseLevelSize:       10 << 20,
		LevelSizeMultiplier: 10,
		BaseTableSize:       5 << 20,
		TableSizeMultiplier: 2,
		NumLevelZeroTables:  15,
		MaxLevelNum:         7,
		NumCompactors:       0, // 不启动压缩器
	})

	// 初始化统计信息
	db.stats = newStats(db.opt)

	// 确保在测试结束时关闭数据库
	defer func() {
		if err := db.lsm.Close(); err != nil {
			t.Errorf("Failed to close LSM: %v", err)
		}
		if err := db.vlog.close(); err != nil {
			t.Errorf("Failed to close vlog: %v", err)
		}
	}()

	// 测试删除功能
	key := []byte("delete_test_key")
	value := []byte("delete_test_value")

	// 1. 写入数据
	err := db.Set(&utils.Entry{
		Key:   key,
		Value: value,
	})
	if err != nil {
		t.Fatalf("Failed to set value: %v", err)
	}

	// 2. 验证数据存在
	entry, err := db.Get(key)
	if err != nil {
		t.Fatalf("Failed to get value before delete: %v", err)
	}
	if string(entry.Value) != string(value) {
		t.Errorf("Expected value %s, got %s", value, entry.Value)
	}
	t.Logf("Before delete: key=%s, value=%s", entry.Key, entry.Value)

	// 3. 删除数据
	err = db.Del(key)
	if err != nil {
		t.Fatalf("Failed to delete key: %v", err)
	}
	t.Logf("Delete operation completed")

	// 4. 验证数据已删除
	entry, err = db.Get(key)
	if err == nil {
		t.Errorf("Expected error when getting deleted key, but got entry: %+v", entry)
		if entry != nil {
			t.Errorf("Entry details: Key=%s, Value=%s, Meta=%d", entry.Key, entry.Value, entry.Meta)
		}
	} else {
		t.Logf("Delete test passed: got expected error: %v", err)
	}

	// 5. 测试删除不存在的键
	nonExistentKey := []byte("non_existent_key")
	err = db.Del(nonExistentKey)
	if err != nil {
		t.Errorf("Delete non-existent key should not fail, but got: %v", err)
	}
	t.Logf("Delete non-existent key test passed")
}

// TestDeleteWithBitCheck 测试删除标记的设置
func TestDeleteWithBitCheck(t *testing.T) {
	// 清理测试目录
	testDir := "./test_data/delete_bit"
	os.RemoveAll(testDir)
	os.MkdirAll(testDir, 0755)
	defer os.RemoveAll(testDir)

	// 创建数据库配置
	opt := &Options{
		WorkDir:        testDir,
		MemTableSize:   1 << 20, // 1MB
		SSTableMaxSz:   1 << 20, // 1MB
		ValueThreshold: 1024,    // 1KB
	}

	// 手动创建数据库实例，不启动压缩器
	db := &DB{
		opt: opt,
	}

	// 初始化值日志
	db.initVLog()

	// 手动初始化LSM，但不启动压缩器
	db.lsm = lsm.NewLSM(&lsm.Options{
		WorkDir:             db.opt.WorkDir,
		MemTableSize:        db.opt.MemTableSize,
		SSTableMaxSz:        db.opt.SSTableMaxSz,
		BlockSize:           8 * 1024,
		BloomFalsePositive:  0,
		BaseLevelSize:       10 << 20,
		LevelSizeMultiplier: 10,
		BaseTableSize:       5 << 20,
		TableSizeMultiplier: 2,
		NumLevelZeroTables:  15,
		MaxLevelNum:         7,
		NumCompactors:       0, // 不启动压缩器
	})

	// 初始化统计信息
	db.stats = newStats(db.opt)

	// 确保在测试结束时关闭数据库
	defer func() {
		if err := db.lsm.Close(); err != nil {
			t.Errorf("Failed to close LSM: %v", err)
		}
		if err := db.vlog.close(); err != nil {
			t.Errorf("Failed to close vlog: %v", err)
		}
	}()

	// 直接测试删除标记的设置
	key := []byte("bit_test_key")

	// 创建删除条目
	deleteEntry := &utils.Entry{
		Key:   key,
		Value: nil,
		Meta:  utils.BitDelete,
	}

	// 写入删除条目
	err := db.Set(deleteEntry)
	if err != nil {
		t.Fatalf("Failed to set delete entry: %v", err)
	}

	// 直接从LSM读取，不经过Get方法的过滤
	keyWithTs := utils.KeyWithTs(key, math.MaxUint32)
	rawEntry, err := db.lsm.Get(keyWithTs)
	if err != nil {
		t.Fatalf("Failed to get raw entry from LSM: %v", err)
	}

	t.Logf("Raw entry from LSM: Key=%s, Value=%v, Meta=%d", rawEntry.Key, rawEntry.Value, rawEntry.Meta)

	// 检查删除标记是否正确设置
	if rawEntry.Meta&utils.BitDelete == 0 {
		t.Errorf("Delete bit not set correctly. Meta=%d, expected to have BitDelete=%d", rawEntry.Meta, utils.BitDelete)
	} else {
		t.Logf("Delete bit correctly set: Meta=%d", rawEntry.Meta)
	}

	// 测试IsDeletedOrExpired函数
	if !lsm.IsDeletedOrExpired(rawEntry) {
		t.Errorf("IsDeletedOrExpired should return true for deleted entry")
	} else {
		t.Logf("IsDeletedOrExpired correctly identified deleted entry")
	}
}
