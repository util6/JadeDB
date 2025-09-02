package JadeDB

import (
	"math"
	"os"
	"testing"

	"github.com/util6/JadeDB/lsm"
	"github.com/util6/JadeDB/utils"
)

// TestDirectDelete 直接测试删除功能，不使用复杂的初始化
func TestDirectDelete(t *testing.T) {
	// 清理测试目录
	testDir := "./test_data/direct_delete"
	os.RemoveAll(testDir)
	os.MkdirAll(testDir, 0755)
	defer os.RemoveAll(testDir)

	// 创建LSM实例
	lsmInstance := lsm.NewLSM(&lsm.Options{
		WorkDir:             testDir,
		MemTableSize:        1 << 20, // 1MB
		SSTableMaxSz:        1 << 20, // 1MB
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

	defer lsmInstance.Close()

	// 测试键值
	key := []byte("direct_delete_key")
	value := []byte("direct_delete_value")
	keyWithTs := utils.KeyWithTs(key, math.MaxUint32)

	// 1. 写入数据
	err := lsmInstance.Set(&utils.Entry{
		Key:   keyWithTs,
		Value: value,
	})
	if err != nil {
		t.Fatalf("Failed to set value: %v", err)
	}

	// 2. 验证数据存在
	entry, err := lsmInstance.Get(keyWithTs)
	if err != nil {
		t.Fatalf("Failed to get value: %v", err)
	}
	if string(entry.Value) != string(value) {
		t.Errorf("Expected value %s, got %s", value, entry.Value)
	}
	t.Logf("Before delete: key=%s, value=%s, meta=%d", entry.Key, entry.Value, entry.Meta)

	// 3. 写入删除条目
	deleteEntry := &utils.Entry{
		Key:   keyWithTs,
		Value: nil,
		Meta:  utils.BitDelete,
	}
	err = lsmInstance.Set(deleteEntry)
	if err != nil {
		t.Fatalf("Failed to set delete entry: %v", err)
	}
	t.Logf("Delete entry written")

	// 4. 验证删除条目
	entry, err = lsmInstance.Get(keyWithTs)
	if err != nil {
		t.Logf("Get returned error after delete: %v", err)
	} else {
		t.Logf("After delete: key=%s, value=%v, meta=%d", entry.Key, entry.Value, entry.Meta)

		// 检查是否被正确标记为删除
		if lsm.IsDeletedOrExpired(entry) {
			t.Logf("Entry correctly identified as deleted")
		} else {
			t.Errorf("Entry not identified as deleted: Meta=%d", entry.Meta)
		}
	}
}

// TestFullDeleteFlow 测试完整的删除流程
func TestFullDeleteFlow(t *testing.T) {
	// 清理测试目录
	testDir := "./test_data/full_delete"
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

	// 创建数据库实例，使用传统模式
	db := Open(opt)
	defer db.Close()

	// 测试键值
	key := []byte("full_delete_key")
	value := []byte("full_delete_value")

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
		t.Fatalf("Failed to get value: %v", err)
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
		t.Errorf("Expected error when getting deleted key, but got entry: key=%s, value=%s, meta=%d",
			entry.Key, entry.Value, entry.Meta)
	} else {
		if err == utils.ErrKeyNotFound {
			t.Logf("Delete test passed: got expected ErrKeyNotFound")
		} else {
			t.Logf("Delete test passed: got error: %v", err)
		}
	}
}
