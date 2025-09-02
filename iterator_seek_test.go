package JadeDB

import (
	"testing"

	"github.com/util6/JadeDB/utils"
)

// TestIteratorSeek 测试迭代器的Seek功能
func TestIteratorSeek(t *testing.T) {
	// 创建测试数据库
	opt := &Options{
		WorkDir:      "./test_seek_db",
		MemTableSize: 1 << 20, // 1MB
		SSTableMaxSz: 1 << 20, // 1MB
	}
	defer func() {
		// 清理测试数据
		utils.RemoveDir("./test_seek_db")
	}()

	db := Open(opt)
	defer db.Close()

	// 插入测试数据
	testData := map[string]string{
		"key1": "value1",
		"key2": "value2",
		"key3": "value3",
		"key5": "value5",
		"key7": "value7",
	}

	for k, v := range testData {
		entry := utils.NewEntry([]byte(k), []byte(v))
		err := db.Set(entry)
		if err != nil {
			t.Fatalf("Failed to set %s: %v", k, err)
		}
	}

	// 创建迭代器
	iter := db.NewIterator(&utils.Options{IsAsc: true})
	defer iter.Close()

	// 测试Seek功能
	testCases := []struct {
		seekKey     string
		expectedKey string
		shouldFind  bool
	}{
		{"key1", "key1", true}, // 精确匹配
		{"key4", "key5", true}, // 不存在的键，应该定位到下一个
		{"key6", "key7", true}, // 不存在的键，应该定位到下一个
		{"key0", "key1", true}, // 小于所有键，应该定位到第一个
		{"key9", "", false},    // 大于所有键，应该无效
	}

	for _, tc := range testCases {
		t.Run("Seek_"+tc.seekKey, func(t *testing.T) {
			iter.Seek([]byte(tc.seekKey))

			if tc.shouldFind {
				if !iter.Valid() {
					t.Errorf("Expected iterator to be valid after seeking to %s", tc.seekKey)
					return
				}

				item := iter.Item()
				if item == nil {
					t.Errorf("Expected non-nil item after seeking to %s", tc.seekKey)
					return
				}

				entry := item.Entry()
				if entry == nil {
					t.Errorf("Expected non-nil entry after seeking to %s", tc.seekKey)
					return
				}

				actualKey := string(entry.Key)
				if actualKey != tc.expectedKey {
					t.Errorf("Expected key %s, got %s", tc.expectedKey, actualKey)
				}
			} else {
				if iter.Valid() {
					t.Errorf("Expected iterator to be invalid after seeking to %s", tc.seekKey)
				}
			}
		})
	}
}
