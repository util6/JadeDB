package JadeDB

import (
	"testing"
)

// TestEntryObjectPool 测试Entry对象池功能
func TestEntryObjectPool(t *testing.T) {
	// 测试Entry对象池的基本功能
	t.Run("BasicPoolOperations", func(t *testing.T) {
		// 获取Entry对象
		entry1 := getEntry()
		if entry1 == nil {
			t.Fatal("getEntry() returned nil")
		}

		// 设置一些数据
		entry1.Key = []byte("test_key")
		entry1.Value = []byte("test_value")
		entry1.ExpiresAt = 12345
		entry1.Meta = 1

		// 归还到对象池
		putEntry(entry1)

		// 再次获取，应该得到重置后的对象
		entry2 := getEntry()
		if entry2 == nil {
			t.Fatal("getEntry() returned nil after put")
		}

		// 验证对象已被重置
		if len(entry2.Key) != 0 {
			t.Errorf("Entry key not reset, got length %d", len(entry2.Key))
		}
		if len(entry2.Value) != 0 {
			t.Errorf("Entry value not reset, got length %d", len(entry2.Value))
		}
		if entry2.ExpiresAt != 0 {
			t.Errorf("Entry ExpiresAt not reset, got %d", entry2.ExpiresAt)
		}
		if entry2.Meta != 0 {
			t.Errorf("Entry Meta not reset, got %d", entry2.Meta)
		}

		putEntry(entry2)
	})

	// 测试Entry切片的处理
	t.Run("EntrySliceOperations", func(t *testing.T) {
		// 创建Entry切片
		entries := getEntrySlice(3)

		// 添加一些Entry对象
		for i := 0; i < 3; i++ {
			entry := getEntry()
			entry.Key = []byte("key" + string(rune('0'+i)))
			entry.Value = []byte("value" + string(rune('0'+i)))
			entries = append(entries, entry)
		}

		if len(entries) != 3 {
			t.Errorf("Expected 3 entries, got %d", len(entries))
		}

		// 释放所有Entry对象
		putEntrySlice(entries)

		// 验证对象池中有可用对象
		entry := getEntry()
		if entry == nil {
			t.Fatal("No entry available after putEntrySlice")
		}

		// 验证对象已被重置
		if len(entry.Key) != 0 || len(entry.Value) != 0 {
			t.Error("Entry not properly reset after putEntrySlice")
		}

		putEntry(entry)
	})
}

// TestRequestObjectPool 测试request对象池功能
func TestRequestObjectPool(t *testing.T) {
	// 获取request对象
	req := requestPool.Get().(*request)
	if req == nil {
		t.Fatal("requestPool.Get() returned nil")
	}

	// 添加一些Entry对象
	for i := 0; i < 3; i++ {
		entry := getEntry()
		entry.Key = []byte("req_key" + string(rune('0'+i)))
		entry.Value = []byte("req_value" + string(rune('0'+i)))
		req.Entries = append(req.Entries, entry)
	}

	if len(req.Entries) != 3 {
		t.Errorf("Expected 3 entries in request, got %d", len(req.Entries))
	}

	// 重置request对象（这会释放所有Entry对象）
	req.reset()

	// 验证Entries切片已清空
	if len(req.Entries) != 0 {
		t.Errorf("Expected empty entries after reset, got %d", len(req.Entries))
	}

	// 归还request对象
	requestPool.Put(req)

	// 验证可以再次获取
	req2 := requestPool.Get().(*request)
	if req2 == nil {
		t.Fatal("requestPool.Get() returned nil after put")
	}

	requestPool.Put(req2)
}
