package JadeDB

import (
	"testing"

	"github.com/util6/JadeDB/utils"
)

// TestBitDeleteConstant 测试BitDelete常量
func TestBitDeleteConstant(t *testing.T) {
	t.Logf("BitDelete constant value: %d", utils.BitDelete)

	// 测试删除标记的设置和检查
	meta := byte(0)
	meta |= utils.BitDelete

	if meta&utils.BitDelete == 0 {
		t.Errorf("BitDelete not set correctly")
	} else {
		t.Logf("BitDelete set correctly: meta=%d", meta)
	}
}

// TestEntryCreation 测试Entry的创建
func TestEntryCreation(t *testing.T) {
	key := []byte("test_key")
	value := []byte("test_value")

	// 创建普通条目
	entry := &utils.Entry{
		Key:   key,
		Value: value,
		Meta:  0,
	}

	t.Logf("Normal entry: Key=%s, Value=%s, Meta=%d", entry.Key, entry.Value, entry.Meta)

	// 创建删除条目
	deleteEntry := &utils.Entry{
		Key:   key,
		Value: nil,
		Meta:  utils.BitDelete,
	}

	t.Logf("Delete entry: Key=%s, Value=%v, Meta=%d", deleteEntry.Key, deleteEntry.Value, deleteEntry.Meta)

	// 测试IsDeletedOrExpired方法
	if deleteEntry.IsDeletedOrExpired() {
		t.Logf("Delete entry correctly identified as deleted")
	} else {
		t.Errorf("Delete entry not identified as deleted")
	}
}
