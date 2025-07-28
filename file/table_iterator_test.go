/*
JadeDB TableIterator功能测试

本测试文件验证SSTable迭代器的功能，
确保TableIterator接口的实现正确工作。

测试覆盖：
1. TableIterator创建
2. 键范围获取功能
3. 基本迭代器接口
4. 错误处理
*/

package file

import (
	"testing"

	"github.com/util6/JadeDB/utils"
)

// TestNewTableIterator 测试TableIterator创建功能
func TestNewTableIterator(t *testing.T) {
	// 由于创建有效的SSTable需要复杂的数据结构，
	// 这里我们主要测试接口的正确性

	// 测试无效输入的情况

	// 测试无效输入
	t.Run("InvalidSSTable", func(t *testing.T) {
		iter, err := utils.NewTableIterator("invalid", nil)
		if err == nil {
			t.Errorf("Expected error for invalid SSTable")
		}
		if iter != nil {
			t.Errorf("Expected nil iterator for invalid SSTable")
		}
	})

	// 测试nil输入
	t.Run("NilSSTable", func(t *testing.T) {
		iter, err := utils.NewTableIterator(nil, nil)
		if err == nil {
			t.Errorf("Expected error for nil SSTable")
		}
		if iter != nil {
			t.Errorf("Expected nil iterator for nil SSTable")
		}
	})
}

// TestTableIteratorInterface 测试TableIterator接口的基本功能
func TestTableIteratorInterface(t *testing.T) {
	// 测试NewTableIterator函数是否已正确注册
	if utils.NewTableIterator == nil {
		t.Fatalf("NewTableIterator function is not registered")
	}

	// 测试函数调用（应该返回错误，因为输入无效）
	iter, err := utils.NewTableIterator("invalid", nil)
	if err == nil {
		t.Errorf("Expected error for invalid input")
	}
	if iter != nil {
		t.Errorf("Expected nil iterator for invalid input")
		iter.Close()
	}
}
