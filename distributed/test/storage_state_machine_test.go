package test

import (
	"testing"

	"github.com/util6/JadeDB/distributed/raft"
)

// TestStorageEngineStateMachine 测试存储引擎状态机
func TestStorageEngineStateMachine(t *testing.T) {
	// 创建模拟存储引擎
	storage := NewMockStorageEngine()
	defer storage.Close()

	// 创建存储状态机
	stateMachine := raft.NewStorageEngineStateMachine(storage, nil)

	// 测试基本操作
	entry := raft.LogEntry{
		Index: 1,
		Term:  1,
		Data:  []byte(`{"type":"put","key":"test_key","value":"test_value"}`),
	}

	result := stateMachine.Apply(entry)
	if result == nil {
		t.Errorf("Expected non-nil result from Apply")
	}

	t.Logf("Storage state machine test passed")
}

// TestStorageEngineStateMachineSnapshot 测试状态机快照
func TestStorageEngineStateMachineSnapshot(t *testing.T) {
	// 创建模拟存储引擎
	storage := NewMockStorageEngine()
	defer storage.Close()

	// 创建存储状态机
	stateMachine := raft.NewStorageEngineStateMachine(storage, nil)

	// 添加一些数据
	storage.Put([]byte("key1"), []byte("value1"))
	storage.Put([]byte("key2"), []byte("value2"))

	// 创建快照
	snapshot, err := stateMachine.Snapshot()
	if err != nil {
		t.Fatalf("Failed to create snapshot: %v", err)
	}

	if len(snapshot) == 0 {
		t.Errorf("Expected non-empty snapshot")
	}

	// 创建新的存储引擎和状态机
	newStorage := NewMockStorageEngine()
	defer newStorage.Close()
	newStateMachine := raft.NewStorageEngineStateMachine(newStorage, nil)

	// 恢复快照
	err = newStateMachine.Restore(snapshot)
	if err != nil {
		t.Fatalf("Failed to restore snapshot: %v", err)
	}

	// 验证数据
	value, err := newStorage.Get([]byte("key1"))
	if err != nil {
		t.Fatalf("Failed to get key1: %v", err)
	}
	if string(value) != "value1" {
		t.Errorf("Expected value1, got %s", string(value))
	}

	t.Logf("Storage state machine snapshot test passed")
}
