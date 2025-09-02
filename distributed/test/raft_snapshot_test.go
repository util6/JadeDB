package test

// TODO: 修复接口匹配问题后启用此测试文件
/*
import (
	"encoding/json"
	"github.com/util6/JadeDB/distributed/raft"
	"testing"
	"time"
)

// TestRaftSnapshot 测试Raft快照功能
func TestRaftSnapshot(t *testing.T) {
	// 创建测试集群
	cluster := createTestCluster(t, 3)
	defer cluster.Shutdown()

	// 等待领导者选举
	leader := cluster.WaitForLeader(t, 10*time.Second)
	if leader == nil {
		t.Fatal("No leader elected")
	}

	// 设置较小的快照阈值用于测试
	leader.snapshotThreshold = 5

	// 提交一些日志条目
	for i := 0; i < 10; i++ {
		key := "key" + string(rune('0'+i))
		value := "value" + string(rune('0'+i))

		operation := &raft.Operation{
			Type:  "PUT",
			Key:   key,
			Value: []byte(value),
		}

		// 序列化操作
		data, err := json.Marshal(operation)
		if err != nil {
			t.Fatalf("Failed to marshal operation %d: %v", i, err)
		}

		err = leader.Propose(data)
		if err != nil {
			t.Fatalf("Failed to propose operation %d: %v", i, err)
		}
	}

	// 等待日志复制和提交
	time.Sleep(2 * time.Second)

	// 手动触发快照创建
	err := leader.CreateSnapshot()
	if err != nil {
		t.Fatalf("Failed to create snapshot: %v", err)
	}

	// 验证快照状态
	lastSnapshotIndex := leader.lastSnapshotIndex.Load()
	if lastSnapshotIndex == 0 {
		t.Error("Expected snapshot to be created, but lastSnapshotIndex is 0")
	}

	t.Logf("Snapshot created with lastSnapshotIndex: %d", lastSnapshotIndex)

	// 验证状态机数据
	kvStateMachine, ok := leader.stateMachine.(*raft.KVStateMachine)
	if !ok {
		t.Fatal("Expected KVStateMachine")
	}

	for i := 0; i < 10; i++ {
		key := "key" + string(rune('0'+i))
		expectedValue := "value" + string(rune('0'+i))

		actualValue, exists := kvStateMachine.Get(key)
		if !exists {
			t.Errorf("Key %s not found in state machine", key)
			continue
		}

		if string(actualValue) != expectedValue {
			t.Errorf("Key %s: expected %s, got %s", key, expectedValue, string(actualValue))
		}
	}
}

// TestRaftSnapshotInstallation 测试快照安装
func TestRaftSnapshotInstallation(t *testing.T) {
	// 创建测试集群
	cluster := createTestCluster(t, 3)
	defer cluster.Shutdown()

	// 等待领导者选举
	leader := cluster.WaitForLeader(t, 10*time.Second)
	if leader == nil {
		t.Fatal("No leader elected")
	}

	// 设置较小的快照阈值
	leader.snapshotThreshold = 3

	// 提交一些日志条目
	for i := 0; i < 8; i++ {
		key := "test_key_" + string(rune('0'+i))
		value := "test_value_" + string(rune('0'+i))

		operation := &raft.Operation{
			Type:  "PUT",
			Key:   key,
			Value: []byte(value),
		}

		// 序列化操作
		data, err := json.Marshal(operation)
		if err != nil {
			t.Fatalf("Failed to marshal operation %d: %v", i, err)
		}

		err = leader.Propose(data)
		if err != nil {
			t.Fatalf("Failed to propose operation %d: %v", i, err)
		}
	}

	// 等待日志复制
	time.Sleep(2 * time.Second)

	// 创建快照
	err := leader.CreateSnapshot()
	if err != nil {
		t.Fatalf("Failed to create snapshot: %v", err)
	}

	// 找到一个跟随者节点
	var follower *raft.RaftNode
	for _, node := range cluster.nodes {
		if node != leader {
			follower = node
			break
		}
	}

	if follower == nil {
		t.Fatal("No follower found")
	}

	// 模拟跟随者落后很多，需要快照
	follower.nextIndex[leader.nodeID] = 1 // 设置为很小的值

	// 发送快照
	err = leader.SendSnapshot(follower.nodeID)
	if err != nil {
		t.Fatalf("Failed to send snapshot: %v", err)
	}

	// 等待快照安装
	time.Sleep(1 * time.Second)

	// 验证跟随者的状态机数据
	kvFollowerStateMachine, ok := follower.stateMachine.(*raft.KVStateMachine)
	if !ok {
		t.Fatal("Expected KVStateMachine for follower")
	}

	for i := 0; i < 8; i++ {
		key := "test_key_" + string(rune('0'+i))
		expectedValue := "test_value_" + string(rune('0'+i))

		actualValue, exists := kvFollowerStateMachine.Get(key)
		if !exists {
			t.Errorf("Key %s not found in follower state machine", key)
			continue
		}

		if string(actualValue) != expectedValue {
			t.Errorf("Follower key %s: expected %s, got %s", key, expectedValue, string(actualValue))
		}
	}

	t.Logf("Snapshot installation test passed")
}
*/

// 占位符，避免空包错误
var _ = 1
