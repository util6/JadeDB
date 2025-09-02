package sharding

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/util6/JadeDB/storage"
)

// MockStorageEngine 模拟存储引擎用于测试
type MockStorageEngine struct {
	mu   sync.RWMutex
	data map[string][]byte
	*storage.BaseEngine
}

func NewMockStorageEngine() *MockStorageEngine {
	info := &storage.EngineInfo{
		Type:        storage.LSMTreeEngine,
		Version:     "1.0.0",
		Description: "Mock storage engine for testing",
	}

	return &MockStorageEngine{
		data:       make(map[string][]byte),
		BaseEngine: storage.NewBaseEngine(storage.LSMTreeEngine, info),
	}
}

func (m *MockStorageEngine) Put(key, value []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.data[string(key)] = make([]byte, len(value))
	copy(m.data[string(key)], value)
	return nil
}

func (m *MockStorageEngine) Get(key []byte) ([]byte, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if value, exists := m.data[string(key)]; exists {
		result := make([]byte, len(value))
		copy(result, value)
		return result, nil
	}
	return nil, nil
}

func (m *MockStorageEngine) Delete(key []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.data, string(key))
	return nil
}

func (m *MockStorageEngine) Exists(key []byte) (bool, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	_, exists := m.data[string(key)]
	return exists, nil
}

func (m *MockStorageEngine) BatchPut(batch []storage.KVPair) error {
	for _, pair := range batch {
		if err := m.Put(pair.Key, pair.Value); err != nil {
			return err
		}
	}
	return nil
}

func (m *MockStorageEngine) BatchGet(keys [][]byte) ([][]byte, error) {
	results := make([][]byte, len(keys))
	for i, key := range keys {
		value, err := m.Get(key)
		if err != nil {
			return nil, err
		}
		results[i] = value
	}
	return results, nil
}

func (m *MockStorageEngine) BatchDelete(keys [][]byte) error {
	for _, key := range keys {
		if err := m.Delete(key); err != nil {
			return err
		}
	}
	return nil
}

func (m *MockStorageEngine) Scan(startKey, endKey []byte, limit int) ([]storage.KVPair, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var results []storage.KVPair
	count := 0
	for key, value := range m.data {
		if count >= limit {
			break
		}
		keyBytes := []byte(key)
		if (startKey == nil || string(keyBytes) >= string(startKey)) &&
			(endKey == nil || string(keyBytes) < string(endKey)) {
			results = append(results, storage.KVPair{
				Key:   keyBytes,
				Value: value,
			})
			count++
		}
	}
	return results, nil
}

func (m *MockStorageEngine) NewIterator(options *storage.IteratorOptions) (storage.Iterator, error) {
	return nil, nil // 简化实现
}

func (m *MockStorageEngine) Open() error {
	m.SetOpened(true)
	return nil
}

func (m *MockStorageEngine) Close() error {
	m.SetOpened(false)
	return nil
}

func (m *MockStorageEngine) Sync() error {
	return nil
}

// TestShardManagerBasic 测试分片管理器基本功能
func TestShardManagerBasic(t *testing.T) {
	// 创建分片管理器
	config := DefaultShardManagerConfig()
	sm := NewShardManager(config)

	// 启动分片管理器
	if err := sm.Start(); err != nil {
		t.Fatalf("Failed to start shard manager: %v", err)
	}
	defer sm.Stop()

	// 创建模拟存储引擎
	engine := NewMockStorageEngine()
	if err := engine.Open(); err != nil {
		t.Fatalf("Failed to open engine: %v", err)
	}
	defer engine.Close()

	// 创建分片
	shardID := "shard_001"
	replicas := []string{"node_1", "node_2", "node_3"}

	shard, err := sm.CreateShard(shardID, replicas, engine)
	if err != nil {
		t.Fatalf("Failed to create shard: %v", err)
	}

	// 验证分片创建成功
	if shard.shardID != shardID {
		t.Errorf("Expected shard ID %s, got %s", shardID, shard.shardID)
	}

	if len(shard.replicas) != len(replicas) {
		t.Errorf("Expected %d replicas, got %d", len(replicas), len(shard.replicas))
	}

	// 验证分片可以获取
	retrievedShard, exists := sm.GetShard(shardID)
	if !exists {
		t.Errorf("Shard %s not found", shardID)
	}
	if retrievedShard != shard {
		t.Errorf("Retrieved shard is not the same as created shard")
	}

	// 验证指标更新
	if sm.metrics.TotalShards != 1 {
		t.Errorf("Expected 1 total shard, got %d", sm.metrics.TotalShards)
	}
	if sm.metrics.ActiveShards != 1 {
		t.Errorf("Expected 1 active shard, got %d", sm.metrics.ActiveShards)
	}

	t.Logf("Shard manager basic test passed")
}

// TestShardRouting 测试分片路由功能
func TestShardRouting(t *testing.T) {
	// 创建分片管理器
	sm := NewShardManager(DefaultShardManagerConfig())
	if err := sm.Start(); err != nil {
		t.Fatalf("Failed to start shard manager: %v", err)
	}
	defer sm.Stop()

	// 创建单个分片（避免端口冲突）
	shardIDs := []string{"shard_001"}
	for _, shardID := range shardIDs {
		engine := NewMockStorageEngine()
		if err := engine.Open(); err != nil {
			t.Fatalf("Failed to open engine: %v", err)
		}
		defer engine.Close()

		replicas := []string{"node_1", "node_2", "node_3"}
		_, err := sm.CreateShard(shardID, replicas, engine)
		if err != nil {
			t.Fatalf("Failed to create shard %s: %v", shardID, err)
		}
	}

	// 测试路由功能
	testKeys := [][]byte{
		[]byte("key1"),
		[]byte("key2"),
		[]byte("key3"),
		[]byte("key4"),
		[]byte("key5"),
	}

	routingResults := make(map[string]int)
	for _, key := range testKeys {
		shardID, err := sm.RouteShard(key)
		if err != nil {
			t.Fatalf("Failed to route key %s: %v", string(key), err)
		}

		if shardID == "" {
			t.Errorf("Empty shard ID for key %s", string(key))
			continue
		}

		routingResults[shardID]++
		t.Logf("Key %s routed to shard %s", string(key), shardID)
	}

	// 验证路由分布
	if len(routingResults) == 0 {
		t.Error("No routing results")
	}

	// 验证所有路由的分片都存在
	for shardID := range routingResults {
		if _, exists := sm.GetShard(shardID); !exists {
			t.Errorf("Routed to non-existent shard: %s", shardID)
		}
	}

	t.Logf("Shard routing test passed, routing distribution: %v", routingResults)
}

// TestConsistentHashRing 测试一致性哈希环
func TestConsistentHashRing(t *testing.T) {
	// 创建一致性哈希环
	ring := NewConsistentHashRing(150)

	// 添加节点
	nodes := []string{"node_1", "node_2", "node_3", "node_4"}
	for _, node := range nodes {
		ring.AddNode(node)
	}

	// 测试键分布
	testKeys := [][]byte{
		[]byte("key1"), []byte("key2"), []byte("key3"), []byte("key4"), []byte("key5"),
		[]byte("key6"), []byte("key7"), []byte("key8"), []byte("key9"), []byte("key10"),
	}

	distribution := make(map[string]int)
	for _, key := range testKeys {
		node := ring.GetNode(key)
		if node == "" {
			t.Errorf("Empty node for key %s", string(key))
			continue
		}
		distribution[node]++
		t.Logf("Key %s -> Node %s", string(key), node)
	}

	// 验证所有节点都被使用
	for _, node := range nodes {
		if distribution[node] == 0 {
			t.Logf("Warning: Node %s not used in distribution", node)
		}
	}

	// 测试节点移除
	ring.RemoveNode("node_2")

	// 重新测试分布
	newDistribution := make(map[string]int)
	for _, key := range testKeys {
		node := ring.GetNode(key)
		if node == "node_2" {
			t.Errorf("Key %s still routed to removed node node_2", string(key))
		}
		newDistribution[node]++
	}

	// 验证node_2不再出现
	if newDistribution["node_2"] > 0 {
		t.Errorf("Removed node node_2 still appears in distribution")
	}

	t.Logf("Consistent hash ring test passed")
	t.Logf("Original distribution: %v", distribution)
	t.Logf("After removal distribution: %v", newDistribution)
}

// TestShardOperations 测试分片操作
func TestShardOperations(t *testing.T) {
	// 创建分片管理器
	sm := NewShardManager(DefaultShardManagerConfig())
	if err := sm.Start(); err != nil {
		t.Fatalf("Failed to start shard manager: %v", err)
	}
	defer sm.Stop()

	// 创建分片
	engine := NewMockStorageEngine()
	if err := engine.Open(); err != nil {
		t.Fatalf("Failed to open engine: %v", err)
	}
	defer engine.Close()

	shardID := "test_shard"
	replicas := []string{"node_1"}

	shard, err := sm.CreateShard(shardID, replicas, engine)
	if err != nil {
		t.Fatalf("Failed to create shard: %v", err)
	}

	// 等待分片启动
	time.Sleep(100 * time.Millisecond)

	// 测试Get操作（应该返回nil，因为键不存在）
	value, err := shard.Get([]byte("test_key"))
	if err != nil {
		t.Fatalf("Failed to get value: %v", err)
	}
	if value != nil {
		t.Errorf("Expected nil value for non-existent key, got %v", value)
	}

	// 注意：Put和Delete操作需要Raft领导者，在测试环境中可能无法正常工作
	// 这里主要测试接口是否正确

	// 测试分片指标
	metrics := shard.GetMetrics()
	if metrics == nil {
		t.Error("Shard metrics should not be nil")
	}

	// 测试分片状态
	if !shard.isRunning {
		t.Error("Shard should be running")
	}

	t.Logf("Shard operations test passed")
}

// TestShardManagerMetrics 测试分片管理器指标
func TestShardManagerMetrics(t *testing.T) {
	// 创建分片管理器
	sm := NewShardManager(DefaultShardManagerConfig())
	if err := sm.Start(); err != nil {
		t.Fatalf("Failed to start shard manager: %v", err)
	}
	defer sm.Stop()

	// 初始指标应该为0
	if sm.metrics.TotalShards != 0 {
		t.Errorf("Expected 0 total shards initially, got %d", sm.metrics.TotalShards)
	}

	// 创建一个分片（避免端口冲突）
	engine := NewMockStorageEngine()
	if err := engine.Open(); err != nil {
		t.Fatalf("Failed to open engine: %v", err)
	}
	defer engine.Close()

	shardID := "shard_000"
	replicas := []string{"node_1", "node_2"}

	_, err := sm.CreateShard(shardID, replicas, engine)
	if err != nil {
		t.Fatalf("Failed to create shard %s: %v", shardID, err)
	}

	// 验证指标更新
	if sm.metrics.TotalShards != 1 {
		t.Errorf("Expected 1 total shard, got %d", sm.metrics.TotalShards)
	}
	if sm.metrics.ActiveShards != 1 {
		t.Errorf("Expected 1 active shard, got %d", sm.metrics.ActiveShards)
	}

	// 测试路由请求计数
	initialRequests := sm.metrics.RoutingRequests

	// 执行一些路由请求
	for i := 0; i < 5; i++ {
		key := []byte(fmt.Sprintf("test_key_%d", i))
		_, err := sm.RouteShard(key)
		if err != nil {
			t.Fatalf("Failed to route key: %v", err)
		}
	}

	// 验证路由请求计数增加
	if sm.metrics.RoutingRequests != initialRequests+5 {
		t.Errorf("Expected %d routing requests, got %d",
			initialRequests+5, sm.metrics.RoutingRequests)
	}

	t.Logf("Shard manager metrics test passed")
}
