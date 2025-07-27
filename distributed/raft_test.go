/*
JadeDB Raft算法测试

本模块包含Raft算法的单元测试和集成测试，验证领导者选举、日志复制、
故障恢复等核心功能的正确性。

测试场景：
1. 基本功能测试：创建节点、启动停止
2. 领导者选举测试：正常选举、网络分区、故障恢复
3. 日志复制测试：正常复制、冲突解决、一致性保证
4. 故障容错测试：节点故障、网络分区、脑裂处理
5. 性能测试：吞吐量、延迟、扩展性

设计原则：
- 确定性测试：使用模拟时间和网络，确保测试结果可重现
- 故障注入：模拟各种故障场景，验证系统的容错能力
- 性能基准：测试系统在不同负载下的性能表现
*/

package distributed

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"
)

// TestStateMachine 测试状态机
type TestStateMachine struct {
	mu    sync.RWMutex
	state map[string]string
}

func NewTestStateMachine() *TestStateMachine {
	return &TestStateMachine{
		state: make(map[string]string),
	}
}

func (tsm *TestStateMachine) Apply(entry LogEntry) interface{} {
	tsm.mu.Lock()
	defer tsm.mu.Unlock()

	// 简单的键值存储操作
	if len(entry.Data) > 0 {
		key := fmt.Sprintf("key_%d", entry.Index)
		value := string(entry.Data)
		tsm.state[key] = value
		return fmt.Sprintf("Applied: %s = %s", key, value)
	}

	return nil
}

func (tsm *TestStateMachine) Snapshot() ([]byte, error) {
	tsm.mu.RLock()
	defer tsm.mu.RUnlock()

	// 简化的快照实现
	return []byte("snapshot"), nil
}

func (tsm *TestStateMachine) Restore(snapshot []byte) error {
	tsm.mu.Lock()
	defer tsm.mu.Unlock()

	// 简化的恢复实现
	tsm.state = make(map[string]string)
	return nil
}

func (tsm *TestStateMachine) Get(key string) string {
	tsm.mu.RLock()
	defer tsm.mu.RUnlock()

	return tsm.state[key]
}

// MockRaftTransport 模拟传输层
type MockRaftTransport struct {
	mu      sync.RWMutex
	clients map[string]*MockRaftClient
	servers map[string]RaftHandler
	network *MockNetwork
}

func NewMockRaftTransport(network *MockNetwork) *MockRaftTransport {
	return &MockRaftTransport{
		clients: make(map[string]*MockRaftClient),
		servers: make(map[string]RaftHandler),
		network: network,
	}
}

func (t *MockRaftTransport) GetClient(address string) RaftClient {
	t.mu.Lock()
	defer t.mu.Unlock()

	if client, exists := t.clients[address]; exists {
		return client
	}

	client := &MockRaftClient{
		address: address,
		network: t.network,
	}
	t.clients[address] = client
	return client
}

func (t *MockRaftTransport) StartServer(address string, handler RaftHandler) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.servers[address] = handler
	return nil
}

func (t *MockRaftTransport) StopServer() error {
	return nil
}

// MockRaftClient 模拟客户端
type MockRaftClient struct {
	address string
	network *MockNetwork
}

func (c *MockRaftClient) RequestVote(ctx context.Context, req *RequestVoteRequest) (*RequestVoteResponse, error) {
	return c.network.SendRequestVote(c.address, req)
}

func (c *MockRaftClient) AppendEntries(ctx context.Context, req *AppendEntriesRequest) (*AppendEntriesResponse, error) {
	return c.network.SendAppendEntries(c.address, req)
}

func (c *MockRaftClient) InstallSnapshot(ctx context.Context, req *InstallSnapshotRequest) (*InstallSnapshotResponse, error) {
	return c.network.SendInstallSnapshot(c.address, req)
}

// MockNetwork 模拟网络
type MockNetwork struct {
	mu        sync.RWMutex
	handlers  map[string]RaftHandler
	partition map[string]bool // 网络分区状态
	delay     time.Duration   // 网络延迟
}

func NewMockNetwork() *MockNetwork {
	return &MockNetwork{
		handlers:  make(map[string]RaftHandler),
		partition: make(map[string]bool),
		delay:     0,
	}
}

func (n *MockNetwork) RegisterHandler(address string, handler RaftHandler) {
	n.mu.Lock()
	defer n.mu.Unlock()

	n.handlers[address] = handler
}

func (n *MockNetwork) SetPartition(address string, partitioned bool) {
	n.mu.Lock()
	defer n.mu.Unlock()

	n.partition[address] = partitioned
}

func (n *MockNetwork) SetDelay(delay time.Duration) {
	n.mu.Lock()
	defer n.mu.Unlock()

	n.delay = delay
}

func (n *MockNetwork) SendRequestVote(address string, req *RequestVoteRequest) (*RequestVoteResponse, error) {
	n.mu.RLock()
	partitioned := n.partition[address]
	delay := n.delay
	handler := n.handlers[address]
	n.mu.RUnlock()

	if partitioned {
		return nil, fmt.Errorf("network partition")
	}

	if delay > 0 {
		time.Sleep(delay)
	}

	if handler == nil {
		return nil, fmt.Errorf("handler not found")
	}

	return handler.HandleRequestVote(req), nil
}

func (n *MockNetwork) SendAppendEntries(address string, req *AppendEntriesRequest) (*AppendEntriesResponse, error) {
	n.mu.RLock()
	partitioned := n.partition[address]
	delay := n.delay
	handler := n.handlers[address]
	n.mu.RUnlock()

	if partitioned {
		return nil, fmt.Errorf("network partition")
	}

	if delay > 0 {
		time.Sleep(delay)
	}

	if handler == nil {
		return nil, fmt.Errorf("handler not found")
	}

	return handler.HandleAppendEntries(req), nil
}

func (n *MockNetwork) SendInstallSnapshot(address string, req *InstallSnapshotRequest) (*InstallSnapshotResponse, error) {
	n.mu.RLock()
	partitioned := n.partition[address]
	delay := n.delay
	handler := n.handlers[address]
	n.mu.RUnlock()

	if partitioned {
		return nil, fmt.Errorf("network partition")
	}

	if delay > 0 {
		time.Sleep(delay)
	}

	if handler == nil {
		return nil, fmt.Errorf("handler not found")
	}

	return handler.HandleInstallSnapshot(req), nil
}

// TestRaftBasicFunctionality 测试Raft基本功能
func TestRaftBasicFunctionality(t *testing.T) {
	// 创建测试集群
	cluster := createTestCluster(t, 3)
	defer cluster.Shutdown()

	// 等待领导者选举
	leader := cluster.WaitForLeader(t, 5*time.Second)
	if leader == nil {
		t.Fatal("Expected leader to be elected")
	}

	// 验证只有一个领导者
	leaders := cluster.GetLeaders()
	if len(leaders) != 1 {
		t.Fatalf("Expected 1 leader, got %d", len(leaders))
	}

	// 提议一些日志条目
	for i := 0; i < 10; i++ {
		data := []byte(fmt.Sprintf("test_data_%d", i))
		err := leader.Propose(data)
		if err != nil {
			t.Fatalf("Failed to propose data: %v", err)
		}
	}

	// 等待日志复制
	cluster.WaitForCommit(t, 10, 5*time.Second)

	// 验证所有节点的状态一致
	cluster.VerifyConsistency(t)
}

// TestRaftLeaderElection 测试领导者选举
func TestRaftLeaderElection(t *testing.T) {
	cluster := createTestCluster(t, 5)
	defer cluster.Shutdown()

	// 等待初始领导者选举
	leader := cluster.WaitForLeader(t, 5*time.Second)
	if leader == nil {
		t.Fatal("Expected leader to be elected")
	}

	// 停止当前领导者
	cluster.StopNode(leader.nodeID)

	// 等待新的领导者选举
	newLeader := cluster.WaitForLeader(t, 5*time.Second)
	if newLeader == nil {
		t.Fatal("Expected new leader to be elected")
	}
	if leader.nodeID == newLeader.nodeID {
		t.Fatal("Expected different leader after failure")
	}

	// 验证新领导者可以正常工作
	err := newLeader.Propose([]byte("test_after_election"))
	if err != nil {
		t.Fatalf("Failed to propose after election: %v", err)
	}

	cluster.WaitForCommit(t, 1, 5*time.Second)
}

// TestRaftLogReplication 测试日志复制
func TestRaftLogReplication(t *testing.T) {
	cluster := createTestCluster(t, 3)
	defer cluster.Shutdown()

	leader := cluster.WaitForLeader(t, 5*time.Second)
	if leader == nil {
		t.Fatal("Expected leader to be elected")
	}

	// 提议大量日志条目
	numEntries := 100
	for i := 0; i < numEntries; i++ {
		data := []byte(fmt.Sprintf("entry_%d", i))
		err := leader.Propose(data)
		if err != nil {
			t.Fatalf("Failed to propose entry %d: %v", i, err)
		}
	}

	// 等待所有条目被提交
	cluster.WaitForCommit(t, uint64(numEntries), 10*time.Second)

	// 验证所有节点的日志一致
	cluster.VerifyLogConsistency(t)
}

// TestRaftNetworkPartition 测试网络分区
func TestRaftNetworkPartition(t *testing.T) {
	cluster := createTestCluster(t, 5)
	defer cluster.Shutdown()

	leader := cluster.WaitForLeader(t, 5*time.Second)
	if leader == nil {
		t.Fatal("Expected leader to be elected")
	}

	// 创建网络分区：隔离领导者
	cluster.network.SetPartition(leader.nodeID, true)

	// 等待新的领导者选举
	newLeader := cluster.WaitForLeader(t, 5*time.Second)
	if newLeader == nil {
		t.Fatal("Expected new leader after partition")
	}
	if leader.nodeID == newLeader.nodeID {
		t.Fatal("Expected different leader after partition")
	}

	// 在分区期间提议日志条目
	err := newLeader.Propose([]byte("partition_test"))
	if err != nil {
		t.Fatalf("Failed to propose during partition: %v", err)
	}

	// 恢复网络分区
	cluster.network.SetPartition(leader.nodeID, false)

	// 等待集群恢复一致性
	time.Sleep(2 * time.Second)
	cluster.VerifyConsistency(t)
}

// TestCluster 测试集群
type TestCluster struct {
	nodes   map[string]*RaftNode
	network *MockNetwork
}

func createTestCluster(t *testing.T, size int) *TestCluster {
	network := NewMockNetwork()
	cluster := &TestCluster{
		nodes:   make(map[string]*RaftNode),
		network: network,
	}

	// 创建节点地址列表
	var peers []string
	for i := 0; i < size; i++ {
		peers = append(peers, fmt.Sprintf("node_%d", i))
	}

	// 创建所有节点
	for i := 0; i < size; i++ {
		nodeID := fmt.Sprintf("node_%d", i)

		config := DefaultDistributedConfig().RaftConfig
		config.ElectionTimeout = 150 * time.Millisecond
		config.HeartbeatTimeout = 50 * time.Millisecond

		stateMachine := NewTestStateMachine()
		transport := NewMockRaftTransport(network)

		node := NewRaftNode(nodeID, peers, config, stateMachine, transport)
		cluster.nodes[nodeID] = node

		// 注册到网络
		network.RegisterHandler(nodeID, node.rpcServer)

		// 启动节点
		err := node.Start()
		if err != nil {
			t.Fatalf("Failed to start node %s: %v", nodeID, err)
		}
	}

	return cluster
}

func (c *TestCluster) Shutdown() {
	for _, node := range c.nodes {
		node.Stop()
	}
}

func (c *TestCluster) WaitForLeader(t *testing.T, timeout time.Duration) *RaftNode {
	deadline := time.Now().Add(timeout)

	for time.Now().Before(deadline) {
		for _, node := range c.nodes {
			if node.IsLeader() {
				return node
			}
		}
		time.Sleep(10 * time.Millisecond)
	}

	return nil
}

func (c *TestCluster) GetLeaders() []*RaftNode {
	var leaders []*RaftNode
	for _, node := range c.nodes {
		if node.IsLeader() {
			leaders = append(leaders, node)
		}
	}
	return leaders
}

func (c *TestCluster) WaitForCommit(t *testing.T, index uint64, timeout time.Duration) {
	deadline := time.Now().Add(timeout)

	for time.Now().Before(deadline) {
		allCommitted := true
		for _, node := range c.nodes {
			if node.commitIndex.Load() < index {
				allCommitted = false
				break
			}
		}

		if allCommitted {
			return
		}

		time.Sleep(10 * time.Millisecond)
	}

	t.Fatalf("Timeout waiting for commit index %d", index)
}

func (c *TestCluster) VerifyConsistency(t *testing.T) {
	// 获取所有节点的提交索引
	var commitIndexes []uint64
	for _, node := range c.nodes {
		commitIndexes = append(commitIndexes, node.commitIndex.Load())
	}

	// 验证提交索引一致
	for i := 1; i < len(commitIndexes); i++ {
		if commitIndexes[0] != commitIndexes[i] {
			t.Fatalf("Commit indexes should be consistent: expected %d, got %d", commitIndexes[0], commitIndexes[i])
		}
	}
}

func (c *TestCluster) VerifyLogConsistency(t *testing.T) {
	// 获取最小提交索引
	minCommitIndex := uint64(^uint64(0))
	for _, node := range c.nodes {
		commitIndex := node.commitIndex.Load()
		if commitIndex < minCommitIndex {
			minCommitIndex = commitIndex
		}
	}

	// 验证已提交的日志条目一致
	var referenceLogs []LogEntry
	var referenceNode *RaftNode

	for _, node := range c.nodes {
		referenceNode = node
		break
	}

	for i := uint64(1); i <= minCommitIndex; i++ {
		entry := referenceNode.log.getEntry(i)
		if entry == nil {
			t.Fatalf("Expected entry at index %d", i)
		}
		referenceLogs = append(referenceLogs, *entry)
	}

	// 与其他节点比较
	for _, node := range c.nodes {
		if node == referenceNode {
			continue
		}

		for i := uint64(1); i <= minCommitIndex; i++ {
			entry := node.log.getEntry(i)
			if entry == nil {
				t.Fatalf("Expected entry at index %d", i)
			}

			refEntry := referenceLogs[i-1]
			if refEntry.Term != entry.Term {
				t.Fatalf("Log term should be consistent: expected %d, got %d", refEntry.Term, entry.Term)
			}
			if refEntry.Index != entry.Index {
				t.Fatalf("Log index should be consistent: expected %d, got %d", refEntry.Index, entry.Index)
			}
			if string(refEntry.Data) != string(entry.Data) {
				t.Fatalf("Log data should be consistent: expected %s, got %s", string(refEntry.Data), string(entry.Data))
			}
		}
	}
}

func (c *TestCluster) StopNode(nodeID string) {
	if node, exists := c.nodes[nodeID]; exists {
		node.Stop()
		delete(c.nodes, nodeID)
	}
}
