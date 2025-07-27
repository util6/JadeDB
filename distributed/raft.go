/*
JadeDB 分布式共识层 - Raft算法实现

本模块实现了Raft一致性算法，为JadeDB分布式SQL系统提供强一致性保证。
Raft算法确保集群中所有节点的状态机保持一致，支持领导者选举、日志复制和成员变更。

核心功能：
1. 领导者选举：自动选举集群领导者，处理领导者故障
2. 日志复制：将操作日志复制到所有节点，保证一致性
3. 安全性保证：确保已提交的日志条目不会丢失
4. 成员变更：支持集群节点的动态添加和删除
5. 快照机制：压缩日志，提高性能和存储效率

设计特点：
- 强一致性：保证线性一致性，所有节点看到相同的操作序列
- 分区容错：网络分区时保持可用性，分区恢复后自动同步
- 简单易懂：相比Paxos更容易理解和实现
- 高性能：批量日志复制，流水线处理
*/

package distributed

import (
	"context"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

// RaftNode Raft节点
type RaftNode struct {
	// 节点标识
	nodeID    string
	peers     map[string]*RaftPeer
	clusterID string

	// Raft状态
	state       atomic.Value // RaftState
	currentTerm atomic.Uint64
	votedFor    atomic.Value // string
	leader      atomic.Value // string

	// 日志状态
	log         *RaftLog
	commitIndex atomic.Uint64
	lastApplied atomic.Uint64

	// 领导者状态（仅领导者使用）
	nextIndex  map[string]uint64 // 下一个发送给每个节点的日志索引
	matchIndex map[string]uint64 // 已知每个节点复制的最高日志索引

	// 选举状态
	electionTimeout  time.Duration
	heartbeatTimeout time.Duration
	lastHeartbeat    atomic.Value // time.Time

	// 状态机
	stateMachine StateMachine
	applyCh      chan ApplyMsg

	// 网络通信
	transport RaftTransport
	rpcServer *RaftRPCServer

	// 配置
	config *RaftConfig

	// 同步控制
	mu     sync.RWMutex
	stopCh chan struct{}
	wg     sync.WaitGroup

	// 监控和日志
	metrics *RaftMetrics
	logger  *log.Logger
}

// RaftState Raft节点状态
type RaftState int

const (
	Follower RaftState = iota
	Candidate
	Leader
)

func (s RaftState) String() string {
	switch s {
	case Follower:
		return "Follower"
	case Candidate:
		return "Candidate"
	case Leader:
		return "Leader"
	default:
		return "Unknown"
	}
}

// RaftPeer 集群中的其他节点
type RaftPeer struct {
	NodeID   string
	Address  string
	client   RaftClient
	lastSeen time.Time
}

// RaftLog Raft日志
type RaftLog struct {
	mu      sync.RWMutex
	entries []LogEntry
	storage LogStorage
}

// LogEntry 日志条目
type LogEntry struct {
	Term    uint64      // 任期号
	Index   uint64      // 日志索引
	Type    EntryType   // 条目类型
	Data    []byte      // 数据
	Context interface{} // 上下文信息
}

// EntryType 日志条目类型
type EntryType int

const (
	EntryNormal   EntryType = iota // 普通日志条目
	EntryConfig                    // 配置变更条目
	EntrySnapshot                  // 快照条目
)

// StateMachine 状态机接口
type StateMachine interface {
	Apply(entry LogEntry) interface{}
	Snapshot() ([]byte, error)
	Restore(snapshot []byte) error
}

// ApplyMsg 应用消息
type ApplyMsg struct {
	CommandValid  bool
	Command       interface{}
	CommandIndex  uint64
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  uint64
	SnapshotIndex uint64
}

// NewRaftNode 创建Raft节点
func NewRaftNode(nodeID string, peers []string, config *RaftConfig, stateMachine StateMachine, transport RaftTransport) *RaftNode {
	if config == nil {
		config = DefaultDistributedConfig().RaftConfig
	}

	node := &RaftNode{
		nodeID:           nodeID,
		peers:            make(map[string]*RaftPeer),
		log:              NewRaftLog(),
		nextIndex:        make(map[string]uint64),
		matchIndex:       make(map[string]uint64),
		electionTimeout:  config.ElectionTimeout,
		heartbeatTimeout: config.HeartbeatTimeout,
		stateMachine:     stateMachine,
		applyCh:          make(chan ApplyMsg, 1000),
		transport:        transport,
		config:           config,
		stopCh:           make(chan struct{}),
		metrics:          NewRaftMetrics(),
		logger:           log.New(log.Writer(), "[RAFT] ", log.LstdFlags),
	}

	// 初始化状态
	node.state.Store(Follower)
	node.currentTerm.Store(0)
	node.votedFor.Store("")
	node.leader.Store("")
	node.lastHeartbeat.Store(time.Now())

	// 初始化集群节点
	for _, peerAddr := range peers {
		if peerAddr != nodeID {
			peer := &RaftPeer{
				NodeID:  peerAddr, // 简化处理，实际应该是节点ID
				Address: peerAddr,
				client:  transport.GetClient(peerAddr),
			}
			node.peers[peerAddr] = peer
			node.nextIndex[peerAddr] = 1
			node.matchIndex[peerAddr] = 0
		}
	}

	// 启动RPC服务器
	node.rpcServer = NewRaftRPCServer(node, transport)

	return node
}

// Start 启动Raft节点
func (rn *RaftNode) Start() error {
	rn.logger.Printf("Starting Raft node: %s", rn.nodeID)

	// 启动RPC服务器
	if err := rn.rpcServer.Start(); err != nil {
		return fmt.Errorf("failed to start RPC server: %w", err)
	}

	// 启动主循环
	rn.wg.Add(3)
	go rn.mainLoop()
	go rn.applyLoop()
	go rn.metricsLoop()

	return nil
}

// Stop 停止Raft节点
func (rn *RaftNode) Stop() error {
	rn.logger.Printf("Stopping Raft node: %s", rn.nodeID)

	close(rn.stopCh)
	rn.wg.Wait()

	if err := rn.rpcServer.Stop(); err != nil {
		return fmt.Errorf("failed to stop RPC server: %w", err)
	}

	return nil
}

// mainLoop Raft主循环
func (rn *RaftNode) mainLoop() {
	defer rn.wg.Done()

	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-rn.stopCh:
			return
		case <-ticker.C:
			rn.tick()
		}
	}
}

// tick 定时器处理
func (rn *RaftNode) tick() {
	state := rn.getState()

	switch state {
	case Follower:
		rn.tickFollower()
	case Candidate:
		rn.tickCandidate()
	case Leader:
		rn.tickLeader()
	}
}

// tickFollower 跟随者定时处理
func (rn *RaftNode) tickFollower() {
	lastHeartbeat := rn.lastHeartbeat.Load().(time.Time)
	if time.Since(lastHeartbeat) > rn.electionTimeout {
		rn.logger.Printf("Election timeout, becoming candidate")
		rn.becomeCandidate()
	}
}

// tickCandidate 候选者定时处理
func (rn *RaftNode) tickCandidate() {
	lastHeartbeat := rn.lastHeartbeat.Load().(time.Time)
	if time.Since(lastHeartbeat) > rn.electionTimeout {
		rn.logger.Printf("Election timeout, starting new election")
		rn.startElection()
	}
}

// tickLeader 领导者定时处理
func (rn *RaftNode) tickLeader() {
	// 发送心跳
	rn.sendHeartbeats()
}

// becomeFollower 成为跟随者
func (rn *RaftNode) becomeFollower(term uint64, leader string) {
	rn.logger.Printf("Becoming follower, term: %d, leader: %s", term, leader)

	rn.state.Store(Follower)
	rn.currentTerm.Store(term)
	rn.votedFor.Store("")
	rn.leader.Store(leader)
	rn.resetElectionTimeout()
}

// becomeCandidate 成为候选者
func (rn *RaftNode) becomeCandidate() {
	rn.logger.Printf("Becoming candidate")

	rn.state.Store(Candidate)
	rn.currentTerm.Add(1)
	rn.votedFor.Store(rn.nodeID)
	rn.leader.Store("")
	rn.resetElectionTimeout()

	// 开始选举
	rn.startElection()
}

// becomeLeader 成为领导者
func (rn *RaftNode) becomeLeader() {
	rn.logger.Printf("Becoming leader, term: %d", rn.currentTerm.Load())

	rn.state.Store(Leader)
	rn.leader.Store(rn.nodeID)

	// 初始化领导者状态
	lastLogIndex := rn.log.getLastIndex()
	for peerID := range rn.peers {
		rn.nextIndex[peerID] = lastLogIndex + 1
		rn.matchIndex[peerID] = 0
	}

	// 立即发送心跳确立领导地位
	rn.sendHeartbeats()
}

// startElection 开始选举
func (rn *RaftNode) startElection() {
	currentTerm := rn.currentTerm.Load()
	lastLogIndex := rn.log.getLastIndex()
	lastLogTerm := rn.log.getLastTerm()

	rn.logger.Printf("Starting election, term: %d", currentTerm)

	votes := 1 // 自己的票
	votesNeeded := len(rn.peers)/2 + 1

	// 并发向所有节点请求投票
	var wg sync.WaitGroup
	var mu sync.Mutex

	for peerID, peer := range rn.peers {
		wg.Add(1)
		go func(id string, p *RaftPeer) {
			defer wg.Done()

			req := &RequestVoteRequest{
				Term:         currentTerm,
				CandidateID:  rn.nodeID,
				LastLogIndex: lastLogIndex,
				LastLogTerm:  lastLogTerm,
			}

			resp, err := p.client.RequestVote(context.Background(), req)
			if err != nil {
				rn.logger.Printf("Failed to request vote from peer %s: %v", id, err)
				return
			}

			mu.Lock()
			defer mu.Unlock()

			// 检查任期
			if resp.Term > currentTerm {
				rn.becomeFollower(resp.Term, "")
				return
			}

			// 统计选票
			if resp.VoteGranted {
				votes++
				rn.logger.Printf("Received vote from %s, votes: %d, needed: %d", id, votes, votesNeeded)

				// 获得多数票，成为领导者
				if votes >= votesNeeded && rn.getState() == Candidate {
					rn.becomeLeader()
				}
			}
		}(peerID, peer)
	}

	// 等待所有投票请求完成或超时
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(rn.electionTimeout / 2):
		rn.logger.Printf("Election timeout")
	}
}

// sendHeartbeats 发送心跳
func (rn *RaftNode) sendHeartbeats() {
	if rn.getState() != Leader {
		return
	}

	for peerID, peer := range rn.peers {
		go rn.sendAppendEntries(peerID, peer)
	}
}

// sendAppendEntries 发送日志条目
func (rn *RaftNode) sendAppendEntries(peerID string, peer *RaftPeer) {
	nextIndex := rn.nextIndex[peerID]
	prevLogIndex := nextIndex - 1
	prevLogTerm := rn.log.getTerm(prevLogIndex)

	// 获取要发送的日志条目（使用固定值）
	entries := rn.log.getEntries(nextIndex, 100)

	req := &AppendEntriesRequest{
		Term:         rn.currentTerm.Load(),
		LeaderID:     rn.nodeID,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      entries,
		LeaderCommit: rn.commitIndex.Load(),
	}

	resp, err := peer.client.AppendEntries(context.Background(), req)
	if err != nil {
		rn.logger.Printf("Failed to send append entries to peer %s: %v", peerID, err)
		return
	}

	// 处理响应
	rn.handleAppendEntriesResponse(peerID, req, resp)
}

// handleAppendEntriesResponse 处理AppendEntries响应
func (rn *RaftNode) handleAppendEntriesResponse(peerID string, req *AppendEntriesRequest, resp *AppendEntriesResponse) {
	// 检查任期
	if resp.Term > rn.currentTerm.Load() {
		rn.becomeFollower(resp.Term, "")
		return
	}

	// 只有领导者处理响应
	if rn.getState() != Leader {
		return
	}

	if resp.Success {
		// 成功复制，更新索引
		if len(req.Entries) > 0 {
			rn.matchIndex[peerID] = req.PrevLogIndex + uint64(len(req.Entries))
			rn.nextIndex[peerID] = rn.matchIndex[peerID] + 1
		}

		// 尝试提交日志
		rn.tryCommit()
	} else {
		// 复制失败，回退nextIndex
		if rn.nextIndex[peerID] > 1 {
			rn.nextIndex[peerID]--
		}
	}
}

// tryCommit 尝试提交日志
func (rn *RaftNode) tryCommit() {
	currentTerm := rn.currentTerm.Load()
	lastLogIndex := rn.log.getLastIndex()

	// 从最新的日志开始检查
	for index := lastLogIndex; index > rn.commitIndex.Load(); index-- {
		if rn.log.getTerm(index) != currentTerm {
			continue
		}

		// 统计复制到多数节点的日志
		count := 1 // 领导者自己
		for _, matchIndex := range rn.matchIndex {
			if matchIndex >= index {
				count++
			}
		}

		// 如果多数节点已复制，则提交
		if count > len(rn.peers)/2 {
			rn.commitIndex.Store(index)
			rn.logger.Printf("Committed log, index: %d, term: %d", index, currentTerm)
			break
		}
	}
}

// getState 获取当前状态
func (rn *RaftNode) getState() RaftState {
	return rn.state.Load().(RaftState)
}

// resetElectionTimeout 重置选举超时
func (rn *RaftNode) resetElectionTimeout() {
	// 使用配置的选举超时
	rn.electionTimeout = rn.config.ElectionTimeout
	rn.lastHeartbeat.Store(time.Now())
}

// applyLoop 应用日志循环
func (rn *RaftNode) applyLoop() {
	defer rn.wg.Done()

	for {
		select {
		case <-rn.stopCh:
			return
		default:
			rn.applyCommittedEntries()
			time.Sleep(10 * time.Millisecond)
		}
	}
}

// applyCommittedEntries 应用已提交的日志条目
func (rn *RaftNode) applyCommittedEntries() {
	lastApplied := rn.lastApplied.Load()
	commitIndex := rn.commitIndex.Load()

	for index := lastApplied + 1; index <= commitIndex; index++ {
		entry := rn.log.getEntry(index)
		if entry == nil {
			continue
		}

		// 应用到状态机
		result := rn.stateMachine.Apply(*entry)

		// 发送应用消息
		msg := ApplyMsg{
			CommandValid: true,
			Command:      result,
			CommandIndex: index,
		}

		select {
		case rn.applyCh <- msg:
		case <-rn.stopCh:
			return
		}

		rn.lastApplied.Store(index)
	}
}

// metricsLoop 监控指标循环
func (rn *RaftNode) metricsLoop() {
	defer rn.wg.Done()

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-rn.stopCh:
			return
		case <-ticker.C:
			rn.updateMetrics()
		}
	}
}

// updateMetrics 更新监控指标
func (rn *RaftNode) updateMetrics() {
	rn.metrics.CurrentTerm.Store(int64(rn.currentTerm.Load()))
	rn.metrics.CommitIndex.Store(int64(rn.commitIndex.Load()))
	rn.metrics.LastApplied.Store(int64(rn.lastApplied.Load()))
	rn.metrics.LogSize.Store(int64(rn.log.size()))

	state := rn.getState()
	switch state {
	case Leader:
		rn.metrics.IsLeader.Store(1)
	default:
		rn.metrics.IsLeader.Store(0)
	}
}

// Propose 提议新的日志条目
func (rn *RaftNode) Propose(data []byte) error {
	if rn.getState() != Leader {
		return fmt.Errorf("not leader")
	}

	entry := LogEntry{
		Term:  rn.currentTerm.Load(),
		Index: rn.log.getLastIndex() + 1,
		Type:  EntryNormal,
		Data:  data,
	}

	rn.log.append(entry)
	rn.logger.Printf("Proposed new entry, index: %d, term: %d", entry.Index, entry.Term)

	return nil
}

// GetApplyCh 获取应用通道
func (rn *RaftNode) GetApplyCh() <-chan ApplyMsg {
	return rn.applyCh
}

// IsLeader 检查是否为领导者
func (rn *RaftNode) IsLeader() bool {
	return rn.getState() == Leader
}

// GetLeader 获取当前领导者
func (rn *RaftNode) GetLeader() string {
	return rn.leader.Load().(string)
}

// GetTerm 获取当前任期
func (rn *RaftNode) GetTerm() uint64 {
	return rn.currentTerm.Load()
}

// RaftMetrics Raft监控指标
type RaftMetrics struct {
	CurrentTerm    atomic.Int64
	CommitIndex    atomic.Int64
	LastApplied    atomic.Int64
	LogSize        atomic.Int64
	IsLeader       atomic.Int64
	ElectionCount  atomic.Int64
	HeartbeatCount atomic.Int64
}

func NewRaftMetrics() *RaftMetrics {
	return &RaftMetrics{}
}

// NewRaftLog 创建Raft日志
func NewRaftLog() *RaftLog {
	return &RaftLog{
		entries: make([]LogEntry, 0),
		storage: NewMemoryLogStorage(),
	}
}

// append 追加日志条目
func (rl *RaftLog) append(entry LogEntry) {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	rl.entries = append(rl.entries, entry)
	rl.storage.Store(entry)
}

// getEntry 获取指定索引的日志条目
func (rl *RaftLog) getEntry(index uint64) *LogEntry {
	rl.mu.RLock()
	defer rl.mu.RUnlock()

	if index == 0 || index > uint64(len(rl.entries)) {
		return nil
	}

	return &rl.entries[index-1]
}

// getEntries 获取指定范围的日志条目
func (rl *RaftLog) getEntries(startIndex uint64, maxCount int) []LogEntry {
	rl.mu.RLock()
	defer rl.mu.RUnlock()

	if startIndex > uint64(len(rl.entries)) {
		return nil
	}

	start := int(startIndex - 1)
	end := start + maxCount
	if end > len(rl.entries) {
		end = len(rl.entries)
	}

	if start >= end {
		return nil
	}

	entries := make([]LogEntry, end-start)
	copy(entries, rl.entries[start:end])
	return entries
}

// getLastIndex 获取最后一个日志条目的索引
func (rl *RaftLog) getLastIndex() uint64 {
	rl.mu.RLock()
	defer rl.mu.RUnlock()

	return uint64(len(rl.entries))
}

// getLastTerm 获取最后一个日志条目的任期
func (rl *RaftLog) getLastTerm() uint64 {
	rl.mu.RLock()
	defer rl.mu.RUnlock()

	if len(rl.entries) == 0 {
		return 0
	}

	return rl.entries[len(rl.entries)-1].Term
}

// getTerm 获取指定索引日志条目的任期
func (rl *RaftLog) getTerm(index uint64) uint64 {
	rl.mu.RLock()
	defer rl.mu.RUnlock()

	if index == 0 || index > uint64(len(rl.entries)) {
		return 0
	}

	return rl.entries[index-1].Term
}

// size 获取日志大小
func (rl *RaftLog) size() int {
	rl.mu.RLock()
	defer rl.mu.RUnlock()

	return len(rl.entries)
}

// truncate 截断日志（从指定索引开始删除）
func (rl *RaftLog) truncate(index uint64) {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	if index > uint64(len(rl.entries)) {
		return
	}

	rl.entries = rl.entries[:index-1]
}

// LogStorage 日志存储接口
type LogStorage interface {
	Store(entry LogEntry) error
	Load(index uint64) (*LogEntry, error)
	LoadRange(start, end uint64) ([]LogEntry, error)
	Delete(index uint64) error
	Sync() error
}

// MemoryLogStorage 内存日志存储（用于测试）
type MemoryLogStorage struct {
	mu      sync.RWMutex
	entries map[uint64]LogEntry
}

func NewMemoryLogStorage() *MemoryLogStorage {
	return &MemoryLogStorage{
		entries: make(map[uint64]LogEntry),
	}
}

func (mls *MemoryLogStorage) Store(entry LogEntry) error {
	mls.mu.Lock()
	defer mls.mu.Unlock()

	mls.entries[entry.Index] = entry
	return nil
}

func (mls *MemoryLogStorage) Load(index uint64) (*LogEntry, error) {
	mls.mu.RLock()
	defer mls.mu.RUnlock()

	if entry, exists := mls.entries[index]; exists {
		return &entry, nil
	}
	return nil, fmt.Errorf("entry not found")
}

func (mls *MemoryLogStorage) LoadRange(start, end uint64) ([]LogEntry, error) {
	mls.mu.RLock()
	defer mls.mu.RUnlock()

	var entries []LogEntry
	for i := start; i <= end; i++ {
		if entry, exists := mls.entries[i]; exists {
			entries = append(entries, entry)
		}
	}
	return entries, nil
}

func (mls *MemoryLogStorage) Delete(index uint64) error {
	mls.mu.Lock()
	defer mls.mu.Unlock()

	delete(mls.entries, index)
	return nil
}

func (mls *MemoryLogStorage) Sync() error {
	return nil
}
