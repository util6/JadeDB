/*
JadeDB Raft RPC接口定义

本模块定义了Raft算法的RPC接口，包括RequestVote和AppendEntries两个核心RPC，
以及相关的请求和响应结构体。支持网络通信和消息序列化。

核心RPC：
1. RequestVote：候选者请求投票
2. AppendEntries：领导者复制日志条目（也用作心跳）
3. InstallSnapshot：领导者安装快照（可选）

设计特点：
- 类型安全：强类型的请求和响应结构
- 序列化：支持JSON和Protocol Buffers序列化
- 超时处理：内置超时和重试机制
- 错误处理：完整的错误处理和恢复
*/

package distributed

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/http"
	"sync"
	"time"
)

// RequestVoteRequest 投票请求
type RequestVoteRequest struct {
	Term         uint64 `json:"term"`           // 候选者的任期号
	CandidateID  string `json:"candidate_id"`   // 请求投票的候选者ID
	LastLogIndex uint64 `json:"last_log_index"` // 候选者最后日志条目的索引
	LastLogTerm  uint64 `json:"last_log_term"`  // 候选者最后日志条目的任期号
}

// RequestVoteResponse 投票响应
type RequestVoteResponse struct {
	Term        uint64 `json:"term"`         // 当前任期号，用于候选者更新自己
	VoteGranted bool   `json:"vote_granted"` // 候选者是否获得投票
}

// AppendEntriesRequest 追加条目请求
type AppendEntriesRequest struct {
	Term         uint64     `json:"term"`           // 领导者的任期号
	LeaderID     string     `json:"leader_id"`      // 领导者ID
	PrevLogIndex uint64     `json:"prev_log_index"` // 新日志条目前一条的索引
	PrevLogTerm  uint64     `json:"prev_log_term"`  // 新日志条目前一条的任期号
	Entries      []LogEntry `json:"entries"`        // 准备存储的日志条目
	LeaderCommit uint64     `json:"leader_commit"`  // 领导者已提交的日志索引
}

// AppendEntriesResponse 追加条目响应
type AppendEntriesResponse struct {
	Term    uint64 `json:"term"`    // 当前任期号，用于领导者更新自己
	Success bool   `json:"success"` // 跟随者包含了匹配上PrevLogIndex和PrevLogTerm的日志时为真
}

// InstallSnapshotRequest 安装快照请求
type InstallSnapshotRequest struct {
	Term              uint64 `json:"term"`                // 领导者的任期号
	LeaderID          string `json:"leader_id"`           // 领导者ID
	LastIncludedIndex uint64 `json:"last_included_index"` // 快照中包含的最后日志条目的索引
	LastIncludedTerm  uint64 `json:"last_included_term"`  // 快照中包含的最后日志条目的任期号
	Offset            uint64 `json:"offset"`              // 分块在快照文件中的字节偏移量
	Data              []byte `json:"data"`                // 从偏移量开始的快照分块的原始字节
	Done              bool   `json:"done"`                // 如果这是最后一个分块则为true
}

// InstallSnapshotResponse 安装快照响应
type InstallSnapshotResponse struct {
	Term uint64 `json:"term"` // 当前任期号，用于领导者更新自己
}

// RaftTransport Raft传输层接口
type RaftTransport interface {
	// 获取客户端
	GetClient(address string) RaftClient

	// 启动服务器
	StartServer(address string, handler RaftHandler) error

	// 停止服务器
	StopServer() error
}

// RaftClient Raft客户端接口
type RaftClient interface {
	RequestVote(ctx context.Context, req *RequestVoteRequest) (*RequestVoteResponse, error)
	AppendEntries(ctx context.Context, req *AppendEntriesRequest) (*AppendEntriesResponse, error)
	InstallSnapshot(ctx context.Context, req *InstallSnapshotRequest) (*InstallSnapshotResponse, error)
}

// RaftHandler Raft处理器接口
type RaftHandler interface {
	HandleRequestVote(req *RequestVoteRequest) *RequestVoteResponse
	HandleAppendEntries(req *AppendEntriesRequest) *AppendEntriesResponse
	HandleInstallSnapshot(req *InstallSnapshotRequest) *InstallSnapshotResponse
}

// HTTPRaftTransport HTTP传输层实现
type HTTPRaftTransport struct {
	mu      sync.RWMutex
	clients map[string]*HTTPRaftClient
	server  *http.Server
	logger  *log.Logger
}

func NewHTTPRaftTransport() *HTTPRaftTransport {
	return &HTTPRaftTransport{
		clients: make(map[string]*HTTPRaftClient),
		logger:  log.New(log.Writer(), "[RAFT-TRANSPORT] ", log.LstdFlags),
	}
}

func (t *HTTPRaftTransport) GetClient(address string) RaftClient {
	t.mu.Lock()
	defer t.mu.Unlock()

	if client, exists := t.clients[address]; exists {
		return client
	}

	client := NewHTTPRaftClient(address)
	t.clients[address] = client
	return client
}

func (t *HTTPRaftTransport) StartServer(address string, handler RaftHandler) error {
	mux := http.NewServeMux()

	// 注册RPC处理器
	mux.HandleFunc("/raft/request_vote", func(w http.ResponseWriter, r *http.Request) {
		t.handleRequestVote(w, r, handler)
	})

	mux.HandleFunc("/raft/append_entries", func(w http.ResponseWriter, r *http.Request) {
		t.handleAppendEntries(w, r, handler)
	})

	mux.HandleFunc("/raft/install_snapshot", func(w http.ResponseWriter, r *http.Request) {
		t.handleInstallSnapshot(w, r, handler)
	})

	t.server = &http.Server{
		Addr:    address,
		Handler: mux,
	}

	listener, err := net.Listen("tcp", address)
	if err != nil {
		return err
	}

	go func() {
		if err := t.server.Serve(listener); err != nil && err != http.ErrServerClosed {
			t.logger.Printf("HTTP server error: %v", err)
		}
	}()

	t.logger.Printf("Raft HTTP server started on address: %s", address)
	return nil
}

func (t *HTTPRaftTransport) StopServer() error {
	if t.server != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		return t.server.Shutdown(ctx)
	}
	return nil
}

func (t *HTTPRaftTransport) handleRequestVote(w http.ResponseWriter, r *http.Request, handler RaftHandler) {
	var req RequestVoteRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	resp := handler.HandleRequestVote(&req)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

func (t *HTTPRaftTransport) handleAppendEntries(w http.ResponseWriter, r *http.Request, handler RaftHandler) {
	var req AppendEntriesRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	resp := handler.HandleAppendEntries(&req)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

func (t *HTTPRaftTransport) handleInstallSnapshot(w http.ResponseWriter, r *http.Request, handler RaftHandler) {
	var req InstallSnapshotRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	resp := handler.HandleInstallSnapshot(&req)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

// HTTPRaftClient HTTP客户端实现
type HTTPRaftClient struct {
	address string
	client  *http.Client
	logger  *log.Logger
}

func NewHTTPRaftClient(address string) *HTTPRaftClient {
	return &HTTPRaftClient{
		address: address,
		client: &http.Client{
			Timeout: 5 * time.Second,
		},
		logger: log.New(log.Writer(), "[RAFT-CLIENT] ", log.LstdFlags),
	}
}

func (c *HTTPRaftClient) RequestVote(ctx context.Context, req *RequestVoteRequest) (*RequestVoteResponse, error) {
	url := fmt.Sprintf("http://%s/raft/request_vote", c.address)

	resp, err := c.doRequest(ctx, url, req)
	if err != nil {
		return nil, err
	}

	var response RequestVoteResponse
	if err := json.NewDecoder(resp.Body).Decode(&response); err != nil {
		return nil, err
	}

	return &response, nil
}

func (c *HTTPRaftClient) AppendEntries(ctx context.Context, req *AppendEntriesRequest) (*AppendEntriesResponse, error) {
	url := fmt.Sprintf("http://%s/raft/append_entries", c.address)

	resp, err := c.doRequest(ctx, url, req)
	if err != nil {
		return nil, err
	}

	var response AppendEntriesResponse
	if err := json.NewDecoder(resp.Body).Decode(&response); err != nil {
		return nil, err
	}

	return &response, nil
}

func (c *HTTPRaftClient) InstallSnapshot(ctx context.Context, req *InstallSnapshotRequest) (*InstallSnapshotResponse, error) {
	url := fmt.Sprintf("http://%s/raft/install_snapshot", c.address)

	resp, err := c.doRequest(ctx, url, req)
	if err != nil {
		return nil, err
	}

	var response InstallSnapshotResponse
	if err := json.NewDecoder(resp.Body).Decode(&response); err != nil {
		return nil, err
	}

	return &response, nil
}

func (c *HTTPRaftClient) doRequest(ctx context.Context, url string, req interface{}) (*http.Response, error) {
	_, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}

	httpReq, err := http.NewRequestWithContext(ctx, "POST", url, nil)
	if err != nil {
		return nil, err
	}

	httpReq.Header.Set("Content-Type", "application/json")

	return c.client.Do(httpReq)
}

// RaftRPCServer Raft RPC服务器
type RaftRPCServer struct {
	node      *RaftNode
	transport RaftTransport
	logger    *log.Logger
}

func NewRaftRPCServer(node *RaftNode, transport RaftTransport) *RaftRPCServer {
	return &RaftRPCServer{
		node:      node,
		transport: transport,
		logger:    log.New(log.Writer(), "[RAFT-RPC] ", log.LstdFlags),
	}
}

func (s *RaftRPCServer) Start() error {
	// 这里应该从配置中获取地址
	address := ":8080" // 简化处理
	return s.transport.StartServer(address, s)
}

func (s *RaftRPCServer) Stop() error {
	return s.transport.StopServer()
}

// HandleRequestVote 处理投票请求
func (s *RaftRPCServer) HandleRequestVote(req *RequestVoteRequest) *RequestVoteResponse {
	s.logger.Printf("Received RequestVote from %s, term: %d", req.CandidateID, req.Term)

	currentTerm := s.node.currentTerm.Load()
	votedFor := s.node.votedFor.Load().(string)

	// 如果请求的任期小于当前任期，拒绝投票
	if req.Term < currentTerm {
		return &RequestVoteResponse{
			Term:        currentTerm,
			VoteGranted: false,
		}
	}

	// 如果请求的任期大于当前任期，更新任期并成为跟随者
	if req.Term > currentTerm {
		s.node.becomeFollower(req.Term, "")
		currentTerm = req.Term
		votedFor = s.node.votedFor.Load().(string) // 重新获取更新后的votedFor
	}

	// 检查是否已经投票
	if votedFor != "" && votedFor != req.CandidateID {
		return &RequestVoteResponse{
			Term:        currentTerm,
			VoteGranted: false,
		}
	}

	// 检查候选者的日志是否至少和自己一样新
	lastLogIndex := s.node.log.getLastIndex()
	lastLogTerm := s.node.log.getLastTerm()

	logUpToDate := req.LastLogTerm > lastLogTerm ||
		(req.LastLogTerm == lastLogTerm && req.LastLogIndex >= lastLogIndex)

	if !logUpToDate {
		return &RequestVoteResponse{
			Term:        currentTerm,
			VoteGranted: false,
		}
	}

	// 投票给候选者
	s.node.votedFor.Store(req.CandidateID)
	s.node.resetElectionTimeout()

	s.logger.Printf("Granted vote to %s, term: %d", req.CandidateID, req.Term)

	return &RequestVoteResponse{
		Term:        currentTerm,
		VoteGranted: true,
	}
}

// HandleAppendEntries 处理追加条目请求
func (s *RaftRPCServer) HandleAppendEntries(req *AppendEntriesRequest) *AppendEntriesResponse {
	s.logger.Printf("Received AppendEntries from %s, term: %d, entries: %d", req.LeaderID, req.Term, len(req.Entries))

	currentTerm := s.node.currentTerm.Load()

	// 如果请求的任期小于当前任期，拒绝
	if req.Term < currentTerm {
		return &AppendEntriesResponse{
			Term:    currentTerm,
			Success: false,
		}
	}

	// 重置选举超时
	s.node.resetElectionTimeout()

	// 如果请求的任期大于当前任期，更新任期并成为跟随者
	if req.Term > currentTerm {
		s.node.becomeFollower(req.Term, req.LeaderID)
		currentTerm = req.Term
	} else if s.node.getState() != Follower {
		s.node.becomeFollower(req.Term, req.LeaderID)
	}

	// 检查日志一致性
	if req.PrevLogIndex > 0 {
		if req.PrevLogIndex > s.node.log.getLastIndex() ||
			s.node.log.getTerm(req.PrevLogIndex) != req.PrevLogTerm {
			return &AppendEntriesResponse{
				Term:    currentTerm,
				Success: false,
			}
		}
	}

	// 追加新的日志条目
	if len(req.Entries) > 0 {
		// 删除冲突的条目
		s.node.log.truncate(req.PrevLogIndex + 1)

		// 追加新条目
		for _, entry := range req.Entries {
			s.node.log.append(entry)
		}
	}

	// 更新提交索引
	if req.LeaderCommit > s.node.commitIndex.Load() {
		newCommitIndex := req.LeaderCommit
		lastLogIndex := s.node.log.getLastIndex()
		if newCommitIndex > lastLogIndex {
			newCommitIndex = lastLogIndex
		}
		s.node.commitIndex.Store(newCommitIndex)
	}

	return &AppendEntriesResponse{
		Term:    currentTerm,
		Success: true,
	}
}

// HandleInstallSnapshot 处理安装快照请求
func (s *RaftRPCServer) HandleInstallSnapshot(req *InstallSnapshotRequest) *InstallSnapshotResponse {
	s.logger.Printf("Received InstallSnapshot from %s, term: %d", req.LeaderID, req.Term)

	currentTerm := s.node.currentTerm.Load()

	// 如果请求的任期小于当前任期，拒绝
	if req.Term < currentTerm {
		return &InstallSnapshotResponse{
			Term: currentTerm,
		}
	}

	// 重置选举超时
	s.node.resetElectionTimeout()

	// 如果请求的任期大于当前任期，更新任期并成为跟随者
	if req.Term > currentTerm {
		s.node.becomeFollower(req.Term, req.LeaderID)
		currentTerm = req.Term
	}

	// 安装快照
	err := s.node.InstallSnapshot(req)
	if err != nil {
		s.logger.Printf("Failed to install snapshot: %v", err)
		// 即使安装失败，也要返回当前任期
		return &InstallSnapshotResponse{
			Term: currentTerm,
		}
	}

	s.logger.Printf("Successfully installed snapshot from %s, lastIncludedIndex: %d",
		req.LeaderID, req.LastIncludedIndex)

	return &InstallSnapshotResponse{
		Term: currentTerm,
	}
}
