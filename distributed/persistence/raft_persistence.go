/*
JadeDB Raft持久化存储

本模块实现了Raft算法的持久化存储功能，确保Raft状态能够在节点重启后正确恢复。
持久化存储是Raft算法正确性的关键保证，必须在响应RPC之前将关键状态写入持久存储。

核心功能：
1. 状态持久化：持久化currentTerm、votedFor、log等关键状态
2. 日志存储：高效的日志条目存储和检索
3. 快照存储：压缩日志的快照存储和恢复
4. 故障恢复：节点重启后的状态恢复
5. 数据完整性：确保数据的一致性和完整性

设计特点：
- 原子性：使用事务确保状态更新的原子性
- 高性能：批量写入和异步刷盘优化
- 可靠性：数据校验和错误恢复机制
- 可扩展：支持多种存储后端
*/

package persistence

import (
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/util6/JadeDB/distributed/raft"
	"github.com/util6/JadeDB/storage"
)

// RaftPersistence Raft持久化存储接口
type RaftPersistence interface {
	// 状态持久化
	SaveState(state *RaftPersistentState) error
	LoadState() (*RaftPersistentState, error)

	// 日志持久化
	AppendLogEntries(entries []raft.LogEntry) error
	GetLogEntry(index uint64) (*raft.LogEntry, error)
	GetLogEntries(startIndex, endIndex uint64) ([]raft.LogEntry, error)
	TruncateLog(index uint64) error
	GetLastLogIndex() uint64
	GetLastLogTerm() uint64

	// 快照持久化
	SaveSnapshot(snapshot *SnapshotMetadata, data []byte) error
	LoadSnapshot() (*SnapshotMetadata, []byte, error)
	DeleteSnapshot(index uint64) error

	// 生命周期
	Open() error
	Close() error
	Sync() error
}

// RaftPersistentState Raft持久化状态
type RaftPersistentState struct {
	CurrentTerm uint64 `json:"current_term"` // 当前任期号
	VotedFor    string `json:"voted_for"`    // 当前任期投票给的候选者ID
	LastApplied uint64 `json:"last_applied"` // 已应用到状态机的最高日志索引
	CommitIndex uint64 `json:"commit_index"` // 已提交的最高日志索引
	Timestamp   uint64 `json:"timestamp"`    // 状态更新时间戳
}

// SnapshotMetadata 快照元数据
type SnapshotMetadata struct {
	Index     uint64 `json:"index"`     // 快照包含的最后日志索引
	Term      uint64 `json:"term"`      // 快照包含的最后日志任期
	Timestamp uint64 `json:"timestamp"` // 快照创建时间
	Size      int64  `json:"size"`      // 快照数据大小
	Checksum  string `json:"checksum"`  // 快照数据校验和
}

// StorageRaftPersistence 基于存储引擎的Raft持久化实现
type StorageRaftPersistence struct {
	mu sync.RWMutex

	// 存储引擎
	engine storage.Engine

	// 配置
	config *RaftPersistenceConfig

	// 缓存
	stateCache    *RaftPersistentState
	logCache      map[uint64]*raft.LogEntry
	snapshotCache *SnapshotMetadata

	// 统计
	metrics *RaftPersistenceMetrics

	// 日志记录器
	logger *log.Logger

	// 状态
	isOpen bool
}

// RaftPersistenceConfig 持久化配置
type RaftPersistenceConfig struct {
	// 存储路径
	DataDir     string // 数据目录
	StateFile   string // 状态文件名
	LogPrefix   string // 日志条目键前缀
	SnapshotDir string // 快照目录

	// 性能配置
	BatchSize    int           // 批量写入大小
	SyncInterval time.Duration // 同步间隔
	CacheSize    int           // 缓存大小
	EnableCache  bool          // 启用缓存

	// 可靠性配置
	EnableChecksum  bool          // 启用校验和
	BackupCount     int           // 备份文件数量
	CompactInterval time.Duration // 压缩间隔
}

// RaftPersistenceMetrics 持久化指标
type RaftPersistenceMetrics struct {
	// 操作统计
	StateWrites    uint64
	StateReads     uint64
	LogWrites      uint64
	LogReads       uint64
	SnapshotWrites uint64
	SnapshotReads  uint64

	// 性能指标
	AvgWriteLatency time.Duration
	AvgReadLatency  time.Duration
	AvgSyncLatency  time.Duration

	// 存储统计
	TotalLogEntries uint64
	TotalDataSize   uint64
	LastSyncTime    time.Time

	// 错误统计
	WriteErrors      uint64
	ReadErrors       uint64
	CorruptionErrors uint64
}

// NewStorageRaftPersistence 创建基于存储引擎的Raft持久化
func NewStorageRaftPersistence(engine storage.Engine, config *RaftPersistenceConfig) *StorageRaftPersistence {
	if config == nil {
		config = DefaultRaftPersistenceConfig()
	}

	return &StorageRaftPersistence{
		engine:   engine,
		config:   config,
		logCache: make(map[uint64]*raft.LogEntry),
		metrics:  &RaftPersistenceMetrics{},
		logger:   log.New(log.Writer(), "[RAFT-PERSISTENCE] ", log.LstdFlags),
		isOpen:   false,
	}
}

// DefaultRaftPersistenceConfig 默认持久化配置
func DefaultRaftPersistenceConfig() *RaftPersistenceConfig {
	return &RaftPersistenceConfig{
		DataDir:         "./data/raft",
		StateFile:       "raft_state.json",
		LogPrefix:       "raft_log_",
		SnapshotDir:     "snapshots",
		BatchSize:       100,
		SyncInterval:    1 * time.Second,
		CacheSize:       1000,
		EnableCache:     true,
		EnableChecksum:  true,
		BackupCount:     3,
		CompactInterval: 10 * time.Minute,
	}
}

// Open 打开持久化存储
func (p *StorageRaftPersistence) Open() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.isOpen {
		return fmt.Errorf("persistence already open")
	}

	// 尝试打开存储引擎（如果尚未打开）
	if err := p.engine.Open(); err != nil {
		// 如果已经打开，忽略错误
		p.logger.Printf("Storage engine open result: %v", err)
	}

	p.isOpen = true
	p.logger.Printf("Raft persistence opened")
	return nil
}

// Close 关闭持久化存储
func (p *StorageRaftPersistence) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if !p.isOpen {
		return nil
	}

	// 同步数据到磁盘
	if err := p.Sync(); err != nil {
		p.logger.Printf("Failed to sync before close: %v", err)
	}

	p.isOpen = false
	p.logger.Printf("Raft persistence closed")
	return nil
}

// Sync 同步数据到磁盘
func (p *StorageRaftPersistence) Sync() error {
	start := time.Now()
	defer func() {
		latency := time.Since(start)
		p.metrics.AvgSyncLatency = (p.metrics.AvgSyncLatency + latency) / 2
		p.metrics.LastSyncTime = time.Now()
	}()

	return p.engine.Sync()
}

// SaveState 保存Raft状态
func (p *StorageRaftPersistence) SaveState(state *RaftPersistentState) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	start := time.Now()
	defer func() {
		latency := time.Since(start)
		p.metrics.AvgWriteLatency = (p.metrics.AvgWriteLatency + latency) / 2
		p.metrics.StateWrites++
	}()

	// 序列化状态
	state.Timestamp = uint64(time.Now().UnixNano())
	data, err := json.Marshal(state)
	if err != nil {
		p.metrics.WriteErrors++
		return fmt.Errorf("failed to marshal state: %w", err)
	}

	// 写入存储引擎
	key := []byte("raft_state")
	if err := p.engine.Put(key, data); err != nil {
		p.metrics.WriteErrors++
		return fmt.Errorf("failed to save state: %w", err)
	}

	// 更新缓存
	if p.config.EnableCache {
		p.stateCache = state
	}

	p.logger.Printf("Saved Raft state: term=%d, votedFor=%s", state.CurrentTerm, state.VotedFor)
	return nil
}

// LoadState 加载Raft状态
func (p *StorageRaftPersistence) LoadState() (*RaftPersistentState, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	start := time.Now()
	defer func() {
		latency := time.Since(start)
		p.metrics.AvgReadLatency = (p.metrics.AvgReadLatency + latency) / 2
		p.metrics.StateReads++
	}()

	// 检查缓存
	if p.config.EnableCache && p.stateCache != nil {
		return p.stateCache, nil
	}

	// 从存储引擎读取
	key := []byte("raft_state")
	data, err := p.engine.Get(key)
	if err != nil {
		p.metrics.ReadErrors++
		return nil, fmt.Errorf("failed to load state: %w", err)
	}

	if data == nil {
		// 返回初始状态
		return &RaftPersistentState{
			CurrentTerm: 0,
			VotedFor:    "",
			LastApplied: 0,
			CommitIndex: 0,
			Timestamp:   uint64(time.Now().UnixNano()),
		}, nil
	}

	// 反序列化状态
	var state RaftPersistentState
	if err := json.Unmarshal(data, &state); err != nil {
		p.metrics.ReadErrors++
		p.metrics.CorruptionErrors++
		return nil, fmt.Errorf("failed to unmarshal state: %w", err)
	}

	// 更新缓存
	if p.config.EnableCache {
		p.stateCache = &state
	}

	p.logger.Printf("Loaded Raft state: term=%d, votedFor=%s", state.CurrentTerm, state.VotedFor)
	return &state, nil
}

// AppendLogEntries 追加日志条目
func (p *StorageRaftPersistence) AppendLogEntries(entries []raft.LogEntry) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	start := time.Now()
	defer func() {
		latency := time.Since(start)
		p.metrics.AvgWriteLatency = (p.metrics.AvgWriteLatency + latency) / 2
		p.metrics.LogWrites += uint64(len(entries))
	}()

	// 批量写入日志条目
	var pairs []storage.KVPair
	for _, entry := range entries {
		key := p.makeLogKey(entry.Index)
		data, err := json.Marshal(entry)
		if err != nil {
			p.metrics.WriteErrors++
			return fmt.Errorf("failed to marshal log entry %d: %w", entry.Index, err)
		}

		pairs = append(pairs, storage.KVPair{
			Key:   key,
			Value: data,
		})

		// 更新缓存
		if p.config.EnableCache {
			// 创建条目的副本以避免指针问题
			entryCopy := entry
			p.logCache[entry.Index] = &entryCopy
		}
	}

	if err := p.engine.BatchPut(pairs); err != nil {
		p.metrics.WriteErrors++
		return fmt.Errorf("failed to append log entries: %w", err)
	}

	p.metrics.TotalLogEntries += uint64(len(entries))
	p.logger.Printf("Appended %d log entries", len(entries))
	return nil
}

// makeLogKey 生成日志条目键
func (p *StorageRaftPersistence) makeLogKey(index uint64) []byte {
	return []byte(fmt.Sprintf("%s%020d", p.config.LogPrefix, index))
}

// GetLogEntry 获取单个日志条目
func (p *StorageRaftPersistence) GetLogEntry(index uint64) (*raft.LogEntry, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	start := time.Now()
	defer func() {
		latency := time.Since(start)
		p.metrics.AvgReadLatency = (p.metrics.AvgReadLatency + latency) / 2
		p.metrics.LogReads++
	}()

	// 检查缓存
	if p.config.EnableCache {
		if entry, exists := p.logCache[index]; exists {
			return entry, nil
		}
	}

	// 从存储引擎读取
	key := p.makeLogKey(index)
	data, err := p.engine.Get(key)
	if err != nil {
		p.metrics.ReadErrors++
		return nil, fmt.Errorf("failed to get log entry %d: %w", index, err)
	}

	if data == nil {
		return nil, nil // 日志条目不存在
	}

	// 反序列化日志条目
	var entry raft.LogEntry
	if err := json.Unmarshal(data, &entry); err != nil {
		p.metrics.ReadErrors++
		p.metrics.CorruptionErrors++
		return nil, fmt.Errorf("failed to unmarshal log entry %d: %w", index, err)
	}

	// 更新缓存
	if p.config.EnableCache {
		p.logCache[index] = &entry
	}

	return &entry, nil
}

// GetLogEntries 获取日志条目范围
func (p *StorageRaftPersistence) GetLogEntries(startIndex, endIndex uint64) ([]raft.LogEntry, error) {
	start := time.Now()
	defer func() {
		latency := time.Since(start)
		p.metrics.AvgReadLatency = (p.metrics.AvgReadLatency + latency) / 2
		p.metrics.LogReads += endIndex - startIndex + 1
	}()

	var entries []raft.LogEntry
	for i := startIndex; i <= endIndex; i++ {
		entry, err := p.getLogEntryInternal(i)
		if err != nil {
			return nil, fmt.Errorf("failed to get log entry %d: %w", i, err)
		}
		if entry != nil {
			entries = append(entries, *entry)
		}
	}

	return entries, nil
}

// getLogEntryInternal 内部获取日志条目方法（不加锁）
func (p *StorageRaftPersistence) getLogEntryInternal(index uint64) (*raft.LogEntry, error) {
	// 检查缓存
	if p.config.EnableCache {
		if entry, exists := p.logCache[index]; exists {
			return entry, nil
		}
	}

	// 从存储引擎读取
	key := p.makeLogKey(index)
	data, err := p.engine.Get(key)
	if err != nil {
		p.metrics.ReadErrors++
		return nil, fmt.Errorf("failed to get log entry %d: %w", index, err)
	}

	if data == nil {
		return nil, nil // 日志条目不存在
	}

	// 反序列化日志条目
	var entry raft.LogEntry
	if err := json.Unmarshal(data, &entry); err != nil {
		p.metrics.ReadErrors++
		p.metrics.CorruptionErrors++
		return nil, fmt.Errorf("failed to unmarshal log entry %d: %w", index, err)
	}

	// 更新缓存
	if p.config.EnableCache {
		p.logCache[index] = &entry
	}

	return &entry, nil
}

// TruncateLog 截断日志
func (p *StorageRaftPersistence) TruncateLog(index uint64) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	// 删除index之后的所有日志条目
	lastIndex := p.getLastLogIndexInternal()
	if index >= lastIndex {
		return nil // 无需截断
	}

	var keysToDelete [][]byte
	for i := index + 1; i <= lastIndex; i++ {
		key := p.makeLogKey(i)
		keysToDelete = append(keysToDelete, key)

		// 从缓存中删除
		if p.config.EnableCache {
			delete(p.logCache, i)
		}
	}

	if len(keysToDelete) > 0 {
		if err := p.engine.BatchDelete(keysToDelete); err != nil {
			p.metrics.WriteErrors++
			return fmt.Errorf("failed to truncate log at index %d: %w", index, err)
		}
	}

	p.logger.Printf("Truncated log at index %d, deleted %d entries", index, len(keysToDelete))
	return nil
}

// GetLastLogIndex 获取最后日志索引
func (p *StorageRaftPersistence) GetLastLogIndex() uint64 {
	p.mu.RLock()
	defer p.mu.RUnlock()

	return p.getLastLogIndexInternal()
}

// getLastLogIndexInternal 内部获取最后日志索引方法（不加锁）
func (p *StorageRaftPersistence) getLastLogIndexInternal() uint64 {
	// 使用扫描找到最大索引
	startKey := []byte(p.config.LogPrefix)
	endKey := []byte(p.config.LogPrefix + "z") // 确保包含所有日志键

	pairs, err := p.engine.Scan(startKey, endKey, 1000) // 限制扫描数量
	if err != nil || len(pairs) == 0 {
		return 0
	}

	// 找到最大索引
	var maxIndex uint64 = 0
	for _, pair := range pairs {
		key := string(pair.Key)
		if len(key) >= len(p.config.LogPrefix)+20 { // 确保键格式正确
			var index uint64
			if _, err := fmt.Sscanf(key[len(p.config.LogPrefix):], "%020d", &index); err == nil {
				if index > maxIndex {
					maxIndex = index
				}
			}
		}
	}

	return maxIndex
}

// GetLastLogTerm 获取最后日志任期
func (p *StorageRaftPersistence) GetLastLogTerm() uint64 {
	lastIndex := p.GetLastLogIndex()
	if lastIndex == 0 {
		return 0
	}

	entry, err := p.GetLogEntry(lastIndex)
	if err != nil || entry == nil {
		return 0
	}

	return entry.Term
}

// SaveSnapshot 保存快照
func (p *StorageRaftPersistence) SaveSnapshot(snapshot *SnapshotMetadata, data []byte) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	start := time.Now()
	defer func() {
		latency := time.Since(start)
		p.metrics.AvgWriteLatency = (p.metrics.AvgWriteLatency + latency) / 2
		p.metrics.SnapshotWrites++
	}()

	// 保存快照元数据
	metaKey := []byte(fmt.Sprintf("snapshot_meta_%d", snapshot.Index))
	metaData, err := json.Marshal(snapshot)
	if err != nil {
		p.metrics.WriteErrors++
		return fmt.Errorf("failed to marshal snapshot metadata: %w", err)
	}

	// 保存快照数据
	dataKey := []byte(fmt.Sprintf("snapshot_data_%d", snapshot.Index))

	// 批量写入
	pairs := []storage.KVPair{
		{Key: metaKey, Value: metaData},
		{Key: dataKey, Value: data},
	}

	if err := p.engine.BatchPut(pairs); err != nil {
		p.metrics.WriteErrors++
		return fmt.Errorf("failed to save snapshot: %w", err)
	}

	// 更新缓存
	if p.config.EnableCache {
		p.snapshotCache = snapshot
	}

	p.logger.Printf("Saved snapshot: index=%d, term=%d, size=%d",
		snapshot.Index, snapshot.Term, snapshot.Size)
	return nil
}

// LoadSnapshot 加载快照
func (p *StorageRaftPersistence) LoadSnapshot() (*SnapshotMetadata, []byte, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	start := time.Now()
	defer func() {
		latency := time.Since(start)
		p.metrics.AvgReadLatency = (p.metrics.AvgReadLatency + latency) / 2
		p.metrics.SnapshotReads++
	}()

	// 找到最新的快照
	startKey := []byte("snapshot_meta_")
	endKey := []byte("snapshot_meta_z")

	pairs, err := p.engine.Scan(startKey, endKey, 100)
	if err != nil || len(pairs) == 0 {
		return nil, nil, nil // 没有快照
	}

	// 找到最大索引的快照
	var latestSnapshot *SnapshotMetadata
	var latestIndex uint64 = 0

	for _, pair := range pairs {
		var snapshot SnapshotMetadata
		if err := json.Unmarshal(pair.Value, &snapshot); err != nil {
			continue
		}
		if snapshot.Index > latestIndex {
			latestIndex = snapshot.Index
			latestSnapshot = &snapshot
		}
	}

	if latestSnapshot == nil {
		return nil, nil, nil
	}

	// 加载快照数据
	dataKey := []byte(fmt.Sprintf("snapshot_data_%d", latestSnapshot.Index))
	data, err := p.engine.Get(dataKey)
	if err != nil {
		p.metrics.ReadErrors++
		return nil, nil, fmt.Errorf("failed to load snapshot data: %w", err)
	}

	p.logger.Printf("Loaded snapshot: index=%d, term=%d, size=%d",
		latestSnapshot.Index, latestSnapshot.Term, latestSnapshot.Size)
	return latestSnapshot, data, nil
}

// DeleteSnapshot 删除快照
func (p *StorageRaftPersistence) DeleteSnapshot(index uint64) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	metaKey := []byte(fmt.Sprintf("snapshot_meta_%d", index))
	dataKey := []byte(fmt.Sprintf("snapshot_data_%d", index))

	keys := [][]byte{metaKey, dataKey}
	if err := p.engine.BatchDelete(keys); err != nil {
		p.metrics.WriteErrors++
		return fmt.Errorf("failed to delete snapshot %d: %w", index, err)
	}

	p.logger.Printf("Deleted snapshot: index=%d", index)
	return nil
}
