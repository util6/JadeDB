package test

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/util6/JadeDB/common"
	"github.com/util6/JadeDB/distributed/coordination"
	"github.com/util6/JadeDB/distributed/locks"
	"github.com/util6/JadeDB/storage"
)

// MockStorageEngine 模拟存储引擎
type MockStorageEngine struct {
	mu   sync.RWMutex
	data map[string][]byte
}

func NewMockStorageEngine() *MockStorageEngine {
	return &MockStorageEngine{
		data: make(map[string][]byte),
	}
}

func (m *MockStorageEngine) Get(key []byte) ([]byte, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	value, exists := m.data[string(key)]
	if !exists {
		return nil, nil
	}

	// 返回副本
	result := make([]byte, len(value))
	copy(result, value)
	return result, nil
}

func (m *MockStorageEngine) Put(key, value []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// 存储副本
	valueCopy := make([]byte, len(value))
	copy(valueCopy, value)
	m.data[string(key)] = valueCopy
	return nil
}

func (m *MockStorageEngine) Delete(key []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	delete(m.data, string(key))
	return nil
}

func (m *MockStorageEngine) Scan(startKey, endKey []byte, limit int) ([]storage.KVPair, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var results []storage.KVPair
	startStr := string(startKey)
	endStr := ""
	if endKey != nil {
		endStr = string(endKey)
	}

	count := 0
	for key, value := range m.data {
		if count >= limit && limit > 0 {
			break
		}

		if key >= startStr && (endStr == "" || key < endStr) {
			keyCopy := make([]byte, len(key))
			copy(keyCopy, []byte(key))
			valueCopy := make([]byte, len(value))
			copy(valueCopy, value)

			results = append(results, storage.KVPair{
				Key:   keyCopy,
				Value: valueCopy,
			})
			count++
		}
	}

	return results, nil
}

func (m *MockStorageEngine) Close() error {
	return nil
}

func (m *MockStorageEngine) Flush() error {
	return nil
}

func (m *MockStorageEngine) Exists(key []byte) (bool, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	_, exists := m.data[string(key)]
	return exists, nil
}

func (m *MockStorageEngine) BatchPut(batch []storage.KVPair) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	for _, kv := range batch {
		valueCopy := make([]byte, len(kv.Value))
		copy(valueCopy, kv.Value)
		m.data[string(kv.Key)] = valueCopy
	}
	return nil
}

func (m *MockStorageEngine) BatchGet(keys [][]byte) ([][]byte, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	results := make([][]byte, len(keys))
	for i, key := range keys {
		if value, exists := m.data[string(key)]; exists {
			valueCopy := make([]byte, len(value))
			copy(valueCopy, value)
			results[i] = valueCopy
		}
	}
	return results, nil
}

func (m *MockStorageEngine) BatchDelete(keys [][]byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	for _, key := range keys {
		delete(m.data, string(key))
	}
	return nil
}

func (m *MockStorageEngine) NewIterator(options *storage.IteratorOptions) (storage.Iterator, error) {
	// 简化实现，返回nil
	return nil, nil
}

func (m *MockStorageEngine) Open() error {
	return nil
}

func (m *MockStorageEngine) Sync() error {
	return nil
}

func (m *MockStorageEngine) GetEngineType() storage.EngineType {
	return storage.LSMTreeEngine
}

func (m *MockStorageEngine) GetEngineInfo() *storage.EngineInfo {
	return &storage.EngineInfo{
		Type:        storage.LSMTreeEngine,
		Version:     "mock-1.0",
		Description: "Mock storage engine for testing",
		Features:    []string{"basic-operations"},
	}
}

func (m *MockStorageEngine) GetStats() *storage.EngineStats {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return &storage.EngineStats{
		KeyCount: int64(len(m.data)),
	}
}

// MockTimestampOracle 模拟时间戳服务器
type MockTimestampOracle struct {
	mu      sync.Mutex
	counter uint64
}

func NewMockTimestampOracle() coordination.TimestampOracle {
	return &MockTimestampOracle{counter: 1000}
}

func (m *MockTimestampOracle) GetTimestamp(ctx context.Context) (uint64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.counter++
	return m.counter, nil
}

func (m *MockTimestampOracle) GetTimestamps(ctx context.Context, count uint32) (uint64, uint64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	start := m.counter + 1
	m.counter += uint64(count)
	return start, m.counter, nil
}

func (m *MockTimestampOracle) UpdateTimestamp(ctx context.Context, timestamp uint64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if timestamp > m.counter {
		m.counter = timestamp
	}
	return nil
}

func (m *MockTimestampOracle) GetCurrentTimestamp() uint64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.counter
}

func (m *MockTimestampOracle) Start() error {
	return nil
}

func (m *MockTimestampOracle) Stop() error {
	return nil
}

func (m *MockTimestampOracle) IsLeader() bool {
	return true
}

func (m *MockTimestampOracle) GetMetrics() *coordination.TSOMetrics {
	return &coordination.TSOMetrics{
		TotalRequests:    100,
		SuccessRequests:  100,
		FailedRequests:   0,
		AvgLatency:       time.Millisecond,
		CurrentTimestamp: m.GetCurrentTimestamp(),
		IsLeader:         true,
	}
}

// MockDistributedLockManager 简化的分布式锁管理器用于测试
type MockDistributedLockManager struct {
	mu    sync.RWMutex
	locks map[string]*MockLockInfo
}

type MockLockInfo struct {
	TxnID      string
	LockType   common.LockType
	PrimaryKey []byte
	StartTS    uint64
	CreatedAt  time.Time
}

func NewMockDistributedLockManagerSimple() *MockDistributedLockManager {
	return &MockDistributedLockManager{
		locks: make(map[string]*MockLockInfo),
	}
}

func (m *MockDistributedLockManager) AcquireDistributedLock(ctx context.Context, key []byte, txnID string, lockType common.LockType, primaryKey []byte, startTS uint64) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	keyStr := string(key)
	if existing, exists := m.locks[keyStr]; exists {
		if existing.TxnID != txnID {
			return fmt.Errorf("lock conflict: key %s already locked by transaction %s", keyStr, existing.TxnID)
		}
	}

	m.locks[keyStr] = &MockLockInfo{
		TxnID:      txnID,
		LockType:   lockType,
		PrimaryKey: primaryKey,
		StartTS:    startTS,
		CreatedAt:  time.Now(),
	}

	return nil
}

func (m *MockDistributedLockManager) ReleaseDistributedLock(key []byte, txnID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	keyStr := string(key)
	if existing, exists := m.locks[keyStr]; exists {
		if existing.TxnID == txnID {
			delete(m.locks, keyStr)
		}
	}

	return nil
}

func (m *MockDistributedLockManager) CheckLock(key []byte) (*MockLockInfo, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	keyStr := string(key)
	if lock, exists := m.locks[keyStr]; exists {
		return lock, nil
	}

	return nil, nil
}

func (m *MockDistributedLockManager) Stop() error {
	return nil
}

// 创建真实的DistributedLockManager用于测试
func NewMockDistributedLockManager() *locks.DistributedLockManager {
	storage := NewMockStorageEngine()
	config := &locks.DistributedLockConfig{
		NodeID:            "test_node",
		ShardCount:        16,
		ReplicationFactor: 3,
		LockTTL:           30 * time.Second,
		AcquireTimeout:    10 * time.Second,
		HeartbeatInterval: 5 * time.Second,
		DeadlockDetection: true,
		DetectionInterval: 2 * time.Second,
		MaxWaitTime:       30 * time.Second,
		BatchSize:         100,
		EnablePersistence: false, // 测试时禁用持久化
		EnableReplication: false, // 测试时禁用复制
	}
	return locks.NewDistributedLockManager("test_node", storage, config)
}

// MockLockManager 简单的锁管理器（用于向后兼容）
type MockLockManager struct {
	mu    sync.RWMutex
	locks map[string]*SimpleLockInfo
}

type SimpleLockInfo struct {
	TxnID     string
	LockType  common.LockType
	CreatedAt time.Time
}

func NewMockLockManager() *MockLockManager {
	return &MockLockManager{
		locks: make(map[string]*SimpleLockInfo),
	}
}

func (m *MockLockManager) AcquireLock(ctx context.Context, key string, txnID string, lockType common.LockType, timeout time.Duration) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if existing, exists := m.locks[key]; exists {
		if existing.TxnID != txnID {
			return fmt.Errorf("lock conflict: key %s already locked by transaction %s", key, existing.TxnID)
		}
	}

	m.locks[key] = &SimpleLockInfo{
		TxnID:     txnID,
		LockType:  lockType,
		CreatedAt: time.Now(),
	}

	return nil
}

func (m *MockLockManager) ReleaseLock(key string, txnID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if existing, exists := m.locks[key]; exists {
		if existing.TxnID == txnID {
			delete(m.locks, key)
		}
	}

	return nil
}

func (m *MockLockManager) CheckLock(key string) (*SimpleLockInfo, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if lock, exists := m.locks[key]; exists {
		return lock, nil
	}

	return nil, nil
}

func (m *MockLockManager) Stop() {
	// 清理所有锁
	m.mu.Lock()
	defer m.mu.Unlock()
	m.locks = make(map[string]*SimpleLockInfo)
}
