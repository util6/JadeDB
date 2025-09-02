/*
JadeDB Raft性能基准测试

本文件包含Raft算法的性能基准测试，用于评估：
1. 日志复制性能
2. 领导者选举性能
3. 快照创建性能
4. 持久化操作性能
5. 并发处理能力
6. 内存使用效率
*/

package test

// TODO: 修复接口匹配问题后启用此测试文件
/*
import (
	"context"
	"fmt"
	"github.com/util6/JadeDB/distributed/raft"
	"sync"
	"testing"
	"time"
)

// BenchmarkRaftLogReplication 基准测试日志复制性能
func BenchmarkRaftLogReplication(b *testing.B) {
	// 创建单节点用于基准测试
	storage := NewMockStorageEngine()
	if err := storage.Open(); err != nil {
		b.Fatalf("Failed to open storage: %v", err)
	}
	defer storage.Close()

	stateMachine := raft.NewKVStateMachine(raft.DefaultStateMachineConfig())
	config := raft.DefaultRaftConfig()
	config.NodeID = "benchmark_node"

	node := raft.NewRaftNodeSimple(config.NodeID, config, stateMachine, storage)
	defer node.Stop()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := node.Start(ctx); err != nil {
		b.Fatalf("Failed to start node: %v", err)
	}

	// 模拟成为领导者
	node.state.Store(raft.Leader)

	// 准备测试数据
	entries := make([]raft.LogEntry, b.N)
	for i := 0; i < b.N; i++ {
		entries[i] = raft.LogEntry{
			Term:  1,
			Index: uint64(i + 1),
			Type:  raft.EntryNormal,
			Data:  []byte(fmt.Sprintf(`{"type":"PUT","key":"bench_key_%d","value":"bench_value_%d"}`, i, i)),
		}
	}

	// 开始基准测试
	b.ResetTimer()
	b.StartTimer()

	for i := 0; i < b.N; i++ {
		if err := node.AppendEntry(entries[i]); err != nil {
			b.Fatalf("Failed to append entry %d: %v", i, err)
		}
	}

	b.StopTimer()

	// 报告性能指标
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "entries/sec")
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N), "ns/entry")
}

// BenchmarkRaftLogReplicationConcurrent 基准测试并发日志复制
func BenchmarkRaftLogReplicationConcurrent(b *testing.B) {
	storage := NewMockStorageEngine()
	if err := storage.Open(); err != nil {
		b.Fatalf("Failed to open storage: %v", err)
	}
	defer storage.Close()

	stateMachine := raft.NewKVStateMachine(raft.DefaultStateMachineConfig())
	config := raft.DefaultRaftConfig()
	config.NodeID = "concurrent_benchmark_node"

	node := raft.NewRaftNodeSimple(config.NodeID, config, stateMachine, storage)
	defer node.Stop()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := node.Start(ctx); err != nil {
		b.Fatalf("Failed to start node: %v", err)
	}

	node.state.Store(raft.Leader)

	// 并发参数
	concurrency := 10
	entriesPerWorker := b.N / concurrency

	b.ResetTimer()
	b.StartTimer()

	var wg sync.WaitGroup
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			startIndex := workerID * entriesPerWorker
			endIndex := startIndex + entriesPerWorker

			for j := startIndex; j < endIndex; j++ {
				entry := raft.LogEntry{
					Term:  1,
					Index: uint64(j + 1),
					Type:  raft.EntryNormal,
					Data:  []byte(fmt.Sprintf(`{"type":"PUT","key":"concurrent_key_%d","value":"concurrent_value_%d"}`, j, j)),
				}

				if err := node.AppendEntry(entry); err != nil {
					b.Errorf("Worker %d failed to append entry %d: %v", workerID, j, err)
				}
			}
		}(i)
	}

	wg.Wait()
	b.StopTimer()

	// 报告并发性能指标
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "entries/sec")
	b.ReportMetric(float64(concurrency), "workers")
}

// BenchmarkRaftPersistence 基准测试持久化性能
func BenchmarkRaftPersistence(b *testing.B) {
	storage := NewMockStorageEngine()
	if err := storage.Open(); err != nil {
		b.Fatalf("Failed to open storage: %v", err)
	}
	defer storage.Close()

	// 创建持久化存储
	config := DefaultRaftPersistenceConfig()
	persistence := NewStorageRaftPersistence(storage, config)
	if err := persistence.Open(); err != nil {
		b.Fatalf("Failed to open persistence: %v", err)
	}
	defer persistence.Close()

	// 准备测试数据
	entries := make([]raft.LogEntry, b.N)
	for i := 0; i < b.N; i++ {
		entries[i] = raft.LogEntry{
			Term:  1,
			Index: uint64(i + 1),
			Type:  raft.EntryNormal,
			Data:  []byte(fmt.Sprintf(`{"type":"PUT","key":"persist_key_%d","value":"persist_value_%d"}`, i, i)),
		}
	}

	b.ResetTimer()
	b.StartTimer()

	// 批量持久化日志条目
	batchSize := 100
	for i := 0; i < b.N; i += batchSize {
		end := i + batchSize
		if end > b.N {
			end = b.N
		}

		batch := entries[i:end]
		if err := persistence.AppendLogEntries(batch); err != nil {
			b.Fatalf("Failed to persist batch starting at %d: %v", i, err)
		}
	}

	b.StopTimer()

	// 报告持久化性能指标
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "entries/sec")
	b.ReportMetric(float64(len(entries[0].Data)), "bytes/entry")
}

// BenchmarkRaftSnapshot 基准测试快照性能
func BenchmarkRaftSnapshot(b *testing.B) {
	storage := NewMockStorageEngine()
	if err := storage.Open(); err != nil {
		b.Fatalf("Failed to open storage: %v", err)
	}
	defer storage.Close()

	stateMachine := raft.NewKVStateMachine(raft.DefaultStateMachineConfig())

	// 向状态机添加大量数据
	dataSize := 10000
	for i := 0; i < dataSize; i++ {
		entry := raft.LogEntry{
			Term:  1,
			Index: uint64(i + 1),
			Type:  raft.EntryNormal,
			Data:  []byte(fmt.Sprintf(`{"type":"PUT","key":"snapshot_key_%d","value":"snapshot_value_%d"}`, i, i)),
		}
		stateMachine.Apply(entry)
	}

	b.ResetTimer()
	b.StartTimer()

	for i := 0; i < b.N; i++ {
		_, err := stateMachine.Snapshot()
		if err != nil {
			b.Fatalf("Failed to create snapshot %d: %v", i, err)
		}
	}

	b.StopTimer()

	// 报告快照性能指标
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "snapshots/sec")
	b.ReportMetric(float64(dataSize), "entries/snapshot")
}

// BenchmarkRaftStateTransition 基准测试状态转换性能
func BenchmarkRaftStateTransition(b *testing.B) {
	storage := NewMockStorageEngine()
	if err := storage.Open(); err != nil {
		b.Fatalf("Failed to open storage: %v", err)
	}
	defer storage.Close()

	stateMachine := raft.NewKVStateMachine(raft.DefaultStateMachineConfig())
	config := raft.DefaultRaftConfig()
	config.NodeID = "state_transition_node"

	node := raft.NewRaftNodeSimple(config.NodeID, config, stateMachine, storage)
	defer node.Stop()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := node.Start(ctx); err != nil {
		b.Fatalf("Failed to start node: %v", err)
	}

	states := []raft.RaftState{raft.Follower, raft.Candidate, raft.Leader}
	stateNames := []string{"Follower", "Candidate", "Leader"}

	b.ResetTimer()
	b.StartTimer()

	for i := 0; i < b.N; i++ {
		state := states[i%len(states)]
		node.state.Store(state)

		// 模拟状态转换的一些操作
		switch state {
		case raft.Follower:
			node.currentTerm.Add(1)
		case raft.Candidate:
			node.votedFor.Store("self")
		case raft.Leader:
			node.leader.Store(node.nodeID)
		}
	}

	b.StopTimer()

	// 报告状态转换性能指标
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "transitions/sec")

	for i, state := range states {
		count := 0
		for j := 0; j < b.N; j++ {
			if j%len(states) == i {
				count++
			}
		}
		b.ReportMetric(float64(count), stateNames[i]+"_transitions")
	}
}

// BenchmarkRaftMemoryUsage 基准测试内存使用
func BenchmarkRaftMemoryUsage(b *testing.B) {
	storage := NewMockStorageEngine()
	if err := storage.Open(); err != nil {
		b.Fatalf("Failed to open storage: %v", err)
	}
	defer storage.Close()

	stateMachine := raft.NewKVStateMachine(raft.DefaultStateMachineConfig())
	config := raft.DefaultRaftConfig()
	config.NodeID = "memory_test_node"

	node := raft.NewRaftNodeSimple(config.NodeID, config, stateMachine, storage)
	defer node.Stop()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := node.Start(ctx); err != nil {
		b.Fatalf("Failed to start node: %v", err)
	}

	node.state.Store(raft.Leader)

	// 测试不同大小的日志条目对内存的影响
	entrySizes := []int{100, 1000, 10000} // 字节

	for _, size := range entrySizes {
		b.Run(fmt.Sprintf("EntrySize_%d", size), func(b *testing.B) {
			data := make([]byte, size)
			for i := range data {
				data[i] = byte(i % 256)
			}

			b.ResetTimer()
			b.StartTimer()

			for i := 0; i < b.N; i++ {
				entry := raft.LogEntry{
					Term:  1,
					Index: uint64(i + 1),
					Type:  raft.EntryNormal,
					Data:  data,
				}

				if err := node.AppendEntry(entry); err != nil {
					b.Fatalf("Failed to append entry %d: %v", i, err)
				}
			}

			b.StopTimer()

			// 报告内存相关指标
			b.ReportMetric(float64(size), "bytes/entry")
			b.ReportMetric(float64(b.N*size), "total_bytes")
			b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "entries/sec")
		})
	}
}

// BenchmarkRaftFailureRecovery 基准测试故障恢复性能
func BenchmarkRaftFailureRecovery(b *testing.B) {
	storage := NewMockStorageEngine()
	if err := storage.Open(); err != nil {
		b.Fatalf("Failed to open storage: %v", err)
	}
	defer storage.Close()

	stateMachine := raft.NewKVStateMachine(raft.DefaultStateMachineConfig())
	config := raft.DefaultRaftConfig()
	config.NodeID = "recovery_benchmark_node"

	node := raft.NewRaftNodeSimple(config.NodeID, config, stateMachine, storage)
	defer node.Stop()

	// 创建故障恢复管理器
	recoveryConfig := DefaultFailureRecoveryConfig()
	recoveryConfig.FailureDetectionInterval = 10 * time.Millisecond
	recovery := NewRaftFailureRecovery(node, recoveryConfig)
	defer recovery.Stop()

	b.ResetTimer()
	b.StartTimer()

	for i := 0; i < b.N; i++ {
		// 模拟节点心跳更新
		nodeID := fmt.Sprintf("test_node_%d", i%10)
		recovery.UpdateNodeHeartbeat(nodeID)

		// 模拟故障检测
		recovery.DetectFailures()

		// 模拟网络可达性更新
		recovery.UpdateNodeReachability(nodeID, i%2 == 0)
	}

	b.StopTimer()

	// 获取故障恢复指标
	metrics := recovery.GetFailureRecoveryMetrics()

	// 报告故障恢复性能指标
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "operations/sec")
	b.ReportMetric(float64(metrics.TotalFailuresDetected), "failures_detected")
	b.ReportMetric(float64(metrics.TotalRecoveryOperations), "recovery_operations")
}
*/

// 占位符，避免空包错误
var _ = 1
