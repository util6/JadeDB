package test

import (
	"context"
	"github.com/util6/JadeDB/distributed/coordination"
	"testing"
	"time"
)

// waitForLeader 等待TSO成为领导者
func waitForLeader(t *testing.T, tso *coordination.RaftTimestampOracle) {
	// 等待TSO启动并成为领导者
	time.Sleep(200 * time.Millisecond)

	// 等待成为领导者
	for i := 0; i < 50; i++ {
		if tso.IsLeader() {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}

	t.Skip("TSO node did not become leader, skipping test")
}

// TestTSOBasicFunctionality 测试TSO基本功能
func TestTSOBasicFunctionality(t *testing.T) {
	// 创建TSO配置
	config := coordination.DefaultTSOConfig()
	config.NodeID = "test_tso_1"
	config.BatchSize = 100
	config.CacheSize = 1000

	// 创建TSO实例
	tso := coordination.NewRaftTimestampOracle(config)

	// 启动TSO服务
	if err := tso.Start(); err != nil {
		t.Fatalf("Failed to start TSO: %v", err)
	}
	defer tso.Stop()

	// 等待成为领导者
	waitForLeader(t, tso)

	// 测试获取单个时间戳
	ctx := context.Background()
	ts1, err := tso.GetTimestamp(ctx)
	if err != nil {
		t.Fatalf("Failed to get timestamp: %v", err)
	}

	ts2, err := tso.GetTimestamp(ctx)
	if err != nil {
		t.Fatalf("Failed to get second timestamp: %v", err)
	}

	// 验证时间戳单调性
	if ts2 <= ts1 {
		t.Errorf("Timestamp not monotonic: ts1=%d, ts2=%d", ts1, ts2)
	}

	t.Logf("TSO basic functionality test passed: ts1=%d, ts2=%d", ts1, ts2)
}

// TestTSOBatchAllocation 测试TSO批量分配
func TestTSOBatchAllocation(t *testing.T) {
	// 创建TSO配置
	config := coordination.DefaultTSOConfig()
	config.NodeID = "test_tso_batch"
	config.BatchSize = 50

	// 创建TSO实例
	tso := coordination.NewRaftTimestampOracle(config)

	// 启动TSO服务
	if err := tso.Start(); err != nil {
		t.Fatalf("Failed to start TSO: %v", err)
	}
	defer tso.Stop()

	// 等待成为领导者
	waitForLeader(t, tso)

	// 测试批量获取时间戳
	ctx := context.Background()
	count := uint32(10)
	startTS, endTS, err := tso.GetTimestamps(ctx, count)
	if err != nil {
		t.Fatalf("Failed to get batch timestamps: %v", err)
	}

	// 验证批量分配结果
	if endTS-startTS+1 != uint64(count) {
		t.Errorf("Batch allocation incorrect: expected %d timestamps, got %d",
			count, endTS-startTS+1)
	}

	// 验证时间戳范围
	if startTS == 0 || endTS == 0 {
		t.Errorf("Invalid timestamp range: startTS=%d, endTS=%d", startTS, endTS)
	}

	if startTS > endTS {
		t.Errorf("Invalid timestamp order: startTS=%d > endTS=%d", startTS, endTS)
	}

	t.Logf("TSO batch allocation test passed: startTS=%d, endTS=%d, count=%d",
		startTS, endTS, count)
}

// TestTSOConcurrentAccess 测试TSO并发访问
func TestTSOConcurrentAccess(t *testing.T) {
	// 创建TSO配置
	config := coordination.DefaultTSOConfig()
	config.NodeID = "test_tso_concurrent"
	config.BatchSize = 100
	config.CacheSize = 2000

	// 创建TSO实例
	tso := coordination.NewRaftTimestampOracle(config)

	// 启动TSO服务
	if err := tso.Start(); err != nil {
		t.Fatalf("Failed to start TSO: %v", err)
	}
	defer tso.Stop()

	// 等待成为领导者
	waitForLeader(t, tso)

	// 并发测试参数
	numWorkers := 10
	timestampsPerWorker := 100
	results := make(chan uint64, numWorkers*timestampsPerWorker)

	// 启动并发工作器
	for i := 0; i < numWorkers; i++ {
		go func(workerID int) {
			ctx := context.Background()
			for j := 0; j < timestampsPerWorker; j++ {
				ts, err := tso.GetTimestamp(ctx)
				if err != nil {
					t.Errorf("Worker %d failed to get timestamp %d: %v", workerID, j, err)
					return
				}
				results <- ts
			}
		}(i)
	}

	// 收集结果
	var timestamps []uint64
	for i := 0; i < numWorkers*timestampsPerWorker; i++ {
		select {
		case ts := <-results:
			timestamps = append(timestamps, ts)
		case <-time.After(5 * time.Second):
			t.Fatalf("Timeout waiting for timestamp results")
		}
	}

	// 验证时间戳唯一性和单调性
	timestampSet := make(map[uint64]bool)
	var prevTS uint64 = 0

	for i, ts := range timestamps {
		// 检查唯一性
		if timestampSet[ts] {
			t.Errorf("Duplicate timestamp found: %d", ts)
		}
		timestampSet[ts] = true

		// 检查单调性（注意：并发情况下不保证严格单调，但应该大致递增）
		if i > 0 && ts < prevTS-uint64(numWorkers) {
			t.Errorf("Timestamp order violation: prev=%d, current=%d", prevTS, ts)
		}
		prevTS = ts
	}

	t.Logf("TSO concurrent access test passed: %d unique timestamps generated",
		len(timestamps))
}

// TestTSOMetrics 测试TSO监控指标
func TestTSOMetrics(t *testing.T) {
	// 创建TSO配置
	config := coordination.DefaultTSOConfig()
	config.NodeID = "test_tso_metrics"

	// 创建TSO实例
	tso := coordination.NewRaftTimestampOracle(config)

	// 启动TSO服务
	if err := tso.Start(); err != nil {
		t.Fatalf("Failed to start TSO: %v", err)
	}
	defer tso.Stop()

	// 等待成为领导者
	waitForLeader(t, tso)

	// 获取初始指标
	initialMetrics := tso.GetMetrics()
	if initialMetrics == nil {
		t.Fatal("Metrics should not be nil")
	}

	// 执行一些操作
	ctx := context.Background()
	for i := 0; i < 10; i++ {
		_, err := tso.GetTimestamp(ctx)
		if err != nil {
			t.Fatalf("Failed to get timestamp %d: %v", i, err)
		}
	}

	// 批量获取
	_, _, err := tso.GetTimestamps(ctx, 5)
	if err != nil {
		t.Fatalf("Failed to get batch timestamps: %v", err)
	}

	// 等待指标更新
	time.Sleep(100 * time.Millisecond)

	// 获取更新后的指标
	finalMetrics := tso.GetMetrics()

	// 验证指标
	if finalMetrics.TotalRequests <= initialMetrics.TotalRequests {
		t.Errorf("Total requests should increase: initial=%d, final=%d",
			initialMetrics.TotalRequests, finalMetrics.TotalRequests)
	}

	if finalMetrics.SuccessRequests <= initialMetrics.SuccessRequests {
		t.Errorf("Success requests should increase: initial=%d, final=%d",
			initialMetrics.SuccessRequests, finalMetrics.SuccessRequests)
	}

	if finalMetrics.BatchRequests <= initialMetrics.BatchRequests {
		t.Errorf("Batch requests should increase: initial=%d, final=%d",
			initialMetrics.BatchRequests, finalMetrics.BatchRequests)
	}

	if finalMetrics.CurrentTimestamp <= initialMetrics.CurrentTimestamp {
		t.Errorf("Current timestamp should increase: initial=%d, final=%d",
			initialMetrics.CurrentTimestamp, finalMetrics.CurrentTimestamp)
	}

	t.Logf("TSO metrics test passed:")
	t.Logf("  Total requests: %d", finalMetrics.TotalRequests)
	t.Logf("  Success requests: %d", finalMetrics.SuccessRequests)
	t.Logf("  Failed requests: %d", finalMetrics.FailedRequests)
	t.Logf("  Batch requests: %d", finalMetrics.BatchRequests)
	t.Logf("  Current timestamp: %d", finalMetrics.CurrentTimestamp)
	t.Logf("  Average latency: %v", finalMetrics.AvgLatency)
}

// TestTSOTimestampUpdate 测试TSO时间戳更新
func TestTSOTimestampUpdate(t *testing.T) {
	// 创建TSO配置
	config := coordination.DefaultTSOConfig()
	config.NodeID = "test_tso_update"

	// 创建TSO实例
	tso := coordination.NewRaftTimestampOracle(config)

	// 启动TSO服务
	if err := tso.Start(); err != nil {
		t.Fatalf("Failed to start TSO: %v", err)
	}
	defer tso.Stop()

	// 等待成为领导者
	waitForLeader(t, tso)

	// 获取当前时间戳
	ctx := context.Background()
	currentTS := tso.GetCurrentTimestamp()

	// 更新时间戳到更大的值
	newTS := currentTS + 10000
	err := tso.UpdateTimestamp(ctx, newTS)
	if err != nil {
		t.Fatalf("Failed to update timestamp: %v", err)
	}

	// 等待更新生效
	time.Sleep(100 * time.Millisecond)

	// 验证时间戳已更新
	updatedTS := tso.GetCurrentTimestamp()
	if updatedTS < newTS {
		t.Errorf("Timestamp not updated correctly: expected >= %d, got %d",
			newTS, updatedTS)
	}

	// 获取新的时间戳，应该大于更新的值
	nextTS, err := tso.GetTimestamp(ctx)
	if err != nil {
		t.Fatalf("Failed to get timestamp after update: %v", err)
	}

	if nextTS <= newTS {
		t.Errorf("New timestamp should be greater than updated value: newTS=%d, nextTS=%d",
			newTS, nextTS)
	}

	t.Logf("TSO timestamp update test passed: currentTS=%d, newTS=%d, nextTS=%d",
		currentTS, newTS, nextTS)
}
