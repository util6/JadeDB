package JadeDB

import (
	"fmt"
	"github.com/util6/JadeDB/utils"
	"math/rand"
	"sync"
	"testing"
	"time"
)

// BenchmarkWriteThroughput 测试写入吞吐量
func BenchmarkWriteThroughput(b *testing.B) {
	tests := []struct {
		name           string
		enablePipeline bool
		batchSize      int
		valueSize      int
	}{
		{"Original_Small", false, 1, 100},
		{"Pipeline_Small", true, 1, 100},
		{"Original_Medium", false, 10, 1000},
		{"Pipeline_Medium", true, 10, 1000},
		{"Original_Large", false, 100, 10000},
		{"Pipeline_Large", true, 100, 10000},
	}

	for _, tt := range tests {
		b.Run(tt.name, func(b *testing.B) {
			benchmarkWriteThroughput(b, tt.enablePipeline, tt.batchSize, tt.valueSize)
		})
	}
}

func benchmarkWriteThroughput(b *testing.B, enableOptimization bool, batchSize, valueSize int) {
	// 清理测试目录
	clearDir()

	// 创建测试选项
	opt := &Options{
		WorkDir:          "./bench_test",
		SSTableMaxSz:     64 << 20,  // 64MB
		MemTableSize:     16 << 20,  // 16MB
		ValueLogFileSize: 128 << 20, // 128MB
		ValueThreshold:   1024,
		MaxBatchCount:    1000,
		MaxBatchSize:     16 << 20, // 16MB
	}

	// 打开数据库
	db := Open(opt)
	db.SetOptimizationEnabled(enableOptimization)
	defer db.Close()

	// 生成测试数据
	entries := make([]*utils.Entry, batchSize)
	for i := 0; i < batchSize; i++ {
		key := fmt.Sprintf("bench_key_%d_%d", i, rand.Int63())
		value := make([]byte, valueSize)
		rand.Read(value)
		entries[i] = utils.NewEntry([]byte(key), value)
	}

	b.ResetTimer()
	b.ReportAllocs()

	start := time.Now()

	// 执行基准测试
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			// 随机选择一个条目进行写入
			entry := entries[rand.Intn(len(entries))]
			if err := db.Set(entry); err != nil {
				b.Fatal(err)
			}
		}
	})

	duration := time.Since(start)

	// 计算吞吐量
	throughput := float64(b.N) / duration.Seconds()
	b.ReportMetric(throughput, "ops/sec")

	// 报告优化统计信息
	if enableOptimization {
		if stats := db.GetAllOptimizationStats(); stats != nil {
			b.Logf("Optimization stats: %+v", stats)
		}
	}
}

// BenchmarkWriteLatency 测试写入延迟
func BenchmarkWriteLatency(b *testing.B) {
	tests := []struct {
		name           string
		enablePipeline bool
		concurrency    int
	}{
		{"Original_Seq", false, 1},
		{"Pipeline_Seq", true, 1},
		{"Original_Concurrent", false, 10},
		{"Pipeline_Concurrent", true, 10},
	}

	for _, tt := range tests {
		b.Run(tt.name, func(b *testing.B) {
			benchmarkWriteLatency(b, tt.enablePipeline, tt.concurrency)
		})
	}
}

func benchmarkWriteLatency(b *testing.B, enableOptimization bool, concurrency int) {
	clearDir()

	opt := &Options{
		WorkDir:          "./bench_test",
		SSTableMaxSz:     64 << 20,
		MemTableSize:     16 << 20,
		ValueLogFileSize: 128 << 20,
		ValueThreshold:   1024,
		MaxBatchCount:    100,
		MaxBatchSize:     4 << 20,
	}

	db := Open(opt)
	db.SetOptimizationEnabled(enableOptimization)
	defer db.Close()

	// 预热
	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("warmup_%d", i)
		value := make([]byte, 1024)
		entry := utils.NewEntry([]byte(key), value)
		db.Set(entry)
	}

	b.ResetTimer()

	// 延迟测量
	latencies := make([]time.Duration, b.N)

	var wg sync.WaitGroup
	semaphore := make(chan struct{}, concurrency)

	for i := 0; i < b.N; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			semaphore <- struct{}{}
			defer func() { <-semaphore }()

			key := fmt.Sprintf("latency_key_%d", idx)
			value := make([]byte, 1024)
			rand.Read(value)
			entry := utils.NewEntry([]byte(key), value)

			start := time.Now()
			if err := db.Set(entry); err != nil {
				b.Error(err)
				return
			}
			latencies[idx] = time.Since(start)
		}(i)
	}

	wg.Wait()

	// 计算延迟统计
	if len(latencies) > 0 {
		var total time.Duration
		for _, lat := range latencies {
			total += lat
		}
		avgLatency := total / time.Duration(len(latencies))

		// 计算P95和P99延迟
		sortedLatencies := make([]time.Duration, len(latencies))
		copy(sortedLatencies, latencies)

		// 简单排序
		for i := 0; i < len(sortedLatencies); i++ {
			for j := i + 1; j < len(sortedLatencies); j++ {
				if sortedLatencies[i] > sortedLatencies[j] {
					sortedLatencies[i], sortedLatencies[j] = sortedLatencies[j], sortedLatencies[i]
				}
			}
		}

		p95Index := int(float64(len(sortedLatencies)) * 0.95)
		p99Index := int(float64(len(sortedLatencies)) * 0.99)

		b.ReportMetric(float64(avgLatency.Nanoseconds()), "avg_latency_ns")
		b.ReportMetric(float64(sortedLatencies[p95Index].Nanoseconds()), "p95_latency_ns")
		b.ReportMetric(float64(sortedLatencies[p99Index].Nanoseconds()), "p99_latency_ns")
	}
}

// BenchmarkBatchWrite 测试批量写入性能
func BenchmarkBatchWrite(b *testing.B) {
	tests := []struct {
		name           string
		enablePipeline bool
		batchSize      int
	}{
		{"Original_Batch10", false, 10},
		{"Pipeline_Batch10", true, 10},
		{"Original_Batch100", false, 100},
		{"Pipeline_Batch100", true, 100},
		{"Original_Batch1000", false, 1000},
		{"Pipeline_Batch1000", true, 1000},
	}

	for _, tt := range tests {
		b.Run(tt.name, func(b *testing.B) {
			benchmarkBatchWrite(b, tt.enablePipeline, tt.batchSize)
		})
	}
}

func benchmarkBatchWrite(b *testing.B, enableOptimization bool, batchSize int) {
	clearDir()

	opt := &Options{
		WorkDir:          "./bench_test",
		SSTableMaxSz:     64 << 20,
		MemTableSize:     16 << 20,
		ValueLogFileSize: 128 << 20,
		ValueThreshold:   1024,
		MaxBatchCount:    int64(batchSize * 2),
		MaxBatchSize:     32 << 20,
	}

	db := Open(opt)
	db.SetOptimizationEnabled(enableOptimization)
	defer db.Close()

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		entries := make([]*utils.Entry, batchSize)
		for j := 0; j < batchSize; j++ {
			key := fmt.Sprintf("batch_key_%d_%d", i, j)
			value := make([]byte, 1024)
			rand.Read(value)
			entries[j] = utils.NewEntry([]byte(key), value)
		}

		start := time.Now()
		if err := db.batchSet(entries); err != nil {
			b.Fatal(err)
		}

		if i == 0 {
			// 只在第一次迭代时报告批量写入延迟
			batchLatency := time.Since(start)
			b.ReportMetric(float64(batchLatency.Nanoseconds()), "batch_latency_ns")
		}
	}

	// 报告批量吞吐量
	totalOps := b.N * batchSize
	b.ReportMetric(float64(totalOps), "total_ops")
}

// BenchmarkMemoryUsage 测试内存使用情况
func BenchmarkMemoryUsage(b *testing.B) {
	clearDir()

	opt := &Options{
		WorkDir:          "./bench_test",
		SSTableMaxSz:     32 << 20,
		MemTableSize:     8 << 20,
		ValueLogFileSize: 64 << 20,
		ValueThreshold:   512,
		MaxBatchCount:    100,
		MaxBatchSize:     8 << 20,
	}

	db := Open(opt)
	db.SetOptimizationEnabled(true)
	defer db.Close()

	b.ResetTimer()

	// 写入大量数据测试内存使用
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("memory_test_key_%d", i)
		value := make([]byte, 2048)
		rand.Read(value)
		entry := utils.NewEntry([]byte(key), value)

		if err := db.Set(entry); err != nil {
			b.Fatal(err)
		}

		// 每1000次操作检查一次内存使用
		if i%1000 == 0 {
			// 这里可以添加内存使用统计
			// runtime.GC()
			// var m runtime.MemStats
			// runtime.ReadMemStats(&m)
			// b.Logf("Iteration %d: Alloc = %d KB", i, m.Alloc/1024)
		}
	}
}

// BenchmarkAllOptimizations 综合测试所有优化功能
func BenchmarkAllOptimizations(b *testing.B) {
	tests := []struct {
		name           string
		enablePipeline bool
		workload       string
	}{
		{"Original_WriteHeavy", false, "write_heavy"},
		{"Optimized_WriteHeavy", true, "write_heavy"},
		{"Original_ReadWrite", false, "read_write"},
		{"Optimized_ReadWrite", true, "read_write"},
		{"Original_BatchWrite", false, "batch_write"},
		{"Optimized_BatchWrite", true, "batch_write"},
	}

	for _, tt := range tests {
		b.Run(tt.name, func(b *testing.B) {
			benchmarkWorkload(b, tt.enablePipeline, tt.workload)
		})
	}
}

func benchmarkWorkload(b *testing.B, enableOptimization bool, workload string) {
	clearDir()

	opt := &Options{
		WorkDir:          "./bench_test",
		SSTableMaxSz:     64 << 20,
		MemTableSize:     16 << 20,
		ValueLogFileSize: 128 << 20,
		ValueThreshold:   1024,
		MaxBatchCount:    1000,
		MaxBatchSize:     16 << 20,
	}

	db := Open(opt)
	db.SetOptimizationEnabled(enableOptimization)
	defer db.Close()

	b.ResetTimer()

	switch workload {
	case "write_heavy":
		benchmarkWriteHeavyWorkload(b, db)
	case "read_write":
		benchmarkReadWriteWorkload(b, db)
	case "batch_write":
		benchmarkBatchWriteWorkload(b, db)
	}

	// 报告所有优化组件的统计信息
	if enableOptimization {
		allStats := db.GetAllOptimizationStats()
		b.Logf("Optimization stats: %+v", allStats)
	}
}

func benchmarkWriteHeavyWorkload(b *testing.B, db *DB) {
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("write_key_%d", i)
		value := make([]byte, 1024)
		rand.Read(value)
		entry := utils.NewEntry([]byte(key), value)

		if err := db.Set(entry); err != nil {
			b.Fatal(err)
		}
	}
}

func benchmarkReadWriteWorkload(b *testing.B, db *DB) {
	// 先写入一些数据
	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("rw_key_%d", i)
		value := make([]byte, 1024)
		rand.Read(value)
		entry := utils.NewEntry([]byte(key), value)
		db.Set(entry)
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if i%3 == 0 {
			// 读操作
			key := fmt.Sprintf("rw_key_%d", rand.Intn(1000))
			_, err := db.Get([]byte(key))
			if err != nil && err != utils.ErrKeyNotFound {
				b.Fatal(err)
			}
		} else {
			// 写操作
			key := fmt.Sprintf("rw_key_%d", rand.Intn(2000))
			value := make([]byte, 1024)
			rand.Read(value)
			entry := utils.NewEntry([]byte(key), value)

			if err := db.Set(entry); err != nil {
				b.Fatal(err)
			}
		}
	}
}

func benchmarkBatchWriteWorkload(b *testing.B, db *DB) {
	batchSize := 100

	for i := 0; i < b.N; i += batchSize {
		entries := make([]*utils.Entry, batchSize)
		for j := 0; j < batchSize; j++ {
			key := fmt.Sprintf("batch_key_%d_%d", i, j)
			value := make([]byte, 1024)
			rand.Read(value)
			entries[j] = utils.NewEntry([]byte(key), value)
		}

		if err := db.BatchSet(entries); err != nil {
			b.Fatal(err)
		}
	}
}
