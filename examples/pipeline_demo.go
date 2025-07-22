package main

import (
	"fmt"
	"log"
	"math/rand"
	"os"
	"runtime"
	"sync"
	"time"

	"github.com/util6/JadeDB"
	"github.com/util6/JadeDB/utils"
)

func main() {
	fmt.Println("JadeDB PebbleDB优化演示")
	fmt.Println("========================")

	// 清理测试目录
	os.RemoveAll("./demo_test")

	// 测试配置
	opt := &JadeDB.Options{
		WorkDir:          "./demo_test",
		SSTableMaxSz:     32 << 20, // 32MB
		MemTableSize:     8 << 20,  // 8MB
		ValueLogFileSize: 64 << 20, // 64MB
		ValueThreshold:   1024,
		MaxBatchCount:    100,
		MaxBatchSize:     8 << 20, // 8MB
	}

	// 对比测试：原始实现 vs 管道优化
	fmt.Println("\n1. 写入性能对比测试")
	fmt.Println("-------------------")

	// 测试原始实现
	fmt.Println("测试原始实现...")
	originalThroughput := testWritePerformance(opt, false, 10000)

	// 清理
	os.RemoveAll("./demo_test")

	// 测试管道优化
	fmt.Println("测试管道优化...")
	pipelineThroughput := testWritePerformance(opt, true, 10000)

	// 输出对比结果
	fmt.Printf("\n性能对比结果:\n")
	fmt.Printf("原始实现吞吐量: %.2f ops/sec\n", originalThroughput)
	fmt.Printf("管道优化吞吐量: %.2f ops/sec\n", pipelineThroughput)
	fmt.Printf("性能提升: %.2fx\n", pipelineThroughput/originalThroughput)

	// 清理
	os.RemoveAll("./demo_test")

	// 延迟测试
	fmt.Println("\n2. 写入延迟对比测试")
	fmt.Println("-------------------")

	originalLatency := testWriteLatency(opt, false, 1000)
	os.RemoveAll("./demo_test")
	pipelineLatency := testWriteLatency(opt, true, 1000)

	fmt.Printf("\n延迟对比结果:\n")
	fmt.Printf("原始实现平均延迟: %.2f ms\n", originalLatency.Seconds()*1000)
	fmt.Printf("管道优化平均延迟: %.2f ms\n", pipelineLatency.Seconds()*1000)
	fmt.Printf("延迟改善: %.2fx\n", originalLatency.Seconds()/pipelineLatency.Seconds())

	// 清理
	os.RemoveAll("./demo_test")

	// 批量写入测试
	fmt.Println("\n3. 批量写入优化演示")
	fmt.Println("-------------------")

	demonstrateBatchOptimization(opt)

	// 清理
	os.RemoveAll("./demo_test")

	// 内存优化演示
	fmt.Println("\n4. 内存优化演示")
	fmt.Println("---------------")

	demonstrateMemoryOptimization(opt)

	// 清理
	os.RemoveAll("./demo_test")

	fmt.Println("\n演示完成!")
}

// testWritePerformance 测试写入性能
func testWritePerformance(opt *JadeDB.Options, enableOptimization bool, numOps int) float64 {
	db := JadeDB.Open(opt)
	db.SetOptimizationEnabled(enableOptimization)
	defer db.Close()

	// 生成测试数据
	entries := make([]*utils.Entry, numOps)
	for i := 0; i < numOps; i++ {
		key := fmt.Sprintf("perf_key_%d", i)
		value := make([]byte, 1024)
		rand.Read(value)
		entries[i] = utils.NewEntry([]byte(key), value)
	}

	// 执行写入测试
	start := time.Now()

	var wg sync.WaitGroup
	concurrency := 10
	semaphore := make(chan struct{}, concurrency)

	for i := 0; i < numOps; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			semaphore <- struct{}{}
			defer func() { <-semaphore }()

			if err := db.Set(entries[idx]); err != nil {
				log.Printf("写入错误: %v", err)
			}
		}(i)
	}

	wg.Wait()
	duration := time.Since(start)

	throughput := float64(numOps) / duration.Seconds()
	return throughput
}

// testWriteLatency 测试写入延迟
func testWriteLatency(opt *JadeDB.Options, enableOptimization bool, numOps int) time.Duration {
	db := JadeDB.Open(opt)
	db.SetOptimizationEnabled(enableOptimization)
	defer db.Close()

	// 预热
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("warmup_%d", i)
		value := make([]byte, 1024)
		entry := utils.NewEntry([]byte(key), value)
		db.Set(entry)
	}

	// 延迟测试
	var totalLatency time.Duration

	for i := 0; i < numOps; i++ {
		key := fmt.Sprintf("latency_key_%d", i)
		value := make([]byte, 1024)
		rand.Read(value)
		entry := utils.NewEntry([]byte(key), value)

		start := time.Now()
		if err := db.Set(entry); err != nil {
			log.Printf("写入错误: %v", err)
			continue
		}
		totalLatency += time.Since(start)
	}

	return totalLatency / time.Duration(numOps)
}

// demonstrateBatchOptimization 演示批量写入优化
func demonstrateBatchOptimization(opt *JadeDB.Options) {
	db := JadeDB.Open(opt)
	db.SetOptimizationEnabled(true)
	defer db.Close()

	batchSizes := []int{1, 10, 100, 1000}

	for _, batchSize := range batchSizes {
		fmt.Printf("测试批量大小: %d\n", batchSize)

		// 生成批量数据
		entries := make([]*utils.Entry, batchSize)
		for i := 0; i < batchSize; i++ {
			key := fmt.Sprintf("batch_%d_key_%d", batchSize, i)
			value := make([]byte, 1024)
			rand.Read(value)
			entries[i] = utils.NewEntry([]byte(key), value)
		}

		// 测试批量写入
		start := time.Now()

		if batchSize == 1 {
			// 单个写入
			if err := db.Set(entries[0]); err != nil {
				log.Printf("写入错误: %v", err)
			}
		} else {
			// 批量写入
			if err := db.BatchSet(entries); err != nil { // 假设我们有这个方法
				log.Printf("批量写入错误: %v", err)
			}
		}

		duration := time.Since(start)
		throughput := float64(batchSize) / duration.Seconds()

		fmt.Printf("  批量大小: %d, 耗时: %v, 吞吐量: %.2f ops/sec\n",
			batchSize, duration, throughput)
	}

	// 显示管道统计信息
	if stats := db.GetPipelineStats(); stats != nil { // 假设我们有这个方法
		fmt.Printf("\n管道统计信息:\n")
		for key, value := range stats {
			fmt.Printf("  %s: %v\n", key, value)
		}
	}

	// 显示合并器统计信息
	if stats := db.GetCoalescerStats(); stats != nil { // 假设我们有这个方法
		fmt.Printf("\n合并器统计信息:\n")
		for key, value := range stats {
			fmt.Printf("  %s: %v\n", key, value)
		}
	}

}

// 为了演示，我们需要在DB结构中添加这些方法
// 这些方法应该在实际的db.go文件中实现

/*
// SetPipelineEnabled 设置是否启用管道优化
func (db *DB) SetPipelineEnabled(enabled bool) {
	db.enablePipeline = enabled
	if enabled && db.pipeline == nil {
		db.pipeline = newCommitPipeline(db)
		db.coalescer = newBatchCoalescer(db)
	}
}

// BatchSet 批量设置键值对
func (db *DB) BatchSet(entries []*utils.Entry) error {
	req, err := db.sendToWriteCh(entries)
	if err != nil {
		return err
	}
	return req.Wait()
}

// GetPipelineStats 获取管道统计信息
func (db *DB) GetPipelineStats() map[string]interface{} {
	if db.pipeline != nil {
		return db.pipeline.getStats()
	}
	return nil
}

// GetCoalescerStats 获取合并器统计信息
func (db *DB) GetCoalescerStats() map[string]interface{} {
	if db.coalescer != nil {
		return db.coalescer.getStats()
	}
	return nil
}
*/

// demonstrateMemoryOptimization 演示内存优化功能
func demonstrateMemoryOptimization(opt *JadeDB.Options) {
	fmt.Println("测试内存优化效果...")

	// 测试对象池复用
	db := JadeDB.Open(opt)
	db.SetOptimizationEnabled(true)
	defer db.Close()

	// 大量小对象创建和销毁
	numOperations := 10000

	fmt.Printf("执行 %d 次小对象操作...\n", numOperations)
	start := time.Now()

	for i := 0; i < numOperations; i++ {
		key := fmt.Sprintf("mem_key_%d", i)
		value := make([]byte, 64) // 小对象
		rand.Read(value)
		entry := utils.NewEntry([]byte(key), value)

		if err := db.Set(entry); err != nil {
			log.Printf("写入错误: %v", err)
		}

		// 每1000次操作强制GC一次，测试对象池效果
		if i%1000 == 0 && i > 0 {
			runtime.GC()
		}
	}

	duration := time.Since(start)
	fmt.Printf("完成时间: %v\n", duration)
	fmt.Printf("平均每操作: %v\n", duration/time.Duration(numOperations))

	// 显示优化统计
	if allStats := db.GetAllOptimizationStats(); allStats != nil {
		fmt.Printf("\n优化统计:\n")
		for key, value := range allStats {
			fmt.Printf("  %s: %v\n", key, value)
		}
	}

	// 显示GC统计
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	fmt.Printf("\nGC统计:\n")
	fmt.Printf("  GC次数: %d\n", m.NumGC)
	fmt.Printf("  堆内存: %d KB\n", m.HeapAlloc/1024)
	fmt.Printf("  系统内存: %d KB\n", m.Sys/1024)
}
