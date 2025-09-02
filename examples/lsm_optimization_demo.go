/*
LSM引擎优化功能演示程序

本程序展示了LSM引擎的各种优化功能，包括：
1. 内存表引用计数管理
2. 智能压缩策略
3. 性能监控和统计
4. 并发安全的关闭机制

使用方法：
go run examples/lsm_optimization_demo.go
*/

package main

import (
	"fmt"
	"log"
	"os"
	"time"

	"github.com/util6/JadeDB/lsm"
	"github.com/util6/JadeDB/utils"
)

func main() {
	fmt.Println("=== LSM引擎优化功能演示 ===")

	// 创建临时工作目录
	workDir := "./demo_work"
	if err := os.MkdirAll(workDir, 0755); err != nil {
		log.Fatal("创建工作目录失败:", err)
	}
	defer os.RemoveAll(workDir) // 清理临时目录

	// 配置LSM选项
	lsmOpt := lsm.NewDefaultOptions()
	lsmOpt.WorkDir = workDir
	lsmOpt.MemTableSize = 1024 * 1024 // 1MB内存表
	lsmOpt.MaxImmutableTables = 4     // 最多4个不可变内存表
	lsmOpt.NumCompactors = 2          // 2个压缩器

	// 创建LSM引擎配置
	config := &lsm.LSMEngineConfig{
		LSMOptions:         lsmOpt,
		WriteBufferSize:    64 * 1024 * 1024, // 64MB
		BloomFilterBits:    10,
		CompressionType:    "snappy",
		MaxLevels:          7,
		CompactionStrategy: "leveled",
		CompactionThreads:  2,
	}

	fmt.Printf("配置信息:\n")
	fmt.Printf("- 工作目录: %s\n", lsmOpt.WorkDir)
	fmt.Printf("- 内存表大小: %d bytes\n", lsmOpt.MemTableSize)
	fmt.Printf("- 最大不可变表数量: %d\n", lsmOpt.MaxImmutableTables)
	fmt.Printf("- 压缩器数量: %d\n", lsmOpt.NumCompactors)

	// 创建LSM引擎实例
	lsmEngine, err := lsm.NewLSMEngine(config)
	if err != nil {
		log.Fatal("创建LSM引擎失败:", err)
	}

	// 打开LSM引擎
	if err := lsmEngine.Open(); err != nil {
		log.Fatal("打开LSM引擎失败:", err)
	}
	defer func() {
		// 安全关闭，忽略可能的重复关闭错误
		if err := lsmEngine.Close(); err != nil {
			log.Printf("关闭LSM引擎时出现错误（可能已关闭）: %v", err)
		}
	}()

	// 获取底层LSM树实例
	lsmTree := lsmEngine.GetLSMTree()
	if lsmTree == nil {
		log.Fatal("获取LSM树实例失败")
	}

	fmt.Println("\n=== 1. 内存表引用计数演示 ===")
	demonstrateRefCounting(lsmTree)

	fmt.Println("\n=== 2. 批量写入和压缩演示 ===")
	demonstrateBatchWriteAndCompaction(lsmTree)

	fmt.Println("\n=== 3. 性能统计演示 ===")
	demonstratePerformanceStats(lsmTree)

	fmt.Println("\n=== 4. 并发安全关闭演示 ===")
	demonstrateSafeClose(lsmTree)

	// 注意：这里演示了LSM树的关闭，但实际的引擎关闭由defer语句处理

	fmt.Println("\n=== 演示完成 ===")
}

// demonstrateRefCounting 演示内存表引用计数功能
func demonstrateRefCounting(lsmTree *lsm.LSM) {
	fmt.Println("写入数据触发内存表轮转...")

	// 写入足够的数据触发内存表轮转
	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("key%08d", i) // 使用固定长度的键
		value := fmt.Sprintf("value%08d_with_some_extra_data_to_fill_space", i)

		entry := &utils.Entry{
			Key:   []byte(key),
			Value: []byte(value),
		}

		if err := lsmTree.Set(entry); err != nil {
			log.Printf("写入失败 %s: %v", key, err)
		}

		// 每100个条目检查一次统计信息
		if i%100 == 0 {
			stats := lsmTree.GetStats()
			fmt.Printf("进度 %d/1000 - 不可变表数量: %v\n", i, stats["immutable_count"])
		}
	}

	// 显示最终统计信息
	stats := lsmTree.GetStats()
	fmt.Printf("最终统计:\n")
	fmt.Printf("- 内存表大小: %v bytes\n", stats["memtable_size"])
	fmt.Printf("- 内存表引用计数: %v\n", stats["memtable_ref_count"])
	fmt.Printf("- 不可变表数量: %v\n", stats["immutable_count"])
	fmt.Printf("- 不可变表总大小: %v bytes\n", stats["immutable_total_size"])
}

// demonstrateBatchWriteAndCompaction 演示批量写入和压缩功能
func demonstrateBatchWriteAndCompaction(lsmTree *lsm.LSM) {
	fmt.Println("执行批量写入操作...")

	// 批量写入更多数据
	batchSize := 500
	for batch := 0; batch < 5; batch++ {
		fmt.Printf("批次 %d/%d...\n", batch+1, 5)

		for i := 0; i < batchSize; i++ {
			key := fmt.Sprintf("batch%02d_key%08d", batch, i)
			value := fmt.Sprintf("batch%02d_value%08d_with_extended_content_for_testing", batch, i)

			entry := &utils.Entry{
				Key:   []byte(key),
				Value: []byte(value),
			}

			if err := lsmTree.Set(entry); err != nil {
				log.Printf("批量写入失败 %s: %v", key, err)
			}
		}

		// 等待压缩过程
		time.Sleep(100 * time.Millisecond)
	}

	// 显示层级统计信息
	stats := lsmTree.GetStats()
	if levels, ok := stats["levels"].(map[string]interface{}); ok {
		fmt.Println("层级统计信息:")
		for levelName, levelInfo := range levels {
			if info, ok := levelInfo.(map[string]interface{}); ok {
				fmt.Printf("- %s: 表数量=%v, 总大小=%v bytes\n",
					levelName, info["table_count"], info["total_size"])
			}
		}
	}
}

// demonstratePerformanceStats 演示性能统计功能
func demonstratePerformanceStats(lsmTree *lsm.LSM) {
	fmt.Println("收集性能统计信息...")

	// 执行一些读取操作
	readCount := 100
	successCount := 0

	start := time.Now()
	for i := 0; i < readCount; i++ {
		key := fmt.Sprintf("key%08d", i)
		if _, err := lsmTree.Get([]byte(key)); err == nil {
			successCount++
		}
	}
	readDuration := time.Since(start)

	fmt.Printf("读取性能测试结果:\n")
	fmt.Printf("- 总读取次数: %d\n", readCount)
	fmt.Printf("- 成功读取次数: %d\n", successCount)
	fmt.Printf("- 总耗时: %v\n", readDuration)
	fmt.Printf("- 平均延迟: %v\n", readDuration/time.Duration(readCount))

	// 显示详细统计信息
	stats := lsmTree.GetStats()
	fmt.Println("\n详细统计信息:")

	// 显示管道统计
	if pipeline, ok := stats["pipeline"].(map[string]interface{}); ok {
		fmt.Println("管道统计:")
		for key, value := range pipeline {
			fmt.Printf("  - %s: %v\n", key, value)
		}
	}

	// 显示合并器统计
	if coalescer, ok := stats["coalescer"].(map[string]interface{}); ok {
		fmt.Println("合并器统计:")
		for key, value := range coalescer {
			fmt.Printf("  - %s: %v\n", key, value)
		}
	}
}

// demonstrateSafeClose 演示并发安全的关闭机制
func demonstrateSafeClose(lsmTree *lsm.LSM) {
	fmt.Println("测试并发安全关闭...")

	// 使用context来控制协程的停止
	stopChan := make(chan struct{})
	done := make(chan bool, 3)

	// 并发写入协程1
	go func() {
		defer func() { done <- true }()
		for i := 0; i < 50; i++ {
			select {
			case <-stopChan:
				return
			default:
			}

			key := fmt.Sprintf("concurrent1_key%08d", i)
			value := fmt.Sprintf("concurrent1_value%08d", i)

			entry := &utils.Entry{
				Key:   []byte(key),
				Value: []byte(value),
			}

			// 忽略错误，因为可能在关闭过程中
			lsmTree.Set(entry)
			time.Sleep(1 * time.Millisecond)
		}
	}()

	// 并发写入协程2
	go func() {
		defer func() { done <- true }()
		for i := 0; i < 50; i++ {
			select {
			case <-stopChan:
				return
			default:
			}

			key := fmt.Sprintf("concurrent2_key%08d", i)
			value := fmt.Sprintf("concurrent2_value%08d", i)

			entry := &utils.Entry{
				Key:   []byte(key),
				Value: []byte(value),
			}

			// 忽略错误，因为可能在关闭过程中
			lsmTree.Set(entry)
			time.Sleep(1 * time.Millisecond)
		}
	}()

	// 并发读取协程
	go func() {
		defer func() { done <- true }()
		for i := 0; i < 50; i++ {
			select {
			case <-stopChan:
				return
			default:
			}

			key := fmt.Sprintf("key%08d", i%100)
			// 忽略错误，因为可能在关闭过程中
			lsmTree.Get([]byte(key))
			time.Sleep(1 * time.Millisecond)
		}
	}()

	// 等待一段时间后开始关闭流程
	time.Sleep(20 * time.Millisecond)

	fmt.Println("开始关闭LSM树...")

	// 首先停止所有协程
	close(stopChan)

	// 等待协程停止
	for i := 0; i < 3; i++ {
		<-done
	}

	// 模拟关闭过程（实际关闭由引擎管理）
	start := time.Now()
	fmt.Printf("模拟LSM树关闭过程，耗时: %v\n", time.Since(start))
	fmt.Println("注意：实际的LSM树关闭由LSMEngine统一管理")

	fmt.Println("所有并发操作已完成")
}
