/*
JadeDB 引擎切换示例

本示例展示了如何使用JadeDB的多引擎支持和无感切换功能。
包括：
1. 创建支持多引擎的数据库实例
2. 在LSM树和B+树引擎之间切换
3. 数据迁移和性能对比
4. 引擎统计信息查看
*/

package main

import (
	"fmt"
	"log"
	"time"

	"github.com/util6/JadeDB"
	"github.com/util6/JadeDB/utils"
)

func main() {
	// 示例1：传统模式（仅LSM引擎）
	fmt.Println("=== 传统模式示例 ===")
	runLegacyModeExample()

	// 示例2：适配器模式（支持引擎切换）
	fmt.Println("\n=== 适配器模式示例 ===")
	runAdapterModeExample()

	// 示例3：性能对比
	fmt.Println("\n=== 性能对比示例 ===")
	runPerformanceComparison()
}

// runLegacyModeExample 传统模式示例
func runLegacyModeExample() {
	// 创建传统模式的数据库实例
	opt := &JadeDB.Options{
		WorkDir:      "./data/legacy",
		MemTableSize: 64 << 20, // 64MB
		SSTableMaxSz: 64 << 20, // 64MB
	}

	db := JadeDB.Open(opt)
	defer db.Close()

	// 写入一些测试数据
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("key_%d", i)
		value := fmt.Sprintf("value_%d", i)

		err := db.Set(&utils.Entry{
			Key:   []byte(key),
			Value: []byte(value),
		})
		if err != nil {
			log.Printf("Failed to set %s: %v", key, err)
		}
	}

	// 读取数据
	entry, err := db.Get([]byte("key_50"))
	if err != nil {
		log.Printf("Failed to get key_50: %v", err)
	} else {
		fmt.Printf("传统模式读取结果: %s = %s\n", entry.Key, entry.Value)
	}

	// 查看引擎信息
	fmt.Printf("当前引擎类型: %v\n", db.GetCurrentEngineType())
	fmt.Printf("引擎模式: %v\n", db.GetEngineMode())
}

// runAdapterModeExample 适配器模式示例
func runAdapterModeExample() {
	// 创建适配器模式的数据库实例，初始使用LSM引擎
	opt := &JadeDB.Options{
		WorkDir:      "./data/adapter",
		MemTableSize: 64 << 20, // 64MB
		SSTableMaxSz: 64 << 20, // 64MB
	}

	db := JadeDB.OpenWithEngine(opt, JadeDB.AdapterMode, JadeDB.LSMEngine)
	defer db.Close()

	// 写入一些测试数据
	fmt.Println("使用LSM引擎写入数据...")
	for i := 0; i < 50; i++ {
		key := fmt.Sprintf("lsm_key_%d", i)
		value := fmt.Sprintf("lsm_value_%d", i)

		err := db.Set(&utils.Entry{
			Key:   []byte(key),
			Value: []byte(value),
		})
		if err != nil {
			log.Printf("Failed to set %s: %v", key, err)
		}
	}

	// 查看当前引擎状态
	fmt.Printf("当前引擎: %v\n", db.GetCurrentEngineType())

	// 切换到B+树引擎
	fmt.Println("切换到B+树引擎...")
	err := db.SwitchEngine(JadeDB.BTreeEngine)
	if err != nil {
		log.Printf("Failed to switch engine: %v", err)
		return
	}

	fmt.Printf("切换后引擎: %v\n", db.GetCurrentEngineType())

	// 验证数据是否迁移成功
	entry, err := db.Get([]byte("lsm_key_25"))
	if err != nil {
		log.Printf("Failed to get migrated data: %v", err)
	} else {
		fmt.Printf("数据迁移验证: %s = %s\n", entry.Key, entry.Value)
	}

	// 使用B+树引擎写入新数据
	fmt.Println("使用B+树引擎写入新数据...")
	for i := 0; i < 50; i++ {
		key := fmt.Sprintf("btree_key_%d", i)
		value := fmt.Sprintf("btree_value_%d", i)

		err := db.Set(&utils.Entry{
			Key:   []byte(key),
			Value: []byte(value),
		})
		if err != nil {
			log.Printf("Failed to set %s: %v", key, err)
		}
	}

	// 再次切换回LSM引擎
	fmt.Println("切换回LSM引擎...")
	err = db.SwitchEngine(JadeDB.LSMEngine)
	if err != nil {
		log.Printf("Failed to switch back to LSM: %v", err)
		return
	}

	// 验证所有数据都可访问
	fmt.Println("验证数据完整性...")
	testKeys := []string{"lsm_key_10", "btree_key_10"}
	for _, key := range testKeys {
		entry, err := db.Get([]byte(key))
		if err != nil {
			log.Printf("Failed to get %s: %v", key, err)
		} else {
			fmt.Printf("数据验证: %s = %s\n", entry.Key, entry.Value)
		}
	}

	// 查看引擎统计信息
	stats := db.GetEngineStats()
	fmt.Println("引擎统计信息:")
	for key, value := range stats {
		fmt.Printf("  %s: %v\n", key, value)
	}
}

// runPerformanceComparison 性能对比示例
func runPerformanceComparison() {
	opt := &JadeDB.Options{
		WorkDir:      "./data/perf",
		MemTableSize: 64 << 20, // 64MB
		SSTableMaxSz: 64 << 20, // 64MB
	}

	db := JadeDB.OpenWithEngine(opt, JadeDB.AdapterMode, JadeDB.LSMEngine)
	defer db.Close()

	// 测试数据量
	testCount := 1000

	// LSM引擎性能测试
	fmt.Println("LSM引擎性能测试...")
	lsmWriteTime := benchmarkWrites(db, "lsm", testCount)
	lsmReadTime := benchmarkReads(db, "lsm", testCount)

	// 切换到B+树引擎
	fmt.Println("切换到B+树引擎...")
	err := db.SwitchEngine(JadeDB.BTreeEngine)
	if err != nil {
		log.Printf("Failed to switch to B+Tree: %v", err)
		return
	}

	// B+树引擎性能测试
	fmt.Println("B+树引擎性能测试...")
	btreeWriteTime := benchmarkWrites(db, "btree", testCount)
	btreeReadTime := benchmarkReads(db, "btree", testCount)

	// 性能对比结果
	fmt.Println("\n=== 性能对比结果 ===")
	fmt.Printf("写入性能:\n")
	fmt.Printf("  LSM引擎:  %v (%v/op)\n", lsmWriteTime, lsmWriteTime/time.Duration(testCount))
	fmt.Printf("  B+树引擎: %v (%v/op)\n", btreeWriteTime, btreeWriteTime/time.Duration(testCount))

	fmt.Printf("读取性能:\n")
	fmt.Printf("  LSM引擎:  %v (%v/op)\n", lsmReadTime, lsmReadTime/time.Duration(testCount))
	fmt.Printf("  B+树引擎: %v (%v/op)\n", btreeReadTime, btreeReadTime/time.Duration(testCount))

	// 列出可用引擎
	fmt.Println("\n可用的存储引擎:")
	engines := db.ListAvailableEngines()
	for _, engine := range engines {
		info, err := db.GetEngineInfo(engine)
		if err != nil {
			fmt.Printf("  %v: (无法获取信息)\n", engine)
		} else {
			fmt.Printf("  %v: %s\n", engine, info.Description)
		}
	}
}

// benchmarkWrites 写入性能测试
func benchmarkWrites(db *JadeDB.DB, prefix string, count int) time.Duration {
	start := time.Now()

	for i := 0; i < count; i++ {
		key := fmt.Sprintf("%s_key_%d", prefix, i)
		value := fmt.Sprintf("%s_value_%d_with_some_longer_content_to_test_performance", prefix, i)

		err := db.Set(&utils.Entry{
			Key:   []byte(key),
			Value: []byte(value),
		})
		if err != nil {
			log.Printf("Write error: %v", err)
		}
	}

	return time.Since(start)
}

// benchmarkReads 读取性能测试
func benchmarkReads(db *JadeDB.DB, prefix string, count int) time.Duration {
	start := time.Now()

	for i := 0; i < count; i++ {
		key := fmt.Sprintf("%s_key_%d", prefix, i)

		_, err := db.Get([]byte(key))
		if err != nil {
			log.Printf("Read error for %s: %v", key, err)
		}
	}

	return time.Since(start)
}
