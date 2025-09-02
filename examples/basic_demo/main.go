package main

import (
	"fmt"
	"os"

	"github.com/util6/JadeDB"
	"github.com/util6/JadeDB/utils"
)

func main() {
	fmt.Println("=== JadeDB 引擎功能演示 ===")

	// 创建测试目录
	testDir := "./demo_data"
	os.RemoveAll(testDir)
	os.MkdirAll(testDir, 0755)
	defer os.RemoveAll(testDir)

	// 1. 测试传统模式
	fmt.Println("\n1. 传统模式测试")
	testLegacyMode(testDir + "/legacy")

	// 2. 测试适配器模式
	fmt.Println("\n2. 适配器模式测试")
	testAdapterMode(testDir + "/adapter")

	fmt.Println("\n=== 演示完成 ===")
}

func testLegacyMode(dir string) {
	os.MkdirAll(dir, 0755)

	opt := &JadeDB.Options{
		WorkDir:      dir,
		MemTableSize: 1 << 20, // 1MB
		SSTableMaxSz: 1 << 20, // 1MB
	}

	fmt.Println("创建传统模式数据库...")
	db := JadeDB.Open(opt)
	defer db.Close()

	fmt.Printf("  引擎模式: %v\n", db.GetEngineMode())
	fmt.Printf("  引擎类型: %v\n", db.GetCurrentEngineType())

	// 列出可用引擎
	engines := db.ListAvailableEngines()
	fmt.Printf("  可用引擎: %v\n", engines)

	// 写入测试数据
	fmt.Println("写入测试数据...")
	err := db.Set(&utils.Entry{
		Key:   []byte("legacy_key"),
		Value: []byte("legacy_value"),
	})
	if err != nil {
		fmt.Printf("  写入失败: %v\n", err)
		return
	}

	// 读取测试数据
	entry, err := db.Get([]byte("legacy_key"))
	if err != nil {
		fmt.Printf("  读取失败: %v\n", err)
		return
	}

	fmt.Printf("  读取成功: %s = %s\n", entry.Key, entry.Value)

	// 尝试切换引擎（应该失败）
	fmt.Println("尝试切换引擎（应该失败）...")
	err = db.SwitchEngine(JadeDB.BTreeEngine)
	if err != nil {
		fmt.Printf("  预期的错误: %v\n", err)
	} else {
		fmt.Println("  意外：切换成功了")
	}
}

func testAdapterMode(dir string) {
	os.MkdirAll(dir, 0755)

	opt := &JadeDB.Options{
		WorkDir:      dir,
		MemTableSize: 1 << 20, // 1MB
		SSTableMaxSz: 1 << 20, // 1MB
	}

	fmt.Println("创建适配器模式数据库（LSM引擎）...")
	db := JadeDB.OpenWithEngine(opt, JadeDB.AdapterMode, JadeDB.LSMEngine)
	defer db.Close()

	fmt.Printf("  引擎模式: %v\n", db.GetEngineMode())
	fmt.Printf("  当前引擎: %v\n", db.GetCurrentEngineType())

	// 写入测试数据
	fmt.Println("写入测试数据...")
	for i := 0; i < 5; i++ {
		key := fmt.Sprintf("adapter_key_%d", i)
		value := fmt.Sprintf("adapter_value_%d", i)

		err := db.Set(&utils.Entry{
			Key:   []byte(key),
			Value: []byte(value),
		})
		if err != nil {
			fmt.Printf("  写入失败 %s: %v\n", key, err)
			return
		}
	}

	// 读取测试数据
	entry, err := db.Get([]byte("adapter_key_0"))
	if err != nil {
		fmt.Printf("  读取失败: %v\n", err)
		return
	}
	fmt.Printf("  LSM引擎读取: %s = %s\n", entry.Key, entry.Value)

	// 切换到B+树引擎
	fmt.Println("切换到B+树引擎...")
	err = db.SwitchEngine(JadeDB.BTreeEngine)
	if err != nil {
		fmt.Printf("  引擎切换失败: %v\n", err)
		return
	}

	fmt.Printf("  切换后引擎: %v\n", db.GetCurrentEngineType())

	// 验证数据迁移
	fmt.Println("验证数据迁移...")
	for i := 0; i < 5; i++ {
		key := fmt.Sprintf("adapter_key_%d", i)
		expectedValue := fmt.Sprintf("adapter_value_%d", i)

		entry, err := db.Get([]byte(key))
		if err != nil {
			fmt.Printf("  迁移后读取失败 %s: %v\n", key, err)
			return
		}

		if string(entry.Value) != expectedValue {
			fmt.Printf("  数据不匹配 %s: 期望 %s, 实际 %s\n", key, expectedValue, entry.Value)
			return
		}
	}
	fmt.Println("  数据迁移验证成功！")

	// 在B+树引擎上写入新数据
	fmt.Println("在B+树引擎上写入新数据...")
	err = db.Set(&utils.Entry{
		Key:   []byte("btree_key"),
		Value: []byte("btree_value"),
	})
	if err != nil {
		fmt.Printf("  B+树写入失败: %v\n", err)
		return
	}

	entry, err = db.Get([]byte("btree_key"))
	if err != nil {
		fmt.Printf("  B+树读取失败: %v\n", err)
		return
	}
	fmt.Printf("  B+树引擎读取: %s = %s\n", entry.Key, entry.Value)

	// 切换回LSM引擎
	fmt.Println("切换回LSM引擎...")
	err = db.SwitchEngine(JadeDB.LSMEngine)
	if err != nil {
		fmt.Printf("  切换回LSM失败: %v\n", err)
		return
	}

	fmt.Printf("  切换后引擎: %v\n", db.GetCurrentEngineType())

	// 验证所有数据都可访问
	fmt.Println("验证所有数据可访问...")
	testKeys := []string{"adapter_key_0", "adapter_key_4", "btree_key"}
	for _, key := range testKeys {
		entry, err := db.Get([]byte(key))
		if err != nil {
			fmt.Printf("  读取失败 %s: %v\n", key, err)
			return
		}
		fmt.Printf("  验证成功: %s = %s\n", key, entry.Value)
	}

	// 显示引擎统计信息
	fmt.Println("引擎统计信息:")
	stats := db.GetEngineStats()
	for key, value := range stats {
		fmt.Printf("  %s: %v\n", key, value)
	}
}
