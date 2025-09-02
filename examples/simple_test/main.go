package main

import (
	"fmt"
	"os"

	"github.com/util6/JadeDB"
	"github.com/util6/JadeDB/utils"
)

func main() {
	fmt.Println("=== JadeDB 简单功能测试 ===")

	// 清理测试目录
	testDir := "./simple_test_data"
	os.RemoveAll(testDir)
	defer os.RemoveAll(testDir)

	// 测试传统模式
	fmt.Println("\n1. 测试传统模式")
	testLegacy(testDir + "/legacy")

	fmt.Println("\n=== 测试完成 ===")
}

func testLegacy(dir string) {
	fmt.Printf("创建数据库目录: %s\n", dir)

	opt := &JadeDB.Options{
		WorkDir:      dir,
		MemTableSize: 1 << 20, // 1MB
		SSTableMaxSz: 1 << 20, // 1MB
	}

	fmt.Println("打开数据库...")
	db := JadeDB.Open(opt)
	defer db.Close()

	fmt.Printf("引擎模式: %v\n", db.GetEngineMode())
	fmt.Printf("引擎类型: %v\n", db.GetCurrentEngineType())

	// 写入数据
	fmt.Println("写入测试数据...")
	err := db.Set(&utils.Entry{
		Key:   []byte("hello"),
		Value: []byte("world"),
	})
	if err != nil {
		fmt.Printf("写入失败: %v\n", err)
		return
	}

	// 读取数据
	fmt.Println("读取测试数据...")
	entry, err := db.Get([]byte("hello"))
	if err != nil {
		fmt.Printf("读取失败: %v\n", err)
		return
	}

	fmt.Printf("读取成功: %s = %s\n", entry.Key, entry.Value)
}
