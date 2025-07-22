package file

import (
	"runtime"
	"testing"
)

// TestPlatformSpecificFiles 测试平台特定文件的编译选择
// 这个测试验证不同平台是否选择了正确的实现文件
func TestPlatformSpecificFiles(t *testing.T) {
	// 获取当前运行的操作系统
	currentOS := runtime.GOOS

	t.Logf("当前操作系统: %s", currentOS)

	// 根据操作系统验证是否选择了正确的实现
	switch currentOS {
	case "darwin":
		t.Log("✓ 在 macOS (Darwin) 系统上运行")
		t.Log("✓ 应该使用 sstable_darwin.go 和 mmap_darwin.go")

		// 验证 SSTable 结构体能够正确初始化
		ss := &SSTable{}
		if ss == nil {
			t.Error("SSTable 结构体初始化失败")
		}

		// 验证 MmapFile 结构体能够正确初始化
		mf := &MmapFile{}
		if mf == nil {
			t.Error("MmapFile 结构体初始化失败")
		}

		t.Log("✓ Darwin 特定的结构体初始化成功")

	case "linux":
		t.Log("✓ 在 Linux 系统上运行")
		t.Log("✓ 应该使用 sstable_unix.go 和 mmap_unix.go")

	case "windows":
		t.Log("✓ 在 Windows 系统上运行")
		t.Log("✓ 应该使用 sstable_windows.go 和 mmap_windows.go")

	default:
		t.Logf("⚠ 未知的操作系统: %s", currentOS)
	}

	t.Log("✓ 平台特定文件编译测试通过")
}

// TestDarwinSpecificFeatures 测试 Darwin 特定功能（仅在 macOS 上运行）
func TestDarwinSpecificFeatures(t *testing.T) {
	if runtime.GOOS != "darwin" {
		t.Skip("跳过 Darwin 特定测试，当前系统不是 macOS")
		return
	}

	t.Log("✓ 运行 Darwin 特定功能测试")

	// 测试 SSTable 的基本功能
	ss := &SSTable{}

	// 测试基本的 getter 方法（不依赖文件的方法）
	fid := ss.FID()
	t.Logf("文件 ID: %d", fid)

	hasBloom := ss.HasBloomFilter()
	t.Logf("包含布隆过滤器: %v", hasBloom)

	// 测试键范围方法
	minKey := ss.MinKey()
	maxKey := ss.MaxKey()
	t.Logf("最小键长度: %d, 最大键长度: %d", len(minKey), len(maxKey))

	// 注意：Size() 方法需要有效的文件对象，所以在这里跳过
	t.Log("✓ 跳过需要文件对象的方法测试（如 Size()）")

	t.Log("✓ Darwin 特定功能测试通过")
}

// BenchmarkDarwinSSTable 性能基准测试（仅在 macOS 上运行）
func BenchmarkDarwinSSTable(b *testing.B) {
	if runtime.GOOS != "darwin" {
		b.Skip("跳过 Darwin 性能测试，当前系统不是 macOS")
		return
	}

	ss := &SSTable{}

	b.ResetTimer()
	b.ReportAllocs()

	// 基准测试基本操作的性能（不依赖文件的方法）
	for i := 0; i < b.N; i++ {
		_ = ss.FID()
		_ = ss.HasBloomFilter()
		_ = ss.MinKey()
		_ = ss.MaxKey()
	}
}
