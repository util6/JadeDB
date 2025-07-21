//go:build windows
// +build windows

package mmap

import (
	"os"
	"unsafe"

	"golang.org/x/sys/windows"
)

// mmap 用于内存映射文件访问。
// 该函数根据文件描述符 fd、是否可写 writable 和文件大小 size 来创建一个内存映射。
// 如果创建成功，返回内存映射的字节切片和 nil 错误；如果失败，返回 nil 切片和相应的错误。
func mmap(fd *os.File, writable bool, size int64) ([]byte, error) {
	// 根据内存映射是否需要写入权限来设置保护属性。
	var prot uint32
	if writable {
		prot = windows.PAGE_READWRITE
	} else {
		prot = windows.PAGE_READONLY
	}

	// 获取文件句柄。
	hFile := windows.Handle(fd.Fd())

	// 创建文件映射对象。
	hMap, err := windows.CreateFileMapping(hFile, nil, prot, 0, 0, nil)
	if err != nil {
		return nil, err
	}
	// 确保在函数返回前关闭文件映射对象。
	defer windows.CloseHandle(hMap)

	// 映射文件到内存。
	ptr, err := windows.MapViewOfFile(hMap, windows.FILE_MAP_READ, 0, 0, 0)
	if err != nil {
		return nil, err
	}

	// 创建切片
	b := (*[1 << 30]byte)(unsafe.Pointer(ptr))[:size]
	return b, nil
}

// Munmap unmaps a previously mapped slice.
func munmap(b []byte) error {
	// 使用原始的 ptr
	ptr := unsafe.Pointer(&b[0])
	if err := windows.UnmapViewOfFile(uintptr(ptr)); err != nil {
		return err
	}
	return nil
}

// This is required because the unix package does not support the madvise system call on OS X.
// On Windows, there is no direct equivalent to madvise, so we can return nil.
func madvise(b []byte, readahead bool) error {
	// Windows does not have an equivalent to madvise.
	return nil
}

// Msync ensures that the data in the memory-mapped region has been written to disk.
func msync(b []byte) error {
	// 使用原始的 ptr
	ptr := unsafe.Pointer(&b[0])
	if err := windows.FlushViewOfFile(uintptr(ptr), uintptr(len(b))); err != nil {
		return err
	}
	return nil
}
