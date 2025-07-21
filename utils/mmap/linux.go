//go:build linux
// +build linux

package mmap

import (
	"os"
	"reflect"
	"unsafe"

	"golang.org/x/sys/unix"
)

// mmap uses the mmap system call to memory-map a file. If writable is true,
// memory protection of the pages is set so that they may be written to as well.
func mmap(fd *os.File, writable bool, size int64) ([]byte, error) {
	// 设置内存映射的保护类型，默认为只读
	mtype := unix.PROT_READ

	// 如果 writable 为 true，则设置为可读写
	if writable {
		mtype |= unix.PROT_WRITE
	}

	// 调用 unix.Mmap 系统调用，将文件描述符 fd 指向的文件映射到内存中
	// 参数说明：
	// - int(fd.Fd())：文件描述符
	// - 0：偏移量，从文件开头开始映射
	// - int(size)：映射的字节数
	// - mtype：保护类型，可读或可读写
	// - unix.MAP_SHARED：共享映射，对映射区域的修改会反映到文件中
	return unix.Mmap(int(fd.Fd()), 0, int(size), mtype, unix.MAP_SHARED)
}

// mremap is a Linux-specific system call to remap pages in memory. This can be used in place of munmap + mmap.
func mremap(data []byte, size int) ([]byte, error) {
	// taken from <https://github.com/torvalds/linux/blob/f8394f232b1eab649ce2df5c5f15b0e528c92091/include/uapi/linux/mman.h#L8>
	const MREMAP_MAYMOVE = 0x1

	header := (*reflect.SliceHeader)(unsafe.Pointer(&data))
	mmapAddr, _, errno := unix.Syscall6(
		unix.SYS_MREMAP,
		header.Data,
		uintptr(header.Len),
		uintptr(size),
		uintptr(MREMAP_MAYMOVE),
		0,
		0,
	)
	if errno != 0 {
		return nil, errno
	}

	header.Data = mmapAddr
	header.Cap = size
	header.Len = size
	return data, nil
}

// munmap unmaps a previously mapped slice.
//
// unix.Munmap maintains an internal list of mmapped addresses, and only calls munmap
// if the address is present in that list. If we use mremap, this list is not updated.
// To bypass this, we call munmap ourselves.
func munmap(data []byte) error {
	if len(data) == 0 || len(data) != cap(data) {
		return unix.EINVAL
	}
	_, _, errno := unix.Syscall(
		unix.SYS_MUNMAP,
		uintptr(unsafe.Pointer(&data[0])),
		uintptr(len(data)),
		0,
	)
	if errno != 0 {
		return errno
	}
	return nil
}

// madvise uses the madvise system call to give advise about the use of memory
// when using a slice that is memory-mapped to a file. Set the readahead flag to
// false if page references are expected in random order.
func madvise(b []byte, readahead bool) error {
	flags := unix.MADV_NORMAL
	if !readahead {
		flags = unix.MADV_RANDOM
	}
	return unix.Madvise(b, flags)
}

// msync writes any modified data to persistent storage.
func msync(b []byte) error {
	return unix.Msync(b, unix.MS_SYNC)
}
