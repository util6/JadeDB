package utils

import (
	"errors"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
)

var (
	gopath = path.Join(os.Getenv("GOPATH"), "src") + "/"
)

// NotFoundKey 找不到key
var (
	// ErrKeyNotFound is returned when key isn't found on a txn.Get.
	ErrKeyNotFound = errors.New("Key not found")
	// ErrEmptyKey is returned if an empty key is passed on an update function.
	ErrEmptyKey = errors.New("Key cannot be empty")
	// ErrReWriteFailure reWrite failure
	ErrReWriteFailure = errors.New("reWrite failure")
	// ErrBadMagic bad magic
	ErrBadMagic = errors.New("bad magic")
	// ErrBadChecksum bad check sum
	ErrBadChecksum = errors.New("bad check sum")
	// ErrChecksumMismatch is returned at checksum mismatch.
	ErrChecksumMismatch = errors.New("checksum mismatch")

	ErrTruncate = errors.New("Do truncate")
	ErrStop     = errors.New("Stop")

	// compact
	ErrFillTables = errors.New("Unable to fill tables")

	ErrBlockedWrites  = errors.New("Writes are blocked, possibly due to DropAll or Close")
	ErrTxnTooBig      = errors.New("Txn is too big to fit into one request")
	ErrDeleteVlogFile = errors.New("Delete vlog file")
	ErrNoRoom         = errors.New("No room for write")
	ErrReadOnlyTxn    = errors.New("cannot perform write operation on read-only transaction")
	ErrDiscardedTxn   = errors.New("this transaction has been discarded. Create a new one")
	ErrConflict       = errors.New("transaction conflict")
	ErrDBClosed       = errors.New("database is closed")

	// ErrInvalidRequest is returned if the user request is invalid.
	ErrInvalidRequest = errors.New("Invalid request")
	// ErrNoRewrite is returned if a call for valueIndex log GC doesn't result in a log file rewrite.
	ErrNoRewrite = errors.New("Value log GC attempt didn't result in any cleanup")

	// ErrRejected is returned if a valueIndex log GC is called either while another GC is running, or
	// after DB::Close has been called.
	ErrRejected = errors.New("Value log GC request rejected")

	// 优化相关错误
	ErrTaskQueueFull = errors.New("Task queue is full")
	ErrTaskTimeout   = errors.New("Task execution timeout")
	ErrTaskPanic     = errors.New("Task execution panic")

	// B+树相关错误
	ErrInvalidOptions = errors.New("Invalid options")
	ErrBufferPoolFull = errors.New("Buffer pool is full")
	ErrPagePinned     = errors.New("Page is pinned and cannot be evicted")
)

// Panic 如果err 不为nil 则panicc
func Panic(err error) {
	if err != nil {
		panic(err)
	}
}

// Check 检查错误，如果有错误则 panic
func Check(err error) {
	if err != nil {
		panic(err)
	}
}

// Wrapf 包装错误并添加格式化消息
func Wrapf(err error, format string, args ...interface{}) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf(format+": %w", append(args, err)...)
}

// Panic2 _
func Panic2(_ interface{}, err error) {
	Panic(err)
}

// Err err
func Err(err error) error {
	if err != nil {
		fmt.Printf("%s %s\n", location(2, true), err)
	}
	return err
}

// WarpErr err
func WarpErr(format string, err error) error {
	if err != nil {
		fmt.Printf("%s %s %s", format, location(2, true), err)
	}
	return err
}
func location(deep int, fullPath bool) string {
	_, file, line, ok := runtime.Caller(deep)
	if !ok {
		file = "???"
		line = 0
	}

	if fullPath {
		if strings.HasPrefix(file, gopath) {
			file = file[len(gopath):]
		}
	} else {
		file = filepath.Base(file)
	}
	return file + ":" + strconv.Itoa(line)
}

// CondPanic e
func CondPanic(condition bool, err error) {
	if condition {
		Panic(err)
	}
}
