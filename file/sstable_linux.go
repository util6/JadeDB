package file
import (
	"os"
	"syscall"
)

func (f *SSTable) open() error {
	fd, err := syscall.Open(f.path, os.O_RDONLY, 0666)
	if err!= nil {
		return err
	}
	f.fd = int32(fd)
	return nil
}
func (f *SSTable) close() error {
	return syscall.Close(int(f.fd))
}
func (f *SSTable) read(offset int64, size int) ([]byte, error) {
	buf := make([]byte, size)
	_, err := syscall.Pread(int(f.fd), buf, offset)))
}
