package main

import (
	"io/fs"
	"os"
	"syscall"
)

const FILE_FLAG_SEQUENTIAL_SCAN = 0x08000000

func openreadonly(path string) (*os.File, error) {
	pathp, e1 := syscall.UTF16PtrFromString(path)
	if e1 != nil {
		return nil, &fs.PathError{Op: "UTF16PtrFromString", Path: path, Err: e1}
	}
	handle, err := syscall.CreateFile(
		pathp,
		syscall.GENERIC_READ,
		syscall.FILE_SHARE_READ|syscall.FILE_SHARE_WRITE|syscall.FILE_SHARE_DELETE,
		nil,
		syscall.OPEN_EXISTING,
		syscall.FILE_FLAG_BACKUP_SEMANTICS|FILE_FLAG_SEQUENTIAL_SCAN,
		0)

	return os.NewFile(uintptr(handle), path), err
}
