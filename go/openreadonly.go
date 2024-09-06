//go:build !windows

package main

import "os"

func openreadonly(name string) (*os.File, error) {
	return os.Open(name)
}
