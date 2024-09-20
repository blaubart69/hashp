package main

import (
	"testing"
)

func TestMikeByteSize(t *testing.T) {

	var tests = []struct {
		bytes uint64
		want  string
	}{
		{0, "0 B"},
		{1, "1 B"},
		{10, "10 B"},
		{999, "999 B"},
		{1000, "0.97 KiB"},
		{1023, "0.99 KiB"},
		{1024, "1.00 KiB"},
		{2048, "2.00 KiB"},
		{1024*1024 - 1, "0.99 MiB"},
		{1024 * 1024, "1.00 MiB"},
		{1024*1024 + 1, "1.00 MiB"},
		{1024 * 1024 * 1024, "1.00 GiB"},
		{1024*1024*1024 + 1024*1024*11, "1.01 GiB"},
	}

	for _, test := range tests {
		got := MikeByteSize(test.bytes)
		if got != test.want {
			t.Errorf("%d bytes should result in %s but we got %s", test.bytes, test.want, got)
		}
	}

	for i := 0; i < 1024*1024+1024+1; i++ {
		got := MikeByteSize(uint64(i))
		if len(got) > 10 {
			t.Errorf("%d bytes results in [%s]. too long.", i, got)
		}
	}
}
