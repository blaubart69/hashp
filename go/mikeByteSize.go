package main

import (
	"fmt"
	"math"
)

func MikeByteSize(b uint64) string {
	if b < 1000 {
		return fmt.Sprintf("%d B", b)
	}

	const unit = 1024
	div, exp := uint64(unit), 0
	for n := b / unit; n >= 1000; n /= unit {
		div *= unit
		exp++
	}

	x := math.Floor((float64(b)/float64(div))*100) / 100

	return fmt.Sprintf("%.2f %ciB", x, "KMGTPE"[exp])
}
