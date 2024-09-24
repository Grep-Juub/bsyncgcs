package utils

import (
	"fmt"
	"sync/atomic"
	"time"
)

func DisplayProgress(done <-chan struct{}, totalSize int64, bytesRead *int64) {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-done:
			fmt.Println("100% completed")
			return
		case <-ticker.C:
			read := atomic.LoadInt64(bytesRead)
			percentage := float64(read) / float64(totalSize) * 100
			fmt.Printf("%.2f%% completed\n", percentage)
		}
	}
}
