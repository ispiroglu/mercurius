package main

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	logger2 "github.com/ispiroglu/mercurius/internal/logger"
	"github.com/ispiroglu/mercurius/pkg/client"
	"go.uber.org/zap"
)

const ADDR = "0.0.0.0:9000"
const TopicName = "one-to-one"
const CLIENT_NAME = "Sample Client"

var logger = logger2.NewLogger()
var messageCount = atomic.Uint64{}

func main() {
	c, err := client.NewClient(CLIENT_NAME, ADDR)
	if err != nil {
		logger.Error("Err", zap.Error(err))
	}

	logger.Info("Publisehd Event")
	maxGoroutines := 10000 // Set the maximum number of goroutines you want to allow
	semaphore := make(chan struct{}, maxGoroutines)
	var wg sync.WaitGroup

	//// Produce messages to topic (asynchronously)
	timer := time.NewTimer(3 * time.Second)
	//// Produce messages until the timer expires
	for {
		time.Sleep(1 * time.Microsecond)
		select {
		case <-timer.C:
			// Stop producing messages when the timer expires
			wg.Wait() // Wait for all goroutines to finish before returning
			fmt.Printf("Published %d messages\n", messageCount.Load())
			return
		default:
			semaphore <- struct{}{} // Acquire semaphore

			wg.Add(1)
			go func() {
				defer wg.Done()
				defer func() {
					<-semaphore // Release semaphore
				}()

				if err := c.Publish(TopicName, []byte(strconv.FormatUint(messageCount.Load(), 10)), context.Background()); err != nil {
					logger.Error("Err", zap.Error(err))
				}
				fmt.Println(strconv.FormatUint(messageCount.Load(), 10))

				messageCount.Add(1)
			}()
		}
	}

	time.Sleep(1 * time.Hour)
}
