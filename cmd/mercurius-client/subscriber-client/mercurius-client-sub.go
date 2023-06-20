package main

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	k "github.com/ispiroglu/mercurius/internal/logger"
	"github.com/ispiroglu/mercurius/pkg/client"
	"github.com/ispiroglu/mercurius/proto"
	"go.uber.org/zap"
)

const ADDR = "0.0.0.0:9000"
const TopicName = "one-to-one"
const CLIENT_NAME = "Sample Client"

var messageCount = atomic.Uint64{}
var start time.Time = time.Time{}
var logger = k.NewLogger()
var ctx, cancel = context.WithCancel(context.Background())

func main() {
	c, err := client.NewClient(CLIENT_NAME, ADDR)
	if err != nil {
		logger.Error("Err", zap.Error(err))
	}
	for i := 0; i < 5000; i++ {
		go func() {
			if err := c.Subscribe(TopicName, ctx, handler); err != nil {
				logger.Error("Err", zap.Error(err))
			}
		}()
	}
	fmt.Println("Anne Bitti")

	timer := time.NewTimer(900 * time.Second)

	// Count of consumed messages

	// Consume messages until the timer expires

ConsumerLoop:
	for {
		select {
		case <-timer.C:
			// Stop consuming messages when the timer expires
			// cancel()
			break ConsumerLoop

		}
	}

	fmt.Printf("Consumed %d messages\n", messageCount.Load())
	z := time.Since(start)
	fmt.Println("Execution time: ", z)
}

func handler(e *proto.Event) error {
	messageCount.Add(1)
	if messageCount.Load() == 1 {
		start = time.Now()
	}
	if messageCount.Load() == 500*5000 {
		z := time.Since(start)
		fmt.Println("Execution time: ", z)
	}
	// fmt.Println(string(e.Body))
	return nil
}
