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

var logger = k.NewLogger()

func main() {
	c, err := client.NewClient(CLIENT_NAME, ADDR)
	if err != nil {
		logger.Error("Err", zap.Error(err))
	}

	ctx, cancel := context.WithCancel(context.Background())
	if err := c.Subscribe(TopicName, ctx, handler); err != nil {
		logger.Error("Err", zap.Error(err))
	}

	timer := time.NewTimer(10 * time.Second)

	// Count of consumed messages

	// Consume messages until the timer expires
ConsumerLoop:
	for {
		select {
		case <-timer.C:
			// Stop consuming messages when the timer expires
			cancel()
			break ConsumerLoop

		}
	}

	fmt.Printf("Consumed %d messages\n", messageCount.Load())
}

func handler(e *proto.Event) error {
	messageCount.Add(1)
	fmt.Println(string(e.Body))
	return nil
}
