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
const N = 100 * 100 * 100

var messageCount = atomic.Uint64{}
var start = time.Time{}
var logger = k.NewLogger()
var ctx, _ = context.WithCancel(context.Background())
var ch = make(chan struct{})

func main() {
	c, err := client.NewClient(CLIENT_NAME, ADDR)
	if err != nil {
		logger.Error("Err", zap.Error(err))
	}
	for i := 0; i < 100; i++ {
		go func() {
			if err := c.Subscribe(TopicName, ctx, handler); err != nil {
				logger.Error("Err", zap.Error(err))
			}
		}()

	}

	<-ch
}

func handler(e *proto.Event) error {
	x := messageCount.Add(1)
	if x == 1 {
		start = time.Now()
	}
	fmt.Println(x)
	if x == N {
		z := time.Since(start)
		fmt.Println("Execution time: ", z)
		ch <- struct{}{}
	}
	return nil
}
