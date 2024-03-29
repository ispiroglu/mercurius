package main

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/google/uuid"

	client_example "github.com/ispiroglu/mercurius/cmd/mercurius-client"
	k "github.com/ispiroglu/mercurius/internal/logger"
	"github.com/ispiroglu/mercurius/pkg/client"
	"github.com/ispiroglu/mercurius/proto"
	"go.uber.org/zap"
)

const ADDR = "0.0.0.0:9000"
const TopicName = "one-to-one"

var messageCount = atomic.Uint64{}
var start = time.Time{}
var logger = k.NewLogger()
var ctx, _ = context.WithCancel(context.Background())
var ch = make(chan struct{})

func main() {

	for i := 0; i < client_example.SubscriberCount; i++ {
		go func() {
			id, _ := uuid.NewUUID()
			c, err := client.NewClient(id, ADDR)
			if err != nil {
				logger.Error("Err", zap.Error(err))
			}
			if err := c.Subscribe(TopicName, ctx, handler); err != nil {
				logger.Error("Err", zap.Error(err))
			}
		}()

	}

	fmt.Println("total time", time.Since(start))
	<-ch
}

func handler(e *proto.Event) error {
	x := messageCount.Add(1)
	if x == 1 {
		start = time.Now()
	}
	fmt.Println("event received: ", string(e.Body))

	return nil
}
