package main

import (
	"context"
	"fmt"
	"strconv"
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

	for i := 0; i < 1; i++ {
		// time.Sleep(2 * time.Microsecond)
		go func() {
			for j := 0; j < 1; j++ {
				if err := c.Publish(TopicName, []byte(strconv.FormatUint(messageCount.Load(), 10)), context.Background()); err != nil {
					logger.Error("Err", zap.Error(err))
				}
				fmt.Println(strconv.FormatUint(messageCount.Load(), 10))

				messageCount.Add(1)
			}
		}()
	}

	time.Sleep(1 * time.Hour)
}
