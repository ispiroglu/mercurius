package main

import (
	"context"
	"fmt"
	"time"

	logger2 "github.com/ispiroglu/mercurius/internal/logger"
	"github.com/ispiroglu/mercurius/pkg/client"
	"go.uber.org/zap"
)

const ADDR = "0.0.0.0:9000"
const TopicName = "one-to-one"
const CLIENT_NAME = "Sample Client"

var logger = logger2.NewLogger()

func main() {
	c, err := client.NewClient(CLIENT_NAME, ADDR)
	if err != nil {
		logger.Error("Err", zap.Error(err))
	}

	for j := 0; j < 500; j++ {
		if err := c.Publish(TopicName, []byte(fmt.Sprintf("This is the sample message: %d", j)), context.Background()); err != nil {
			logger.Error("Err", zap.Error(err))
		}

		time.Sleep(1 * time.Second)
	}

}
