package main

import (
	"context"
	logger2 "github.com/ispiroglu/mercurius/internal/logger"
	"github.com/ispiroglu/mercurius/pkg/client"
	"go.uber.org/zap"
	"time"
)

const ADDR = "0.0.0.0:9000"
const TopicName = "SampleTopicName"
const CLIENT_NAME = "Sample Client"

var logger = logger2.NewLogger()

func main() {
	c, err := client.NewClient(CLIENT_NAME, ADDR)
	if err != nil {
		logger.Error("Err", zap.Error(err))
	}

	if err := c.Publish(TopicName, "ASDAS", context.Background()); err != nil {
		logger.Error("Err", zap.Error(err))
	}

	time.Sleep(1 * time.Hour)
}
