package main

import (
	"context"
	"time"

	k "github.com/ispiroglu/mercurius/internal/logger"
	"github.com/ispiroglu/mercurius/pkg/client"
	"github.com/ispiroglu/mercurius/proto"
	"go.uber.org/zap"
)

const ADDR = "0.0.0.0:9000"
const TopicName = "one-to-one"
const CLIENT_NAME = "Sample Client"

var logger = k.NewLogger()

func main() {
	c, err := client.NewClient(CLIENT_NAME, ADDR)
	if err != nil {
		logger.Error("Err", zap.Error(err))
	}

	if err := c.Subscribe(TopicName, context.Background(), handler); err != nil {
		logger.Error("Err", zap.Error(err))
	}

	time.Sleep(1 * time.Hour)
}

func handler(e *proto.Event) error {
	logger.Info("Received Event", zap.String("Message", string(e.Body)))
	return nil
}
