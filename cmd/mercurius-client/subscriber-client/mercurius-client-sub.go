package main

import (
	"context"
	"errors"
	k "github.com/ispiroglu/mercurius/internal/logger"
	"github.com/ispiroglu/mercurius/pkg/client"
	"github.com/ispiroglu/mercurius/proto"
	"go.uber.org/zap"
	"math/rand"
	"time"
)

const ADDR = "0.0.0.0:9000"
const TopicName = "SampleTopicName"
const CLIENT_NAME = "Sample Client"

var logger = k.NewLogger()

func main() {
	c, err := client.NewClient(CLIENT_NAME, ADDR)
	if err != nil {
		logger.Error("Err", zap.Error(err))
	}

	if err := c.Subscribe(TopicName, context.Background(), x); err != nil {
		logger.Error("Err", zap.Error(err))
	}

	time.Sleep(1 * time.Hour)
}

func x(x *proto.Event) error {
	println(x.Topic)
	var e error = nil
	if rand.Intn(2) == 0 {
		e = errors.New("Sealm")
	}
	return e
}
