package broker

import (
	"context"
	"log"

	"github.com/ispiroglu/mercurius/internal/logger"
	"github.com/ispiroglu/mercurius/proto"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const ADDR = "0.0.0.0:9000"

var handler = NewRetryHandler()

func init() {
	logger := logger.NewLogger()
	conn, err := grpc.Dial(ADDR, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("Failed to dial to %s, %v", ADDR, err)
	}

	c := proto.NewMercuriusClient(conn)
	go func(retryChannel chan *proto.Event, logger *zap.Logger) {
		defer conn.Close()
		for {
			event := <-retryChannel
			logger.Info("Retrying event " + event.Id)

			_, err := c.Publish(context.Background(), event)
			if err != nil {
				logger.Error(err.Error())
				/*============================
				| TODO proper timeout system |
				=============================*/
				now := int64(timestamppb.Now().AsTime().Unix())
				if event.CreatedAt.AsTime().Unix()+int64(event.ExpiresAt) > now {
					logger.Info("Timeout for event " + event.Id)
				}
				retryChannel <- event
			}
			logger.Info("Retry for event " + event.Id + " is successful.")
		}
	}(GetRetryQueue(), logger)
}

type RetryHandler struct {
	RetryQueue chan *proto.Event
}

func NewRetryHandler() *RetryHandler {
	return &RetryHandler{
		RetryQueue: make(chan *proto.Event),
	}
}

func GetRetryQueue() chan *proto.Event {
	return handler.RetryQueue
}
