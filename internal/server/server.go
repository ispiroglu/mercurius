package server

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	client_example "github.com/ispiroglu/mercurius/cmd/mercurius-client"
	"github.com/ispiroglu/mercurius/internal/broker"
	"github.com/ispiroglu/mercurius/internal/logger"
	"github.com/ispiroglu/mercurius/proto"
	"go.uber.org/zap"
)

var eventCount = atomic.Uint64{}
var startTime time.Time

type Server struct {
	logger     *zap.Logger
	broker     *broker.Broker
	RetryQueue chan *proto.Event
	proto.UnimplementedMercuriusServer
}

func NewMercuriusServer() *Server {
	return &Server{
		logger: logger.NewLogger(),
		broker: broker.NewBroker(),
	}
}

func (s *Server) Publish(_ context.Context, event *proto.Event) (*proto.ACK, error) {
	checkPublishEventCount()
	return s.broker.Publish(event)
}

func (s *Server) Subscribe(req *proto.SubscribeRequest, stream proto.Mercurius_SubscribeServer) error {
	ctx := stream.Context()
	sub, err := s.broker.Subscribe(ctx, req.Topic, req.SubscriberID, req.SubscriberName)
	if err != nil {
		s.logger.Error("Error on subscribe", zap.String("Topic", req.Topic), zap.String("SubscriberID", req.SubscriberID), zap.String("Subscriber Name", req.SubscriberName)) //, zap.Error(err))
		return err
	}

	err = sub.HandleBulkEvent(&stream)
	if err != nil {
		s.logger.Error("", zap.Error(err))
	}

	s.broker.Unsubscribe(sub)
	return err
}

func checkPublishEventCount() {
	if eventCount.Add(1) == 1 {
		startTime = time.Now()
	}

	if eventCount.Load() == client_example.TotalPublishCount {
		elapsedTime := time.Since(startTime)
		fmt.Println("Elapsed time since the start of events:", elapsedTime)
	}
}
