package server

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/ispiroglu/mercurius/internal/broker"
	"github.com/ispiroglu/mercurius/internal/logger"
	"github.com/ispiroglu/mercurius/proto"
	"go.uber.org/zap"
)

const totalEventCount = 100 * 100 * 100

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

var y = atomic.Uint64{}
var messageCount = atomic.Uint64{}
var start time.Time

func (s *Server) Publish(_ context.Context, event *proto.Event) (*proto.ACK, error) {
	if y.Add(1) == 1 {
		start = time.Now()
	}

	if y.Load() == uint64(100*100) {
		t := time.Since(start)
		fmt.Println("Execution time", t)
	}
	return s.broker.Publish(event)
}

func (s *Server) Subscribe(req *proto.SubscribeRequest, stream proto.Mercurius_SubscribeServer) error {
	ctx := stream.Context()
	sub, err := s.broker.Subscribe(ctx, req.Topic, req.SubscriberID, req.SubscriberName)
	if err != nil {
		s.logger.Error("Error on subscribe", zap.String("Topic", req.Topic), zap.String("SubscriberID", req.SubscriberID), zap.String("Subscriber Name", req.SubscriberName)) //, zap.Error(err))
		return err
	}

	// We cannot make here work with worker pool concurrently because cannot use stream.Send() concurrently.
	for w := 0; w < 1; w++ {
		go consumerTask(stream, sub, s.logger)
	}

	cc := make(chan struct{})
	<-cc

	return nil
}

func (s *Server) Retry(_ context.Context, req *proto.RetryRequest) (*proto.ACK, error) {
	s.broker.SubscriberRepository.Subscribers[req.SubscriberID].RetryQueue <- req.Event
	return &proto.ACK{}, nil
}

func consumerTask(stream proto.Mercurius_SubscribeServer, sub *broker.Subscriber, logger *zap.Logger) {
	for {
		select {
		case <-sub.Ctx.Done():

			broker.SubscriberRetryHandler.RemoveRetryQueue(sub.Id)
			go func(sub *broker.Subscriber) {
				// s.broker.Unsubscribe(sub)
			}(sub)

			return
		case event := <-sub.EventChannel:
			if err := stream.Send(event); err != nil {
				logger.Error("Error on sending event", zap.String("Error: ", err.Error()), zap.String("TopicName", event.Topic), zap.String("SubscriberID", sub.Id), zap.String("Subscriber Name", sub.Name)) //, zap.Error(err))
				sub.RetryQueue <- event
			}
			x := messageCount.Add(1)
			if x == totalEventCount {
				z := time.Since(start)
				fmt.Println("Execution time: ", z)
			}
		}
	}
}
