package server

import (
	"context"
	"fmt"
	"runtime"
	"sync/atomic"

	"github.com/ispiroglu/mercurius/internal/broker"
	"github.com/ispiroglu/mercurius/internal/logger"
	"github.com/ispiroglu/mercurius/proto"
	"go.uber.org/zap"
)

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
	return s.broker.Publish(event)
}

func (s *Server) Subscribe(req *proto.SubscribeRequest, stream proto.Mercurius_SubscribeServer) error {
	// s.logger.Info("Received subscribe request", zap.String("Topic", req.Topic))
	ctx := stream.Context()
	sub, err := s.broker.Subscribe(ctx, req.Topic, req.SubscriberID, req.SubscriberName)
	if err != nil {
		s.logger.Error("Error on subscribe", zap.String("Topic", req.Topic), zap.String("SubscriberID", req.SubscriberID), zap.String("Subscriber Name", req.SubscriberName)) //, zap.Error(err))
		return err
	}

	// This does not changes any time
	for w := 0; w < 1; w++ {
		go consumerTask(stream, sub, s.logger)
	}

	cc := make(chan struct{})
	<-cc

	for {

		select {
		case <-sub.Ctx.Done():

			broker.SubscriberRetryHandler.RemoveRetryQueue(sub.Id)
			go func(sub *broker.Subscriber) {
				// s.broker.Unsubscribe(sub)
				runtime.GC()
			}(sub)

			return nil
		case event := <-sub.EventChannel:
			go func() {
				if err := stream.Send(event); err != nil {

					s.logger.Error("Error on sending event", zap.String("TopicName", event.Topic), zap.String("SubscriberID", sub.Id), zap.String("Subscriber Name", sub.Name)) //, zap.Error(err))
					s.logger.Info("Sending event to retry queue")
					sub.RetryQueue <- event
				}
			}()
		}
	}

	return nil
}

func (s *Server) Retry(_ context.Context, req *proto.RetryRequest) (*proto.ACK, error) {
	s.logger.Info("Received retry request")
	s.broker.SubscriberRepository.Subscribers[req.SubscriberID].RetryQueue <- req.Event
	return &proto.ACK{}, nil
}

var messageCount = atomic.Uint64{}

func consumerTask(stream proto.Mercurius_SubscribeServer, sub *broker.Subscriber, logger *zap.Logger) {
	for {

		select {
		case <-sub.Ctx.Done():

			broker.SubscriberRetryHandler.RemoveRetryQueue(sub.Id)
			go func(sub *broker.Subscriber) {
				// s.broker.Unsubscribe(sub)
				runtime.GC()
			}(sub)

			return
		case event := <-sub.EventChannel:
			fmt.Println("ASDAS", messageCount.Add(1))
			// time.Sleep(4 * time.Second)
			go func() {
				if err := stream.Send(event); err != nil {
					panic("")
					logger.Error("Error on sending event", zap.String("TopicName", event.Topic), zap.String("SubscriberID", sub.Id), zap.String("Subscriber Name", sub.Name)) //, zap.Error(err))
					logger.Info("Sending event to retry queue")
					sub.RetryQueue <- event
				}
			}()
		}
	}
}
