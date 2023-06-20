package server

import (
	"context"
	"runtime"
	"sync/atomic"

	"github.com/ispiroglu/mercurius/internal/broker"
	"github.com/ispiroglu/mercurius/internal/logger"
	"github.com/ispiroglu/mercurius/proto"
	"go.uber.org/zap"
)

var messageCount = atomic.Uint64{}

type Server struct {
	logger     *zap.Logger
	broker     *broker.Broker
	RetryQueue chan *proto.Event
	proto.UnimplementedMercuriusServer
	GetCount  *atomic.Uint64
	SendCount *atomic.Uint64
}

func NewMercuriusServer() *Server {
	return &Server{
		logger:    logger.NewLogger(),
		broker:    broker.NewBroker(),
		SendCount: &atomic.Uint64{},
		GetCount:  &atomic.Uint64{},
	}
}

// Publish TODO: Why are we abstracting the publishing at server level and broker level??
// When we switch to multiple broker implementation we will need this.
// We should handle the ctx here.
func (s *Server) Publish(_ context.Context, event *proto.Event) (*proto.ACK, error) {
	// s.logger.Info("Geldi", zap.String("count", string(event.Body)))
	s.GetCount.Add(1)

	// s.logger.Info("", zap.Int("SAA", runtime.NumGoroutine()))

	return s.broker.Publish(event)
	// return &proto.ACK{}, nil
}

func (s *Server) Subscribe(req *proto.SubscribeRequest, stream proto.Mercurius_SubscribeServer) error {
	// s.logger.Info("Received subscribe request", zap.String("Topic", req.Topic))
	ctx := stream.Context()
	sub, err := s.broker.Subscribe(ctx, req.Topic, req.SubscriberID, req.SubscriberName)
	if err != nil {
		s.logger.Error("Error on subscribe", zap.String("Topic", req.Topic), zap.String("SubscriberID", req.SubscriberID), zap.String("Subscriber Name", req.SubscriberName)) //, zap.Error(err))
		return err
	}

	// TODO: Who is the subscriber? How to handle fan outs??
	// TODO: How to implement done channel? Should we implement?
	// TODO: Should we run this for block in a goroutine? -> !This gives an error! Why

	/*////////////////////
	    |  TODO: If the client cannot process it should
		|  ask for the event again for a later time. Context?
		////////////////////*/
	for {

		select {
		case <-sub.Ctx.Done():
			// UNSUB

			// Brokers will have subscription map in the future.
			// So this process will be a broker level
			broker.SubscriberRetryHandler.RemoveRetryQueue(sub.Id)
			go func(sub *broker.Subscriber) {
				s.broker.Unsubscribe(sub)
				runtime.GC()
			}(sub)

			return nil
		case event := <-sub.EventChannel:
			go func() {
				if err := stream.Send(event); err != nil {
					s.logger.Error("Error on sending event", zap.String("TopicName", event.Topic), zap.String("SubscriberID", req.SubscriberID), zap.String("Subscriber Name", req.SubscriberName)) //, zap.Error(err))
					s.logger.Info("Sending event to retry queue")
					sub.RetryQueue <- event
				}
			}()
		}
	}
}

func (s *Server) Retry(_ context.Context, req *proto.RetryRequest) (*proto.ACK, error) {
	s.logger.Info("Received retry request")
	s.broker.SubscriberRepository.Subscribers[req.SubscriberID].RetryQueue <- req.Event
	return &proto.ACK{}, nil
}
