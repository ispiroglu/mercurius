package server

import (
	"context"
	"math/rand"

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

// Publish TODO: Why are we abstracting the publishing at server level and broker level??
// When we switch to multiple broker implementation we will need this.
// We should handle the ctx here.
func (s *Server) Publish(_ context.Context, event *proto.Event) (*proto.ACK, error) {
	s.logger.Info("Received publish request", zap.String("Topic", event.Topic))
	return s.broker.Publish(event)
}

func (s *Server) Subscribe(req *proto.SubscribeRequest, stream proto.Mercurius_SubscribeServer) error {
	s.logger.Info("Received subscribe request", zap.String("Topic", req.Topic))
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
			}(sub)

			return nil
		case event := <-sub.EventChannel:
			if rand.Intn(1) == 0 {
				s.logger.Error("Simulated error")
				sub.RetryQueue <- event
			} else if err := stream.Send(event); err != nil {
				s.logger.Error("Error on sending event", zap.String("TopicName", event.Topic), zap.String("SubscriberID", req.SubscriberID), zap.String("Subscriber Name", req.SubscriberName)) //, zap.Error(err))
				s.logger.Info("Sending event to retry queue")
				sub.RetryQueue <- event
			}
		}
	}
}
