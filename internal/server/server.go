package server

import (
	"context"
	"github.com/ispiroglu/mercurius/internal/broker"
	"github.com/ispiroglu/mercurius/internal/logger"
	"github.com/ispiroglu/mercurius/proto"
	"go.uber.org/zap"
)

type Server struct {
	logger *zap.Logger
	broker *broker.Broker
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
	s.logger.Info("Received publish request", zap.String("Topic", req.Topic))
	ctx := stream.Context()
	sub, err := s.broker.Subscribe(ctx, req.Topic, req.SubscriberID, req.SubscriberName)
	if err != nil {
		s.logger.Error("Error on subscribe", zap.String("Topic", req.Topic), zap.String("SubscriberID", req.SubscriberID), zap.String("Subscriber Name", req.SubscriberName)) //, zap.Error(err))
		return err
	}

	// TODO: Who is the subscriber? How to handle fan outs??
	// TODO: How to implement done channel? Should we implement?
	// TODO: Should we run this for block in a goroutine? -> !This gives an error! Why
loop:
	for {

		select {
		case <-sub.Ctx.Done():
			// UNSUB

			// Brokers will have subscription map in the future.
			// So this process will be a broker level
			go func(sub *broker.Subscriber) {
				s.logger.Info("Received Unsubscribe signal")
				s.broker.Unsubscribe(sub)
			}(sub)

			break loop
		case event := <-sub.EventChannel:
			if err := stream.Send(event); err != nil {
				s.logger.Error("Error on sending event", zap.String("Topic", event.Topic), zap.String("SubscriberID", req.SubscriberID), zap.String("Subscriber Name", req.SubscriberName)) //, zap.Error(err))
				break loop                                                                                                                                                                  // TODO: Should change this to a done channel or error channel !
			}
		}

	}

	return nil
}
