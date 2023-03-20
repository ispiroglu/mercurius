package server

import (
	"context"
	"github.com/ispiroglu/mercurius/internal/broker"
	"github.com/ispiroglu/mercurius/proto"
)

type Server struct {
	broker *broker.Broker
	proto.UnimplementedMercuriusServer
}

func NewMercuriusServer() *Server {
	return &Server{
		broker: broker.NewBroker(),
	}
}

// Publish TODO: Why are we abstracting the publishing at server level and broker level??
// When we switch to multiple broker implementation we will need this.
// We should handle the ctx here.
func (s *Server) Publish(ctx context.Context, event *proto.Event) (*proto.ACK, error) {
	return s.broker.Publish(event)
}

func (s *Server) Subscribe(req *proto.SubscribeRequest, stream proto.Mercurius_SubscribeServer) error {

	c, err := s.broker.Subscribe(context.Background(), req.Topic, req.SubscriberID)
	if err != nil {
		return err
	}

	// TODO: Who is the subscriber? How to handle fan outs??
	// TODO: How to implement done channel? Should we implement?
	// TODO: Should we run this for block in a goroutine? -> !This gives an error! Why
ForScope:
	for event := range c {
		if err := stream.Send(event); err != nil {
			break ForScope // TODO: Should change this to a done channel or error channel !
		}
	}

	return nil
}
