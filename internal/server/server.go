package server

import (
	"context"
	"github.com/ispiroglu/mercurius/internal/broker"
	"github.com/ispiroglu/mercurius/proto"
	"log"
)

type Server struct {
	broker broker.Broker
	proto.UnimplementedMercuriusServer
}

// Publish TODO: Why are we abstracting the publishing at server level and broker level??
// When we switch to multiple broker implementation we will need this.
// We should handle the ctx here.
func (s *Server) Publish(ctx context.Context, event *proto.Event) (*proto.ACK, error) {
	log.Println("Recieved publish request for:", event.Topic)
	return s.broker.Publish(event)
}

func (s *Server) Subscribe(req *proto.SubscribeRequest, stream proto.Mercurius_SubscribeServer) error {
	log.Printf("Recieved subscribe request by %s - %s to %s", req.SubscriberName, req.SubscriberID, req.Topic)

	c, err := s.broker.Subscribe(req.Topic)
	if err != nil {
		log.Printf("Cannot subscribe %s - %s to %s", req.SubscriberName, req.SubscriberID, req.Topic)
		return err
	}

	// TODO: Who is the subscriber? How to handle fan outs??
	// TODO: How to implement done channel? Should we implement?
	for {
		event := <-c

	}
	return nil
}
