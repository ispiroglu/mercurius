package broker

import (
	"context"
	pb "github.com/ispiroglu/mercurius/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"log"
)

type Broker struct {
	TopicRepository
	pb.UnimplementedBrokerServer
}

func (b *Broker) Publish(ctx context.Context, event *pb.Event) (*pb.ACK, error) {
	log.Println("Publishing event: ", event.Topic)

	topic, err := b.GetTopic(event.Topic)
	if err != nil {
		log.Println(err)
		return nil, err
	}

	topic.PublishEvent(event)
	return &pb.ACK{}, nil
}

func (b *Broker) Subscribe(*pb.SubscribeRequest, pb.Broker_SubscribeServer) error {
	return status.Errorf(codes.Unimplemented, "method Subscribe not implemented")
}
