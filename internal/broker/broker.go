package broker

import (
	pb "github.com/ispiroglu/mercurius/proto"
	"log"
)

// Broker TODO: Should we have broker interface instead of struct?
type Broker struct {
	TopicRepository
}

func (b *Broker) Publish(event *pb.Event) (*pb.ACK, error) {
	log.Println("Publishing event: ", event.Topic)

	topic, err := b.GetTopic(event.Topic)
	if err != nil {
		log.Println(err)
		return nil, err
	}

	topic.PublishEvent(event)
	return &pb.ACK{}, nil
}

// Subscribe Who is the subscriber? How to handle fanouts??
func (b *Broker) Subscribe(topic string) (<-chan pb.Event, error) {
	return nil, nil
}
