package broker

import (
	"context"

	"github.com/ispiroglu/mercurius/internal/logger"
	pb "github.com/ispiroglu/mercurius/proto"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Broker struct {
	logger *zap.Logger
	*TopicRepository
	SubscriberRepository *SubscriberRepository
}

type IBroker interface {
	Publish(event *pb.Event) (*pb.ACK, error)
	Subscribe(ctx context.Context, topic string, sId string, sName string) (<-chan *pb.Event, error)
}

func NewBroker() *Broker {
	return &Broker{
		logger:               logger.NewLogger(),
		TopicRepository:      NewTopicRepository(),
		SubscriberRepository: NewSubscriberRepository(),
	}
}

func (b *Broker) Publish(event *pb.Event) (*pb.ACK, error) {
	t, err := b.findOrInsertTopic(event.Topic)
	if err != nil {
		return nil, err
	}

	go t.PublishEvent(event)
	return &pb.ACK{}, nil
}

func (b *Broker) Unsubscribe(sub *Subscriber) {
	b.TopicRepository.Unsubscribe(sub)
	if err := b.SubscriberRepository.Unsubscribe(sub); err != nil {
		b.logger.Warn("Failed to unsubscribe subscriber", zap.String("SubscriberID", sub.Id), zap.String("Subscriber Name", sub.Name), zap.Error(err))
	}
}

func (b *Broker) Subscribe(ctx context.Context, topicName string, sId string, sName string) (*Subscriber, error) {
	t, err := b.findOrInsertTopic(topicName)
	if err != nil {
		return nil, err
	}

	s, err := t.AddSubscriber(ctx, sId, sName)
	if err != nil {
		b.logger.Error("Broker could not add subscriber to topic", zap.String("Topic", topicName), zap.String("SubscriberID", sId), zap.Error(err))
		return nil, err
	}
	return s, nil
}

func (b *Broker) findOrInsertTopic(topicName string) (*Topic, error) {
	t, err := b.GetTopic(topicName)
	if err != nil {
		t, err = b.CreateTopic(topicName)
		if err != nil {
			st, ok := status.FromError(err)
			if !ok && st.Code() != codes.AlreadyExists {
				return nil, err
			} else if st.Code() == codes.AlreadyExists {
				t, _ = b.GetTopic(topicName)
			}
		}
	}

	return t, nil
}
