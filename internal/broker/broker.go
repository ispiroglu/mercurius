package broker

import (
	"context"

	"github.com/ispiroglu/mercurius/internal/logger"
	pb "github.com/ispiroglu/mercurius/proto"
	"go.uber.org/zap"
)

type Broker struct {
	logger *zap.Logger
	*TopicRepository
	SubscriberRepository *SubscriberRepository
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

	t.PublishEvent(event)
	return &pb.ACK{}, nil
}

func (b *Broker) Unsubscribe(sub *Subscriber) {
	b.TopicRepository.Unsubscribe(sub)
	if err := b.SubscriberRepository.Unsubscribe(sub); err != nil {
		// b.logger.Warn("Failed to unsubscribe subscriber", zap.String("SubscriberID", sub.Id), zap.String("Subscriber Name", sub.Name), zap.Error(err))
	}
}

func (b *Broker) Subscribe(ctx context.Context, topicName string, sId string, sName string) (*Subscriber, error) {
	t, err := b.findOrInsertTopic(topicName)
	if err != nil {
		b.logger.Error("Broker could not find or insert topic", zap.String("Topic", topicName), zap.Error(err))
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
	if topic, ok := b.TopicRepository.Topics.Load(topicName); ok {
		return topic.(*Topic), nil
	}

	t := newTopic(topicName)
	b.TopicRepository.Topics.Store(topicName, t)
	return t, nil
}
