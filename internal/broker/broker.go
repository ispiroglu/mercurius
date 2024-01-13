package broker

import (
	"context"
	"errors"
	"sync"

	"github.com/ispiroglu/mercurius/internal/logger"
	pb "github.com/ispiroglu/mercurius/proto"
	"go.uber.org/zap"
)

type Broker struct {
	logger *zap.Logger
	*TopicRepository
	SubscriberRepository *SubscriberRepository
	retriedEvents        sync.Map
}

func NewBroker() *Broker {
	return &Broker{
		logger:               logger.NewLogger(),
		TopicRepository:      NewTopicRepository(),
		SubscriberRepository: NewSubscriberRepository(),
		retriedEvents:        sync.Map{},
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

	b.SubscriberRepository.addSub(s)
	return s, nil
}

func (b *Broker) Retry(_ context.Context, in *pb.RetryRequest) (*pb.ACK, error) {
	retryCountInterface, ok := b.retriedEvents.Load(in.Event.Id)
	retryCount := 0
	if ok {
		retryCount = retryCountInterface.(int)
	}

	retryCount++

	if retryCount == 4 {
		err := errors.New("exceeded retry limit")
		b.retriedEvents.Delete(in.Event.Id)

		return nil, err
	}

	streamPool, ok := b.SubscriberRepository.StreamPools.Load(in.SubscriberID)
	if !ok {
		return nil, errors.New("invalid retry request")
	}
	*streamPool.(*StreamPool).Ch <- in.Event

	b.retriedEvents.Store(in.Event.Id, retryCount)
	return &pb.ACK{}, nil
}

func (b *Broker) GetTopics() map[string]bool {
	topics := make(map[string]bool, 0)
	b.TopicRepository.Topics.Range(func(key, value interface{}) bool {
		topics[key.(string)] = true
		return true
	})

	return topics
}

func (b *Broker) findOrInsertTopic(topicName string) (*Topic, error) {
	if topic, ok := b.TopicRepository.Topics.Load(topicName); ok {
		return topic.(*Topic), nil
	}

	t := newTopic(topicName)
	b.TopicRepository.Topics.Store(topicName, t)
	return t, nil
}
