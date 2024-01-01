package broker

import (
	"sync"

	"github.com/ispiroglu/mercurius/internal/logger"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type TopicRepository struct {
	logger *zap.Logger
	Topics sync.Map
}

func NewTopicRepository() *TopicRepository {
	return &TopicRepository{
		logger: logger.NewLogger(),
		Topics: sync.Map{},
	}
}

func (r *TopicRepository) GetTopic(name string) (*Topic, error) {
	topic, exist := r.Topics.Load(name)
	if !exist {
		//	r.logger.Warn("Could not found topic", zap.String("Topic", name))
		return nil, status.Error(codes.NotFound, "cannot found the topic called:"+name)
	}

	return topic.(*Topic), nil
}

func (r *TopicRepository) CreateTopic(name string) (*Topic, error) {
	if _, err := r.GetTopic(name); err == nil {
		// r.logger.Warn("Cannot create the topic that already exists", zap.String("Topic", name))
		return nil, status.Error(codes.AlreadyExists, "there is already a topic named:"+name)
	}

	createdTopic := newTopic(name)
	r.Topics.Store(name, createdTopic)

	return createdTopic, nil
}

func (r *TopicRepository) Unsubscribe(subscriber *Subscriber) {
	topic, err := r.GetTopic(subscriber.TopicName)
	if err != nil {
		panic(err)
	}

	if err := topic.SubscriberRepository.Unsubscribe(subscriber); err != nil {
		// r.logger.Warn("", zap.Error(err))
	}
}
