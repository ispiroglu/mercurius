package broker

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ispiroglu/mercurius/internal/logger"
	"go.uber.org/zap"

	"github.com/ispiroglu/mercurius/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Topic struct {
	sync.RWMutex
	logger               *zap.Logger
	Name                 string
	SubscriberRepository *SubscriberRepository
	EventChan            chan *proto.Event
}

type TopicRepository struct {
	sync.RWMutex
	logger *zap.Logger
	Topics map[string]*Topic
}

type ITopicRepository interface {
	GetTopic(string) (*Topic, error)
	CreateTopic(string) (*Topic, error)
	PublishEvent(*proto.Event)
	AddSubscriber(context.Context, string, string) error
}

func NewTopicRepository() *TopicRepository {
	return &TopicRepository{
		logger: logger.NewLogger(),
		Topics: map[string]*Topic{},
	}
}

func (r *TopicRepository) GetTopic(name string) (*Topic, error) {
	topic, exist := r.Topics[name]
	if !exist {
		//	r.logger.Warn("Could not found topic", zap.String("Topic", name))
		return nil, status.Error(codes.NotFound, "cannot found the topic called:"+name)
	}

	return topic, nil
}

func (r *TopicRepository) CreateTopic(name string) (*Topic, error) {
	r.Lock()
	defer r.Unlock()

	if _, err := r.GetTopic(name); err == nil {
		// r.logger.Warn("Cannot create the topic that already exists", zap.String("Topic", name))
		return nil, status.Error(codes.AlreadyExists, "there is already a topic named:"+name)
	}

	createdTopic := newTopic(name)
	r.Topics[name] = createdTopic

	return createdTopic, nil
}

var publishCount = uint32(100 * 100)
var publish = atomic.Uint32{}
var start time.Time
var TOTAL = 100 * 100

func (t *Topic) PublishEvent(event *proto.Event) {
	if len(t.SubscriberRepository.Subscribers) == 0 {
		t.EventChan <- event
	} else {
		publish.Add(1)

		if publish.Load() == uint32(1) {
			start = time.Now()
		}

		if publish.Load() == publishCount {
			fmt.Println("Executionnn Time", time.Since(start))
		}

		// This line produces a sync wait
		// as it waits for the previous subscriber to complete its send operation before proceeding to the next subscriber.
		// maybe a worker pool to minimize this?
		// One subscribers fullness affects other subscribers.
		var ts time.Time = time.Now()
		for _, s := range t.SubscriberRepository.Subscribers {
			s.EventChannel <- event
		}
		fmt.Println("Rotate etmem su kadar surdu", time.Since(ts), len(t.SubscriberRepository.Subscribers))
	}
}

func (t *Topic) AddSubscriber(ctx context.Context, id string, name string) (*Subscriber, error) {

	s, err := t.SubscriberRepository.addSubscriber(ctx, id, name, t.Name)
	if err != nil {
		t.logger.Error("Could not add already existing subscriber to topic", zap.String("Topic", t.Name), zap.String("SubscriberID", id), zap.String("Subscriber name", name))
		errorMessage := fmt.Sprintf("This subscriber: %s is alreay added to this topic: %s\n", id, t.Name)
		return nil, status.Error(codes.AlreadyExists, errorMessage)
	}

	//t.logger.Info("Added subscriber", zap.String("Topic", t.Name), zap.String("sId", id), zap.String("sName", name))
	if len(t.SubscriberRepository.Subscribers) == 1 {
		if len(t.EventChan) != 0 {
			for event := range t.EventChan {
				s.EventChannel <- event
			}
		}
	}

	return s, nil
}

func (r *TopicRepository) Unsubscribe(subscriber *Subscriber) {
	t := subscriber.TopicName
	if err := r.Topics[t].SubscriberRepository.Unsubscribe(subscriber); err != nil {
		r.logger.Warn("", zap.Error(err))
	}
}

func newTopic(name string) *Topic {
	return &Topic{
		logger:               logger.NewLogger(),
		Name:                 name,
		SubscriberRepository: NewSubscriberRepository(),
		EventChan:            make(chan *proto.Event),
	}
}
