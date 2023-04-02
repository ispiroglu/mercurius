package broker

import (
	"context"
	"fmt"
	"github.com/ispiroglu/mercurius/internal/logger"
	"go.uber.org/zap"
	proto2 "google.golang.org/protobuf/proto"
	"sync"

	"github.com/ispiroglu/mercurius/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Topic struct {
	sync.RWMutex
	logger      *zap.Logger
	Name        string                 // IDs must be Unique
	Subscribers map[string]*Subscriber // TODO: How should we add or remove subscribers?
	EventChan   chan *proto.Event
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
		r.logger.Warn("Could not found topic", zap.String("Topic", name))
		return nil, status.Error(codes.NotFound, "cannot found the topic called:"+name)
	}

	return topic, nil
}

func (r *TopicRepository) CreateTopic(name string) (*Topic, error) {
	r.Lock()
	defer r.Unlock()

	_, err := r.GetTopic(name)
	if err == nil {
		r.logger.Warn("Cannot create the topic that already exists", zap.String("Topic", name))
		return nil, status.Error(codes.AlreadyExists, "there is already a topic named:"+name)
	}

	r.logger.Info("Creating topic", zap.String("Topic", name))
	createdTopic := newTopic(name)
	r.Topics[name] = createdTopic
	return createdTopic, nil
}

func (t *Topic) PublishEvent(event *proto.Event) {
	// TODO: What else need to be done for Publishing at Topic Level?
	if len(t.Subscribers) == 0 {
		t.logger.Info("There is no subscriber at this time. Publishing the event to event channel!", zap.String("Topic Name", t.Name))
		t.EventChan <- proto2.Clone(event).(*proto.Event) // Do we need this cloning?
	} else {
		t.Lock()
		for _, s := range t.Subscribers {
			go func(s *Subscriber, event *proto.Event) {
				s.logger.Info("Sending event to subscriber", zap.String("Topic", event.Topic), zap.String("SubscriberID", s.Id), zap.String("Subscriber name", s.Name))
				s.EventChannel <- event
			}(s, proto2.Clone(event).(*proto.Event))
		}
		t.Unlock()
	}
}

func (t *Topic) AddSubscriber(ctx context.Context, id string, name string) (<-chan *proto.Event, error) {
	t.Lock()
	defer t.Unlock()

	if t.Subscribers[id] != nil {
		t.logger.Error("Could not add already existing subscriber to topic", zap.String("Topic", t.Name), zap.String("SubscriberID", id), zap.String("Subscriber name", name))
		errorMessage := fmt.Sprintf("This subscriber: %s is alreay added to this topic: %s\n", id, t.Name)
		return nil, status.Error(codes.AlreadyExists, errorMessage)
	}

	s := NewSubscriber(ctx, id, name)
	t.Subscribers[id] = s
	t.logger.Info("Added subscriber", zap.String("Topic", t.Name), zap.String("sId", id), zap.String("sName", name))

	if len(t.Subscribers) == 1 {
		go func() {
			for event := range t.EventChan {
				s.logger.Info("Sending event to subscriber", zap.String("Topic", event.Topic), zap.String("SubscriberID", s.Id), zap.String("Subscriber name", s.Name))
				s.EventChannel <- event
			}
		}()
	}

	return s.EventChannel, nil
}

func newTopic(name string) *Topic {
	return &Topic{
		logger:      logger.NewLogger(),
		Name:        name,
		Subscribers: make(map[string]*Subscriber),
		EventChan:   make(chan *proto.Event), // TODO: Should this be buffered? Or should we consider asynchrony in upper layer?
	}
}
