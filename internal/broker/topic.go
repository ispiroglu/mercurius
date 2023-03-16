package broker

import (
	"context"
	"fmt"
	"github.com/ispiroglu/mercurius/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"sync"
)

type Topic struct {
	sync.RWMutex
	Name        string                 // IDs must be Unique
	Subscribers map[string]*Subscriber // TODO: How should we add or remove subscribers?
	EventChan   chan *proto.Event
}

type TopicRepository struct {
	sync.RWMutex
	Topics map[string]*Topic
}

func NewTopicRepository() *TopicRepository {
	return &TopicRepository{
		Topics: map[string]*Topic{},
	}
}

func (r *TopicRepository) GetTopic(name string) (*Topic, error) {
	r.RLock()
	defer r.RUnlock()

	topic, exist := r.Topics[name]
	if !exist {
		return nil, status.Error(codes.NotFound, "cannot found the topic called:"+name)
	}

	return topic, nil
}

func (r *TopicRepository) CreateTopic(name string) (*Topic, error) {
	r.Lock()
	defer r.Unlock()

	_, err := r.GetTopic(name)
	if err == nil {
		return nil, status.Error(codes.AlreadyExists, "there is already a topic named:"+name)
	}

	createdTopic := newTopic(name)
	r.Topics[name] = createdTopic
	return createdTopic, nil
}

func (t *Topic) PublishEvent(event *proto.Event) { // TODO: Should we pass pointer or copy of event
	// TODO: What else need to be done for Publishing at Topic Level?

	t.Lock() // TODO: Do we need Lock? Or different layers of Lock? RW LOCK??
	t.EventChan <- event
	t.Unlock()
}

func (t *Topic) AddSubscriber(ctx context.Context, id string) error {
	t.Lock()
	if t.Subscribers[id] != nil {
		errorMessage := fmt.Sprintf("This subscriber: %s is alreay added to this topic: %s\n", id, t.Name)
		return status.Error(codes.AlreadyExists, errorMessage)
	}

	t.Subscribers[id] = NewSubscriber(ctx, id)
	t.Unlock()
	return nil
}

func newTopic(name string) *Topic {
	return &Topic{
		Name:        name,
		Subscribers: map[string]*Subscriber{},
		EventChan:   make(chan *proto.Event), // TODO: Should this be buffered? Or should we consider asynchrony in upper layer?
	}
}
