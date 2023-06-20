package broker

import (
	"context"
	"sync"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/ispiroglu/mercurius/internal/logger"
	"github.com/ispiroglu/mercurius/proto"
	"go.uber.org/zap"
)

type SubscriberRepository struct {
	sync.RWMutex
	logger      *zap.Logger
	Subscribers map[string]*Subscriber
}

type Subscriber struct {
	logger       *zap.Logger
	Id           string
	Name         string
	EventChannel chan *proto.Event
	RetryQueue   chan *proto.Event
	TopicName    string
	Ctx          context.Context // TODO: What to do with this?
}

func (r *SubscriberRepository) Unsubscribe(subscriber *Subscriber) error {
	r.Lock()
	defer r.Unlock()

	if _, ok := r.Subscribers[subscriber.Id]; !ok {
		return status.Error(codes.NotFound, "Cannot found subscriber at repository.")
	}

	delete(r.Subscribers, subscriber.Id)
	return nil
}

// NewSubscriber Should we handle the channel buffer size here or get at register level?
func NewSubscriber(ctx context.Context, sId string, sName string, topicName string) *Subscriber {
	eq := make(chan *proto.Event, 100)
	return &Subscriber{
		logger:       logger.NewLogger(),
		Id:           sId,
		Name:         sName,
		EventChannel: eq,
		RetryQueue:   SubscriberRetryHandler.CreateRetryQueue(sId, eq),
		TopicName:    topicName,
		Ctx:          ctx,
	}
}

func NewSubscriberRepository() *SubscriberRepository {
	return &SubscriberRepository{
		logger:      logger.NewLogger(),
		Subscribers: map[string]*Subscriber{},
	}
}

func (r *SubscriberRepository) addSubscriber(ctx context.Context, id string, subName string, topicName string) (*Subscriber, error) {
	r.Lock()
	defer r.Unlock()
	if r.Subscribers[id] != nil {
		return nil, status.Error(codes.AlreadyExists, "Already Exists")
	}

	s := NewSubscriber(ctx, id, subName, topicName)
	r.Subscribers[id] = s
	return s, nil
}

func (r *SubscriberRepository) addSub(s *Subscriber) {
	r.Lock()
	defer r.Unlock()
	r.Subscribers[s.Id] = s
}
