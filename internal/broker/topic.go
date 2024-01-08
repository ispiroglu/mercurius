package broker

import (
	"context"
	"fmt"
	"sync"

	"github.com/alitto/pond"
	"github.com/ispiroglu/mercurius/internal/logger"
	"github.com/ispiroglu/mercurius/proto"

	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Topic struct {
	sync.RWMutex
	logger               *zap.Logger
	Name                 string
	SubscriberRepository *SubscriberRepository
	EventChan            chan *proto.Event
	workerPool           *pond.WorkerPool
}

func (t *Topic) PublishEvent(event *proto.Event) {
	if t.SubscriberRepository.poolCount.Load() == 0 {
		t.EventChan <- event
	} else {

		t.SubscriberRepository.StreamPools.Range(func(k any, v interface{}) bool {
			c := *v.(*StreamPool).Ch
			c <- event
			return true
		})
	}
}

func (t *Topic) AddSubscriber(ctx context.Context, id string, name string) (*Subscriber, error) {
	s, err := t.SubscriberRepository.addSubscriber(ctx, id, name, t.Name)

	if err != nil {
		t.logger.Error("Could not add already existing subscriber to topic", zap.String("Topic", t.Name), zap.String("SubscriberID", id), zap.String("Subscriber name", name))
		errorMessage := fmt.Sprintf("This subscriber: %s is alreay added to this topic: %s\n", id, t.Name)
		return nil, status.Error(codes.AlreadyExists, errorMessage)
	}

	//t.sendBufferedEvents(name)
	return s, nil
}

func (t *Topic) sendBufferedEvents(subName string) {
	for event := range t.EventChan {
		s, ok := t.SubscriberRepository.StreamPools.Load(subName)
		if !ok {
			panic("Subscriber not found")
		}
		*s.(*StreamPool).Ch <- event
	}
}

func newTopic(name string) *Topic {
	return &Topic{
		logger:               logger.NewLogger(),
		Name:                 name,
		SubscriberRepository: NewSubscriberRepository(),
		EventChan:            make(chan *proto.Event), // If channel is full, there will be waiting goroutines.
		workerPool:           nil,
	}
}
