package broker

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

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
}

var publishCount = uint32(100 * 100)
var publish = atomic.Uint32{}
var start time.Time

func (t *Topic) PublishEvent(event *proto.Event) {
	if t.SubscriberRepository.poolCount.Load() == 0 {
		t.EventChan <- event
	} else {
		publish.Add(1)

		if publish.Load() == uint32(1) {
			start = time.Now()
		}

		if publish.Load() == publishCount {
			fmt.Println("Total Routing Time: ", time.Since(start))
		}

		t.SubscriberRepository.StreamPools.Range(func(k any, v interface{}) bool {
			v.(*StreamPool).Ch <- event
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

	return s, nil
}

func newTopic(name string) *Topic {
	return &Topic{
		logger:               logger.NewLogger(),
		Name:                 name,
		SubscriberRepository: NewSubscriberRepository(),
		EventChan:            make(chan *proto.Event),
	}
}
