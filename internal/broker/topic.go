package broker

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/alitto/pond"
	client_example "github.com/ispiroglu/mercurius/cmd/mercurius-client"
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

var publish = atomic.Uint64{}
var start time.Time

func (t *Topic) PublishEvent(event *proto.Event) {
	if t.SubscriberRepository.poolCount.Load() == 0 {
		t.EventChan <- event
	} else {
		c := publish.Add(1)

		if c == uint64(1) {
			start = time.Now()
		}

		if publish.Load() == client_example.TotalPublishCount {
			// 	fmt.Println("Total Routing Time: ", time.Since(start))
		}

		t.workerPool.Submit(func() {
			t.SubscriberRepository.StreamPools.Range(func(k any, v interface{}) bool {
				v.(*StreamPool).Ch <- event
				return true
			})
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
		s.(*StreamPool).Ch <- event
	}
}

func newTopic(name string) *Topic {

	return &Topic{
		logger:               logger.NewLogger(),
		Name:                 name,
		SubscriberRepository: NewSubscriberRepository(),
		EventChan:            make(chan *proto.Event), // If channel is full, there will be waiting goroutines.
		workerPool:           pond.New(50, 100, pond.Strategy(pond.Eager()), pond.IdleTimeout(10*time.Millisecond)),
	}
}
