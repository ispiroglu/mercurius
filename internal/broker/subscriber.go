package broker

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/ispiroglu/mercurius/internal/logger"
	"github.com/ispiroglu/mercurius/proto"
	"go.uber.org/zap"
)

const totalEventCount = 100 * 100 * 100
const subscriberBulkEventCount = 100 * 100

var messageCount = atomic.Uint64{}
var _start time.Time

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
	Ctx          context.Context
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

func NewSubscriber(ctx context.Context, sId string, sName string, topicName string) *Subscriber {
	channelSize := 100 * 100 // This channel size should be configurable.
	eventChannel := make(chan *proto.Event, channelSize)
	return &Subscriber{
		logger:       logger.NewLogger(),
		Id:           sId,
		Name:         sName,
		EventChannel: eventChannel,
		RetryQueue:   SubscriberRetryHandler.CreateRetryQueue(sId, eventChannel),
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

// Should move this to subscriber.
func (s *Subscriber) HandleBulkEvent(stream proto.Mercurius_SubscribeServer) error {
	eventBuffer := make([]*proto.Event, 0, subscriberBulkEventCount)
	for {
		select {
		case <-s.Ctx.Done():
			return nil
		case event := <-s.EventChannel:
			checkSentEventCount()
			eventBuffer = append(eventBuffer, event)
			if len(eventBuffer) == subscriberBulkEventCount {
				bulkEvent := &proto.BulkEvent{
					EventList: eventBuffer,
				}
				s.sendEvent(bulkEvent, stream)
				eventBuffer = eventBuffer[:0]
			}
		}
	}
}

func (sub *Subscriber) sendEvent(bulkEvent *proto.BulkEvent, stream proto.Mercurius_SubscribeServer) {
	if err := stream.Send(bulkEvent); err != nil {
		sub.logger.Error("Error on sending event", zap.String("Error: ", err.Error()), zap.String("SubscriberID", sub.Id), zap.String("Subscriber Name", sub.Name)) //, zap.Error(err))
		// sub.RetryQueue <- event
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

func checkSentEventCount() {
	x := messageCount.Add(1)
	if x == 1 {
		_start = time.Now()
	}
	if x == totalEventCount {
		z := time.Since(_start)
		fmt.Println("Total stream send time: ", z)
	}
}
