package broker

import (
	"context"
	"sync/atomic"
	"time"

	client_example "github.com/ispiroglu/mercurius/cmd/mercurius-client"
	"github.com/ispiroglu/mercurius/internal/logger"
	"github.com/ispiroglu/mercurius/proto"
	"go.uber.org/zap"
)

const channelSize = 1

// const subscriberBulkEventCount = 100 * 100
const subscriberBulkEventCount = 1

var messageCount = atomic.Uint64{}
var _start time.Time

type Subscriber struct {
	logger       *zap.Logger
	Id           string
	Name         string
	EventChannel chan *proto.Event
	RetryQueue   chan *proto.Event
	TopicName    string
	Ctx          context.Context
}

func NewSubscriber(ctx context.Context, sId string, sName string, topicName string) *Subscriber {
	// This channel size should be configurable.
	eventChannel := make(chan *proto.Event, channelSize)
	return &Subscriber{
		logger:       logger.NewLogger(),
		Id:           sId,
		Name:         sName,
		EventChannel: eventChannel,
		RetryQueue:   SubscriberRetryHandler.CreateRetryQueue(sName, eventChannel),
		TopicName:    topicName,
		Ctx:          ctx,
	}
}

func (s *Subscriber) HandleBulkEvent(stream *proto.Mercurius_SubscribeServer) error {
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

func (s *Subscriber) sendEvent(bulkEvent *proto.BulkEvent, stream *proto.Mercurius_SubscribeServer) {
	if err := (*stream).Send(bulkEvent); err != nil {
		s.logger.Error("Error on sending event", zap.String("Error: ", err.Error()), zap.String("SubscriberID", s.Id), zap.String("Subscriber Name", s.Name)) //, zap.Error(err))
		// s.RetryQueue <- event.Event
	}
}

func checkSentEventCount() {
	eventCount := messageCount.Add(1)
	if eventCount == 1 {
		_start = time.Now()
	}
	if eventCount == client_example.TotalReceiveCount {
		_ = time.Since(_start)
		// fmt.Println("Total stream send time: ", z)
	}
}
