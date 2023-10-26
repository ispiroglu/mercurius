package server

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/ispiroglu/mercurius/internal/broker"
	"github.com/ispiroglu/mercurius/internal/logger"
	"github.com/ispiroglu/mercurius/proto"
	"go.uber.org/zap"
)

const totalEventCount = 100 * 100 * 100
const subscriberBulkEventCount = 100

var y = atomic.Uint64{}
var messageCount = atomic.Uint64{}
var start time.Time

type Server struct {
	logger     *zap.Logger
	broker     *broker.Broker
	RetryQueue chan *proto.Event
	proto.UnimplementedMercuriusServer
}

func NewMercuriusServer() *Server {
	return &Server{
		logger: logger.NewLogger(),
		broker: broker.NewBroker(),
	}
}

func (s *Server) Publish(_ context.Context, event *proto.Event) (*proto.ACK, error) {
	checkPublishEventCount()
	return s.broker.Publish(event)
}

func (s *Server) Subscribe(req *proto.SubscribeRequest, stream proto.Mercurius_SubscribeServer) error {
	ctx := stream.Context()
	sub, err := s.broker.Subscribe(ctx, req.Topic, req.SubscriberID, req.SubscriberName)
	if err != nil {
		s.logger.Error("Error on subscribe", zap.String("Topic", req.Topic), zap.String("SubscriberID", req.SubscriberID), zap.String("Subscriber Name", req.SubscriberName)) //, zap.Error(err))
		return err
	}

	return handleEvent(stream, sub, s.logger)
}

func (s *Server) Retry(_ context.Context, req *proto.RetryRequest) (*proto.ACK, error) {
	s.broker.SubscriberRepository.Subscribers[req.SubscriberID].RetryQueue <- req.Event
	return &proto.ACK{}, nil
}

// Should move this to subscriber.
func handleEvent(stream proto.Mercurius_SubscribeServer, sub *broker.Subscriber, logger *zap.Logger) error {
	eventBuffer := make([]*proto.Event, 0, subscriberBulkEventCount)
	for {
		select {
		case <-sub.Ctx.Done():
			handleUnsubscribe(sub)
		case event := <-sub.EventChannel:
			checkSentEventCount()
			eventBuffer = append(eventBuffer, event)
			if len(eventBuffer) == subscriberBulkEventCount {
				bulkEvent := &proto.BulkEvent{
					EventList: eventBuffer,
				}
				handleBulkEventSent(bulkEvent, stream, sub)
				eventBuffer = eventBuffer[:0]
			}
		}
	}
}

func handleUnsubscribe(sub *broker.Subscriber) error {
	broker.SubscriberRetryHandler.RemoveRetryQueue(sub.Id)
	go func(sub *broker.Subscriber) {
		// s.broker.Unsubscribe(sub)
	}(sub)

	return nil
}

func handleBulkEventSent(bulkEvent *proto.BulkEvent, stream proto.Mercurius_SubscribeServer, sub *broker.Subscriber) {
	if err := stream.Send(bulkEvent); err != nil {

		fmt.Println("Cannot send event. retry!")
		// sub.logger.Error("Error on sending event", zap.String("Error: ", err.Error()), zap.String("TopicName", event.Topic), zap.String("SubscriberID", sub.Id), zap.String("Subscriber Name", sub.Name)) //, zap.Error(err))
		// sub.RetryQueue <- event
	}
}

func checkPublishEventCount() {
	if y.Add(1) == 1 {
		start = time.Now()
	}

	if y.Load() == uint64(100*100) {
		t := time.Since(start)
		fmt.Println("Enterence time Of Events: ", t)
	}
}

func checkSentEventCount() {
	x := messageCount.Add(1)
	if x == totalEventCount {
		z := time.Since(start)
		fmt.Println("Total stream send time: ", z)
	}
}
