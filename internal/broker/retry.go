package broker

import (
	"time"

	"github.com/ispiroglu/mercurius/internal/logger"
	"github.com/ispiroglu/mercurius/proto"
	"go.uber.org/zap"
)

// What should these values be?
const retryBufferSize = 50
const retryCount = 5
const retryTime = 10
const retryTimeType = time.Second

var SubscriberRetryHandler = NewRetryHandler()

type RetryHandler struct {
	bufferSize  int
	RetryQueues map[string]chan *proto.Event
	logger      *zap.Logger
}

func NewRetryHandler() *RetryHandler {
	return &RetryHandler{
		bufferSize:  retryBufferSize,
		RetryQueues: make(map[string]chan *proto.Event),
		logger:      logger.NewLogger(),
	}
}

func (rh *RetryHandler) Remove(subId string) {
	delete(rh.RetryQueues, subId)
}

func (rh *RetryHandler) Create(subId string) chan *proto.Event {
	c := make(chan *proto.Event)
	rh.RetryQueues[subId] = c
	go func() {
		eventRetryCount := make(map[string]int)
		for {
			event := <-c
			eventRetryCount[event.Id]++
			if eventRetryCount[event.Id] == retryCount {
				rh.logger.Info("Discarding event " + event.Id + " maximum retries reached")
				delete(eventRetryCount, event.Id)
			} else {
				go func() {
					time.Sleep(retryTime * retryTimeType)
					c <- event
				}()
			}
		}
	}()
	return c
}

func GetRetryQueue(subId string) chan *proto.Event {
	return SubscriberRetryHandler.RetryQueues[subId]
}
