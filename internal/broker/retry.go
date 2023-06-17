package broker

import (
	"strconv"
	"time"

	"github.com/ispiroglu/mercurius/internal/logger"
	"github.com/ispiroglu/mercurius/proto"
	"go.uber.org/zap"
)

/*
	TODO: How to handle full buffer??
	TODO: Find a way for Internal Server Error
*/

// What should these values be?
// This values should be configurable from yml.
const retryBufferSize = 5000000
const retryCount = 5
const retryTime = 1
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

func (rh *RetryHandler) RemoveRetryQueue(subId string) {
	delete(rh.RetryQueues, subId)
}

func (rh *RetryHandler) CreateRetryQueue(subId string, eq chan *proto.Event) chan *proto.Event {
	rq := make(chan *proto.Event, retryBufferSize)
	rh.RetryQueues[subId] = rq
	go rh.HandleRetryQueue(rq, eq)
	return rq
}

func GetRetryQueue(subId string) chan *proto.Event {
	return SubscriberRetryHandler.RetryQueues[subId]
}

// TODO remove entry from map
func (rh *RetryHandler) HandleRetryQueue(rq chan *proto.Event, eq chan *proto.Event) {
	eventRetryCount := make(map[string]int)
	for {
		event := <-rq
		eventRetryCount[event.Id]++
		if eventRetryCount[event.Id] == -1 {
			delete(eventRetryCount, event.Id)
			rh.logger.Info("Discarded event " + event.Id + " retry limit reached")
		} else {
			rh.logger.Info("Retrying for event " + event.Id + " [" + strconv.Itoa(eventRetryCount[event.Id]) + "]")
			go func() {
				if eventRetryCount[event.Id] == retryCount {
					eventRetryCount[event.Id] = -2
				}
				time.Sleep(retryTime * retryTimeType)
				eq <- event
			}()
		}
	}
}
