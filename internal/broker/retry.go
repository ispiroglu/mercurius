package broker

import (
	"sync"

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
const retryBufferSize = 1

// const retryCount = 5
// const retryTime = 100
// const retryTimeType = time.Millisecond

var SubscriberRetryHandler = NewRetryHandler()

type RetryHandler struct {
	bufferSize int
	repository *RetryMapRepository
	logger     *zap.Logger
}

type RetryMapRepository struct {
	sync.RWMutex
	logger      *zap.Logger
	RetryQueues map[string]chan *proto.Event
}

func NewRetryMapRepository() *RetryMapRepository {
	return &RetryMapRepository{
		RWMutex:     sync.RWMutex{},
		logger:      &zap.Logger{},
		RetryQueues: map[string]chan *proto.Event{},
	}
}

func NewRetryHandler() *RetryHandler {
	return &RetryHandler{
		bufferSize: retryBufferSize,
		repository: NewRetryMapRepository(),
		logger:     logger.NewLogger(),
	}
}

func (rh *RetryHandler) RemoveRetryQueue(subId string) {
	rh.repository.Lock()
	defer rh.repository.Unlock()

	delete(rh.repository.RetryQueues, subId)
}

func (rh *RetryHandler) CreateRetryQueue(subId string, eq chan *proto.Event) chan *proto.Event {
	rq := make(chan *proto.Event)
	rh.repository.addChannel(subId, rq)
	go rh.HandleRetryQueue(rq, eq)
	return rq
}

func GetRetryQueue(subId string) chan *proto.Event {
	return SubscriberRetryHandler.repository.RetryQueues[subId]
}

// TODO remove entry from map
func (rh *RetryHandler) HandleRetryQueue(rq chan *proto.Event, eq chan *proto.Event) {
	// eventRetryCount := make(map[string]int)
	// for {
	// 	event := <-rq
	// 	eventRetryCount[event.Id]++
	// 	if eventRetryCount[event.Id] == -1 {
	// 		delete(eventRetryCount, event.Id)
	// 		rh.logger.Info("Discarded event " + event.Id + " retry limit reached")
	// 	} else {
	// 		rh.logger.Info("Retrying for event " + event.Id + " [" + strconv.Itoa(eventRetryCount[event.Id]) + "]")
	// 		go func() {
	// 			if eventRetryCount[event.Id] == retryCount {
	// 				eventRetryCount[event.Id] = -2
	// 			}
	// 			time.Sleep(retryTime * retryTimeType)
	// 			eq <- event
	// 		}()
	// 	}
	// }
}

func (r *RetryMapRepository) addChannel(sId string, c chan *proto.Event) {
	r.Lock()
	defer r.Unlock()

	r.RetryQueues[sId] = c
}
