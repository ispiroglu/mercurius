package broker

import (
	"strconv"
	"sync"
	"time"

	"github.com/ispiroglu/mercurius/internal/logger"
	"github.com/ispiroglu/mercurius/proto"
	"go.uber.org/zap"
)

const retryBufferSize = 1

const retryCount = 5
const retryTime = 100
const retryTimeType = time.Millisecond

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

func (rh *RetryHandler) CreateRetryQueue(sName string, eq chan *proto.Event) chan *proto.Event {
	rq := make(chan *proto.Event)
	rh.repository.addChannel(sName, rq)
	go rh.HandleRetryQueue(rq, eq)
	return rq
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

func (r *RetryMapRepository) addChannel(sName string, c chan *proto.Event) {
	r.Lock()
	defer r.Unlock()

	r.RetryQueues[sName] = c
}
