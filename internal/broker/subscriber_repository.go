package broker

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/ispiroglu/mercurius/internal/logger"
	"go.uber.org/zap"
)

type SubscriberRepository struct {
	logger      *zap.Logger
	StreamPools *sync.Map
	poolCount   atomic.Uint32
}

func NewSubscriberRepository() *SubscriberRepository {
	return &SubscriberRepository{
		logger:      logger.NewLogger(),
		StreamPools: &sync.Map{},
	}
}

func (r *SubscriberRepository) Unsubscribe(subscriber *Subscriber) error {
	val, ok := r.StreamPools.LoadAndDelete(subscriber.Name)
	if !ok {
		return fmt.Errorf("subscriber not found")
	}

	val.(*StreamPool).Delete()

	r.poolCount.Store(
		r.poolCount.Load() - 1,
	)
	return nil
}

/*
func (r *SubscriberRepository) PublishEvent(event *proto.Event) {
	if r.poolCount.Load() == 0 {
		panic("No subscriber found")
	} else {

		r.StreamPools

		r.StreamPools.Range(func(k any, v interface{}) bool {
			c := *v.(*StreamPool).Ch
			select {
			case c <- event.Event:
			default:
				panic("ASDA")
			}
			return true
		})
	}
}*/

func (r *SubscriberRepository) addSubscriber(ctx context.Context, id string, subName string, topicName string) (*Subscriber, error) {

	// Handle subName conflict?
	s := NewSubscriber(ctx, id, subName, topicName)
	var pool *StreamPool

	if p, ok := r.StreamPools.Load(subName); ok {
		pool = p.(*StreamPool)
	} else {
		pool = newStreamPool(subName)
		r.StreamPools.Store(subName, pool)
	}

	pool.AddSubscriber(s)
	r.poolCount.Add(1)
	return s, nil
}

func (r *SubscriberRepository) addSub(s *Subscriber) {

	var pool *StreamPool
	if p, ok := r.StreamPools.Load(s.Name); ok {
		pool = p.(*StreamPool)
	} else {
		pool = newStreamPool(s.Name)
		r.StreamPools.Store(s.Name, pool)
	}

	pool.AddSubscriber(s)
	r.poolCount.Add(1)
}
