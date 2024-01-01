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
	StreamPools sync.Map
	poolCount   atomic.Uint32
}

func NewSubscriberRepository() *SubscriberRepository {
	return &SubscriberRepository{
		logger:      logger.NewLogger(),
		StreamPools: sync.Map{},
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

func (r *SubscriberRepository) addSubscriber(ctx context.Context, id string, subName string, topicName string) (*Subscriber, error) {

	// Handle subName conflict?
	s := NewSubscriber(ctx, id, subName, topicName)
	pool, _ := r.StreamPools.LoadOrStore(subName, newStreamPool(subName))

	pool.(*StreamPool).AddSubscriber(s)
	r.poolCount.Add(1)
	return s, nil
}
