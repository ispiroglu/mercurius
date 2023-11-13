package broker

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/ispiroglu/mercurius/internal/logger"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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

	if _, ok := r.StreamPools.Load(subscriber.Id); !ok {
		return status.Error(codes.NotFound, "Cannot found subscriber at repository.")
	}

	r.StreamPools.Delete(subscriber.Id)
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
