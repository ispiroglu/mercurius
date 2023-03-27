package broker

import (
	"context"
	"github.com/ispiroglu/mercurius/internal/logger"
	"github.com/ispiroglu/mercurius/proto"
	"go.uber.org/zap"
)

const bufferSize = 20

type Subscriber struct {
	logger       *zap.Logger
	Id           string
	Name         string
	EventChannel chan *proto.Event
	Ctx          context.Context // TODO: What to do with this?
}

// NewSubscriber Should we handle the channel buffer size here or get at register level?
func NewSubscriber(ctx context.Context, sId string, sName string) *Subscriber {
	return &Subscriber{
		logger:       logger.NewLogger(),
		Id:           sId,
		Name:         sName,
		EventChannel: make(chan *proto.Event, bufferSize),
		Ctx:          ctx,
	}
}
