package broker

import (
	"context"
	"github.com/ispiroglu/mercurius/proto"
)

const bufferSize = 20

type Subscriber struct {
	Id           string
	EventChannel chan *proto.Event // Should this attribte be pointer or not?
	Ctx          context.Context   // TODO: What to do with this?
}

// NewSubscriber Should we handle the channel buffer size here or get at register level?
func NewSubscriber(ctx context.Context, sId string) *Subscriber {
	return &Subscriber{
		Id:           sId,
		EventChannel: make(chan *proto.Event, bufferSize),
		Ctx:          ctx,
	}
}
