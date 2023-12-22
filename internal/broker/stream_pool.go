package broker

import (
	"github.com/ispiroglu/mercurius/proto"
	"sync"
)

type StreamPool struct {
	SubscriberName string
	Streams        []*Subscriber
	Ch             chan *proto.Event
	sync.Mutex
}

func newStreamPool(name string) *StreamPool {

	return &StreamPool{
		SubscriberName: name,
		Streams:        make([]*Subscriber, 0),
		Ch:             make(chan *proto.Event),
		Mutex:          sync.Mutex{},
	}
}

func (p *StreamPool) AddSubscriber(s *Subscriber) {
	p.Lock()
	defer p.Unlock()

	p.Streams = append(p.Streams, s)
	go s.worker(p.Ch)
}

func (s *Subscriber) worker(ch chan *proto.Event) {
	for e := range ch {
		s.EventChannel <- e
	}
}
