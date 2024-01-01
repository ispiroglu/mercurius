package broker

import (
	"github.com/ispiroglu/mercurius/proto"
	"sync"
)

type StreamPool struct {
	SubscriberName string
	Streams        []*Subscriber
	Ch             chan *proto.Event
	chClose        chan struct{}
	closed         bool
	sync.Mutex
}

func newStreamPool(name string) *StreamPool {
	return &StreamPool{
		SubscriberName: name,
		Streams:        make([]*Subscriber, 0),
		Ch:             make(chan *proto.Event, 10),
		chClose:        make(chan struct{}),
		closed:         false,
		Mutex:          sync.Mutex{},
	}
}

func (p *StreamPool) Delete() {
	p.Lock()
	defer p.Unlock()
	p.closed = true
	p.chClose <- struct{}{}
	for _, s := range p.Streams {
		close(s.EventChannel)
	}
	p.Streams = nil
	close(p.chClose)
	close(p.Ch)
}

// Make sure that every subscriber has added to the same stream pool
func (p *StreamPool) AddSubscriber(s *Subscriber) {
	p.Lock()
	defer p.Unlock()

	p.Streams = append(p.Streams, s)
	go s.worker(&p.Ch, &p.chClose)
}

func (s *Subscriber) worker(ch *chan *proto.Event, closeCh *chan struct{}) {
	for {
		select {
		case e := <-*ch:
			s.EventChannel <- e
		case <-*closeCh:
			return
		}
	}
}
