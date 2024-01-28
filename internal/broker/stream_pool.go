package broker

import (
	"sync"

	"github.com/ispiroglu/mercurius/proto"
)

type StreamPool struct {
	SubscriberName string
	Streams        []*Subscriber
	Ch             *chan *proto.Event
	chClose        chan struct{}
	closed         bool
	sync.Mutex
}

func newStreamPool(name string) *StreamPool {
	ch := make(chan *proto.Event)
	return &StreamPool{
		SubscriberName: name,
		Streams:        make([]*Subscriber, 0),
		Ch:             &ch,
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
	p.Streams = nil
	close(p.chClose)
	close(*p.Ch)
}

// Make sure that every subscriber has added to the same stream pool
func (p *StreamPool) AddSubscriber(s *Subscriber) {
	p.Lock()
	defer p.Unlock()

	p.Streams = append(p.Streams, s)
	go s.worker(p.Ch, &p.chClose)
}

func (s *Subscriber) worker(ch *chan *proto.Event, closeCh *chan struct{}) {
	for {
		select {
		case e := <-*ch:
			s.EventChannel <- e
			// time.Sleep(20 * time.Microsecond)
		case <-*closeCh:
			return
		}
	}
}
