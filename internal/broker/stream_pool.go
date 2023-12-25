package broker

import (
	"bytes"
	"runtime"
	"strconv"
	"sync"

	"github.com/ispiroglu/mercurius/proto"
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
		Streams:        []*Subscriber{},
		Ch:             make(chan *proto.Event),
		Mutex:          sync.Mutex{},
	}
}

func (p *StreamPool) AddSubscriber(s *Subscriber) {
	p.Lock()
	defer p.Unlock()

	p.Streams = append(p.Streams, s)
	id := len(p.Streams)

	go s.worker(id, p.Ch)
}

func (s *Subscriber) worker(id int, ch chan *proto.Event) {
	for e := range ch {
		s.EventChannel <- e
	}
}

func getGoroutineID() string {
	b := make([]byte, 64)
	b = b[:runtime.Stack(b, false)]
	b = bytes.TrimPrefix(b, []byte("goroutine "))
	b = b[:bytes.IndexByte(b, ' ')]
	n, _ := strconv.Atoi(string(b))
	return strconv.Itoa(n)
}
