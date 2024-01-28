package broker_test

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/ispiroglu/mercurius/pkg/client"
	"github.com/ispiroglu/mercurius/proto"
	"github.com/stretchr/testify/assert"
)

const ADDR = "0.0.0.0:9000"
const TopicName = "top"
const MessageCount = 100
const n = 10

var messageCountOneToOne = atomic.Uint64{}
var messageCountNtoOne = atomic.Uint64{}
var messageCountOnetoN = atomic.Uint64{}
var messageCountNtoN = atomic.Uint64{}

type SafeMap struct {
	m map[string]bool
	sync.Mutex
}

func (sm *SafeMap) Add(key string) {
	sm.Lock()
	sm.m[key] = true
	sm.Unlock()
}

var testDoneChannel = make(chan struct{})
var testOneNChannel = make(chan struct{})
var testNOneChannel = make(chan struct{})
var testNNChannel = make(chan struct{})

var testMapOneOne = &SafeMap{m: make(map[string]bool), Mutex: sync.Mutex{}}
var testMapNOne = &SafeMap{m: make(map[string]bool), Mutex: sync.Mutex{}}
var testMapOneN = &SafeMap{m: make(map[string]bool), Mutex: sync.Mutex{}}
var testMapNN = &SafeMap{m: make(map[string]bool), Mutex: sync.Mutex{}}
var canProcess = false
var gotSecondTime = false

func TestOneOneMessageReliability(t *testing.T) {
	var controlMap = make(map[string]bool)

	for i := 0; i < MessageCount; i++ {
		controlMap[strconv.Itoa(i)] = true
	}

	pubId, _ := uuid.NewUUID()
	cPub, _ := client.NewClient(pubId, ADDR)

	subId, _ := uuid.NewUUID()
	cSub, _ := client.NewClient(subId, ADDR)

	t.Run("Messages sent and received should be the same", func(t *testing.T) {
		_ = cSub.Subscribe(TopicName, context.Background(), authenticityHandlerOneOne)

		time.Sleep(1 * time.Second)
		for i := 0; i < MessageCount; i++ {
			fmt.Println(i)
			err := cPub.Publish(TopicName, []byte(fmt.Sprint(i)), context.Background())
			if err != nil {
				panic(err)
			}
			time.Sleep(1 * time.Millisecond)
		}

		<-testDoneChannel
		assert.Equal(t, true, reflect.DeepEqual(testMapOneOne.m, controlMap))
	})

	t.Cleanup(func() {
		testMapOneOne = &SafeMap{m: make(map[string]bool), Mutex: sync.Mutex{}}
		messageCountOneToOne = atomic.Uint64{}
	})
}

func TestNOneMessageReliability(t *testing.T) {
	t.Run("Messages sent and received should be the same", func(t *testing.T) {
		var controlMap = make(map[string]bool)

		for i := 0; i < MessageCount; i++ {
			controlMap[strconv.Itoa(i)] = true
		}

		cPub := &client.Client{}

		subId, _ := uuid.NewUUID()
		cSub, _ := client.NewClient(subId, ADDR)

		_ = cSub.Subscribe(TopicName, context.Background(), authenticityHandlerNOne)
		time.Sleep(1 * time.Second)
		signalChannel := make(chan struct{})

		for i := 0; i < MessageCount; i++ {

			if i%(MessageCount/n) == 0 {
				pubId, _ := uuid.NewUUID()
				cPub, _ = client.NewClient(pubId, ADDR)
				fmt.Println("Changed publisher")
			}

			go func(i int, signalChannel chan struct{}, cPub *client.Client) {
				<-signalChannel
				_ = cPub.Publish(TopicName, []byte(fmt.Sprintf("%d", i)), context.Background())

			}(i, signalChannel, cPub)

		}

		close(signalChannel)
		<-testNOneChannel
		assert.Equal(t, true, reflect.DeepEqual(testMapNOne.m, controlMap))
	})

	t.Cleanup(func() {
		testMapNOne = &SafeMap{m: make(map[string]bool), Mutex: sync.Mutex{}}
		messageCountNtoOne = atomic.Uint64{}
	})
}

func TestOneNMessageReliability(t *testing.T) {
	t.Run("Messages sent and received should be the same", func(t *testing.T) {
		var controlMap = make(map[string]bool)

		for i := 0; i < MessageCount; i++ {
			controlMap[strconv.Itoa(i)] = true
		}

		pubId, _ := uuid.NewUUID()
		cPub, _ := client.NewClient(pubId, ADDR)

		signalChannel := make(chan struct{})
		for i := 0; i < MessageCount; i++ {
			subId, _ := uuid.NewUUID()
			cSub, _ := client.NewClient(subId, ADDR)
			go func(i int, signalChannel chan struct{}, cSub *client.Client) {
				<-signalChannel
				_ = cSub.Subscribe(TopicName, context.Background(), authenticityHandlerOneN)
			}(i, signalChannel, cSub)
		}

		close(signalChannel)
		time.Sleep(5 * time.Second)

		for i := 0; i < MessageCount; i++ {
			fmt.Println(i)
			err := cPub.Publish(TopicName, []byte(fmt.Sprint(i)), context.Background())
			if err != nil {
				panic(err)
			}
			time.Sleep(1 * time.Millisecond)
		}
		<-testOneNChannel
		assert.Equal(t, true, reflect.DeepEqual(testMapOneN.m, controlMap))
	})

	t.Cleanup(func() {
		testMapOneN = &SafeMap{m: make(map[string]bool), Mutex: sync.Mutex{}}
		messageCountOnetoN = atomic.Uint64{}
	})
}

func TestNNMessageReliability(t *testing.T) {
	t.Run("Messages sent and received should be the same", func(t *testing.T) {
		var controlMap = make(map[string]bool)

		for i := 0; i < MessageCount; i++ {
			controlMap[strconv.Itoa(i)] = true
		}

		pubId, _ := uuid.NewUUID()
		cPub, _ := client.NewClient(pubId, ADDR)

		subSignalChannel := make(chan struct{})
		for i := 0; i < MessageCount; i++ {
			subId, _ := uuid.NewUUID()
			cSub, _ := client.NewClient(subId, ADDR)
			go func(i int, signalChannel chan struct{}, cSub *client.Client) {
				<-signalChannel
				_ = cSub.Subscribe(TopicName, context.Background(), authenticityHandlerNN)
			}(i, subSignalChannel, cSub)
		}

		close(subSignalChannel)
		time.Sleep(5 * time.Second)
		pubSignalChannel := make(chan struct{})
		for i := 0; i < MessageCount; i++ {

			if i%(MessageCount/n) == 0 {
				pubId, _ := uuid.NewUUID()
				cPub, _ = client.NewClient(pubId, ADDR)
				fmt.Println("Changed publisher")
			}

			go func(i int, signalChannel chan struct{}, cPub *client.Client) {
				<-signalChannel
				_ = cPub.Publish(TopicName, []byte(fmt.Sprintf("%d", i)), context.Background())

			}(i, pubSignalChannel, cPub)

		}
		close(pubSignalChannel)

		<-testNNChannel
		assert.Equal(t, true, reflect.DeepEqual(testMapNN.m, controlMap))
	})

	t.Cleanup(func() {
		testMapNN = &SafeMap{m: make(map[string]bool), Mutex: sync.Mutex{}}
		messageCountNtoN = atomic.Uint64{}
	})
}

func TestMessageResendRequest(t *testing.T) {
	t.Run("Messages should be resent if requested.", func(t *testing.T) {

		pubId, _ := uuid.NewUUID()
		cPub, _ := client.NewClient(pubId, ADDR)

		subId, _ := uuid.NewUUID()
		cSub, _ := client.NewClient(subId, ADDR)

		_ = cSub.Subscribe(TopicName, context.Background(), resendHandler)
		time.Sleep(1 * time.Second)
		_ = cPub.Publish(TopicName, []byte("message"), context.Background())
		time.Sleep(10 * time.Second)
		assert.Equal(t, true, gotSecondTime)
	})
	t.Cleanup(func() {
		gotSecondTime = false
	})

}

func authenticityHandlerOneOne(e *proto.Event) error {
	testMapOneOne.Add(string(e.Body))
	messageCount := messageCountOneToOne.Add(1)
	fmt.Println("Message count -> ", messageCount)
	if messageCount == MessageCount {
		testDoneChannel <- struct{}{}
	}
	return nil
}

func authenticityHandlerNOne(e *proto.Event) error {
	testMapNOne.Add(string(e.Body))
	messageCount := messageCountNtoOne.Add(1)
	if messageCount == MessageCount {
		testNOneChannel <- struct{}{}
	}
	return nil
}

func authenticityHandlerOneN(e *proto.Event) error {
	testMapOneN.Add(string(e.Body))
	messageCount := messageCountOnetoN.Add(1)
	if messageCount == MessageCount {
		testOneNChannel <- struct{}{}
	}
	return nil
}

func authenticityHandlerNN(e *proto.Event) error {
	testMapNN.Add(string(e.Body))
	messageCount := messageCountNtoN.Add(1)
	if messageCount == MessageCount*MessageCount {
		testNNChannel <- struct{}{}
	}
	return nil
}

func resendHandler(_ *proto.Event) error {

	if !canProcess {
		canProcess = true
		fmt.Println("First time")
		return errors.New("Couldn't process")
	}
	fmt.Println("Second time")
	gotSecondTime = true

	return nil
}
