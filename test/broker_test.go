package broker_test

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ispiroglu/mercurius/pkg/client"
	"github.com/ispiroglu/mercurius/proto"
	"github.com/stretchr/testify/assert"
)

const ADDR = "0.0.0.0:9000"
const TopicName = "top"
const MessageCount = 100
const n = 10

var testMapOneOne = make(map[string]bool)
var messageCountOneOne = atomic.Uint64{}
var doneOneOne = make(chan bool)

var testMapNOne = make(map[string]bool)
var canProcess = false
var gotSecondTime = false

func TestOneOneMessageReliability(t *testing.T) {
	var controlMap = make(map[string]bool)

	for i := 0; i < MessageCount; i++ {
		controlMap[strconv.Itoa(i)] = true
	}

	cPub, _ := client.NewClient("pub", ADDR)

	cSub, _ := client.NewClient("sub", ADDR)
	ctx, cancel := context.WithCancel(context.Background())

	t.Run("Messages sent and received should be the same", func(t *testing.T) {

		_ = cSub.Subscribe(TopicName, ctx, authenticityHandlerOneOne)

		for i := 0; i < MessageCount; i++ {
			_ = cPub.Publish(TopicName, []byte(fmt.Sprintf("%d", i)), context.Background())
		}

		<-doneOneOne

		assert.Equal(t, true, reflect.DeepEqual(testMapOneOne, controlMap))
	})
	t.Cleanup(cancel)
}

func TestNOneMessageReliability(t *testing.T) {
	t.Run("Messages sent and received should be the same", func(t *testing.T) {
		var controlMap = make(map[string]bool)

		for i := 0; i < MessageCount; i++ {
			controlMap[strconv.Itoa(i)] = true
		}

		cPub, _ := client.NewClient("pub", ADDR)

		cSub, _ := client.NewClient("sub", ADDR)

		_ = cSub.Subscribe(TopicName, context.Background(), authenticityHandlerNOne)

		for i := 0; i < MessageCount; i++ {
			if i%(MessageCount/n) == 0 {
				cPub, _ = client.NewClient("pub", ADDR)
				fmt.Println("Changed publisher")
			}
			_ = cPub.Publish(TopicName, []byte(fmt.Sprintf("%d", i)), context.Background())
		}

		time.Sleep(3 * time.Second)

		assert.Equal(t, true, reflect.DeepEqual(testMapNOne, controlMap))
	})

}

func TestMessageResendRequest(t *testing.T) {
	t.Run("Messages should be resent if requested.", func(t *testing.T) {
		cPub, _ := client.NewClient("pub", ADDR)

		cSub, _ := client.NewClient("sub", ADDR)

		_ = cSub.Subscribe(TopicName, context.Background(), resendHandler)
		_ = cPub.Publish(TopicName, []byte("message"), context.Background())
		time.Sleep(1 * time.Second)
		assert.Equal(t, true, gotSecondTime)
	})
}

func authenticityHandlerOneOne(e *proto.Event) error {
	testMapOneOne[string(e.Body)] = true
	messageCountOneOne.Add(1)
	fmt.Println(messageCountOneOne.Load())
	if messageCountOneOne.Load() == MessageCount {
		doneOneOne <- true
	}
	return nil
}

func authenticityHandlerNOne(e *proto.Event) error {
	testMapNOne[string(e.Body)] = true
	return nil
}

func resendHandler(e *proto.Event) error {

	if !canProcess {
		canProcess = true
		fmt.Println("First time")
		return errors.New("Couldn't process")
	}
	fmt.Println("Second time")
	gotSecondTime = true

	return nil
}
