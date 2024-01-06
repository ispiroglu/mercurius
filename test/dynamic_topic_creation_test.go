package broker_test

import (
	"context"
	"github.com/google/uuid"
	"github.com/ispiroglu/mercurius/pkg/client"
	"github.com/stretchr/testify/assert"
	"reflect"
	"strconv"
	"sync"
	"testing"
	"time"
)

const topicCount = 100

var topicTestMap = &SafeMap{m: make(map[string]bool), Mutex: sync.Mutex{}}

// TODO: this test.
func TestDynamicTopicCreation(t *testing.T) {
	t.Run("Messages sent and received should be the same", func(t *testing.T) {
		var controlMap = make(map[string]bool)

		for i := 0; i < MessageCount; i++ {
			controlMap[strconv.Itoa(i)] = true
		}

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

		// Send request to get topics

		assert.Equal(t, true, reflect.DeepEqual(testMapOneN.m, controlMap))
	})

	t.Cleanup(func() {
		topicTestMap = &SafeMap{m: make(map[string]bool), Mutex: sync.Mutex{}}
	})
}
