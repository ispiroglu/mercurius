package broker_test

import (
	"context"
	"fmt"
	"reflect"
	"strconv"
	"testing"
	"time"

	"github.com/ispiroglu/mercurius/pkg/client"
	"github.com/ispiroglu/mercurius/proto"
	"github.com/stretchr/testify/assert"
)

const ADDR = "0.0.0.0:9000"
const TopicName = "top"
const MessageCount = 1000
const n = 10

var testMapOneOne = make(map[string]bool)
var testMapNOne = make(map[string]bool)

func TestOneOneMessageReliability(t *testing.T) {
	t.Run("Messages sent and received should be the same", func(t *testing.T) {
		var controlMap = make(map[string]bool)

		for i := 0; i < MessageCount; i++ {
			controlMap[strconv.Itoa(i)] = true
		}

		cPub, _ := client.NewClient("pub", ADDR)

		cSub, _ := client.NewClient("sub", ADDR)

		cSub.Subscribe(TopicName, context.Background(), authenticityHandlerOneOne)

		for i := 0; i < MessageCount; i++ {
			cPub.Publish(TopicName, []byte(fmt.Sprintf("%d", i)), context.Background())
		}

		time.Sleep(3 * time.Second)

		assert.Equal(t, true, reflect.DeepEqual(testMapOneOne, controlMap))
	})

}

func TestNOneMessageReliability(t *testing.T) {
	t.Run("Messages sent and received should be the same", func(t *testing.T) {
		var controlMap = make(map[string]bool)

		for i := 0; i < MessageCount; i++ {
			controlMap[strconv.Itoa(i)] = true
		}

		cPub, _ := client.NewClient("pub", ADDR)

		cSub, _ := client.NewClient("sub", ADDR)

		cSub.Subscribe(TopicName, context.Background(), authenticityHandlerNOne)

		for i := 0; i < MessageCount; i++ {
			if i%(MessageCount/n) == 0 {
				cPub, _ = client.NewClient("pub", ADDR)
				fmt.Println("Changed publisher")
			}
			cPub.Publish(TopicName, []byte(fmt.Sprintf("%d", i)), context.Background())
		}

		time.Sleep(3 * time.Second)

		assert.Equal(t, true, reflect.DeepEqual(testMapOneOne, controlMap))
	})

}

func authenticityHandlerOneOne(e *proto.Event) error {
	testMapOneOne[string(e.Body)] = true
	return nil
}

func authenticityHandlerNOne(e *proto.Event) error {
	testMapNOne[string(e.Body)] = true
	return nil
}
