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
const TopicName = "one-to-one"
const MessageCount = 500

var testMap map[string]bool = make(map[string]bool)

func TestMessageAuthenticity(t *testing.T) {
	t.Run("Messages sent and received should be the same", func(t *testing.T) {
		var controlMap = make(map[string]bool)

		for i := 0; i < MessageCount; i++ {
			controlMap[strconv.Itoa(i)] = true
		}

		cPub, _ := client.NewClient("pub", ADDR)

		cSub, _ := client.NewClient("sub", ADDR)

		cSub.Subscribe(TopicName, context.Background(), authenticityHandler)

		for i := 0; i < MessageCount; i++ {
			cPub.Publish(TopicName, []byte(fmt.Sprintf("%d", i)), context.Background())
		}

		time.Sleep(1 * time.Second)
		fmt.Println(testMap)
		fmt.Println(controlMap)

		assert.Equal(t, true, reflect.DeepEqual(testMap, controlMap))
	})

}

func authenticityHandler(e *proto.Event) error {
	testMap[string(e.Body)] = true
	return nil
}
