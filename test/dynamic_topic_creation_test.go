package broker_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"reflect"
	"strconv"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/ispiroglu/mercurius/pkg/client"
	"github.com/stretchr/testify/assert"
)

const topicCount = 100

func TestDynamicTopicCreation(t *testing.T) {
	t.Run("Messages sent and received should be the same", func(t *testing.T) {
		var controlMap = make(map[string]bool)

		for i := 0; i < topicCount; i++ {
			controlMap[strconv.Itoa(i)] = true
		}

		signalChannel := make(chan struct{})
		for i := 0; i < topicCount; i++ {
			topicName := fmt.Sprint(i)
			subId, _ := uuid.NewUUID()
			cSub, _ := client.NewClient(subId, ADDR)
			go func(i int, signalChannel chan struct{}, cSub *client.Client) {
				<-signalChannel
				_ = cSub.Subscribe(topicName, context.Background(), nil)
			}(i, signalChannel, cSub)
		}

		close(signalChannel)
		time.Sleep(2 * time.Second)

		// Send request to localhost:8080/topics to get all the topics created in server
		topics := GetAllTopics()

		assert.Equal(t, true, reflect.DeepEqual(topics, controlMap))
	})

}

func GetAllTopics() map[string]bool {
	resp, err := http.Get("http://localhost:8080/topics")
	if err != nil {
		panic(err)
	}
	defer resp.Body.Close() // nolint:errcheck

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		panic(err)
	}

	var topics map[string]bool
	err = json.Unmarshal(body, &topics)
	if err != nil {
		panic(err)
	}

	return topics
}
