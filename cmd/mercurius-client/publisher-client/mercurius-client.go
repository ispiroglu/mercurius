package main

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"

	client_example "github.com/ispiroglu/mercurius/cmd/mercurius-client"
	logger2 "github.com/ispiroglu/mercurius/internal/logger"
	"github.com/ispiroglu/mercurius/pkg/client"
	"go.uber.org/zap"
)

const ADDR = "0.0.0.0:9000"
const TopicName = "one-to-one"
const CLIENT_NAME = "Sample Client"

var logger = logger2.NewLogger()
var messageCount = atomic.Uint64{}

var start time.Time

func main() {

	logger.Info("Published Event")
	var z time.Duration
	_ = z
	wg := sync.WaitGroup{}
	wg.Add(client_example.PublisherCount)
	client_example.StartTime = time.Now()
	fmt.Println(client_example.StartTime)

	signal := make(chan struct{})
	for i := 0; i < client_example.PublisherCount; i++ {
		id, _ := uuid.NewUUID()
		c, err := client.NewClient(id, ADDR)
		if err != nil {
			logger.Error("Err", zap.Error(err))
		}

		go func(w *sync.WaitGroup, ch chan struct{}, cl *client.Client) {
			for j := 0; j < client_example.PublishCount; j++ {
				<-ch
				x := messageCount.Add(1)
				if x == 1 {
					start = time.Now()
				}

				if err := cl.Publish(TopicName, []byte(strconv.FormatUint(x, 10)), context.Background()); err != nil {
					logger.Error("Err", zap.Error(err))
					panic(err)
				}

				fmt.Println("event sent: ", x)
				if x == client_example.TotalPublishCount {
					z = time.Since(start)
				}
			}
			w.Done()
		}(&wg, signal, c)
	}

	time.Sleep(1 * time.Second)
	close(signal)
	wg.Wait()
	// fmt.Println("Execution time: ", z)

}

// package main

// import (
// 	"context"
// 	"fmt"
// 	"strconv"
// 	"sync"
// 	"sync/atomic"
// 	"time"

// 	logger2 "github.com/ispiroglu/mercurius/internal/logger"
// 	"github.com/ispiroglu/mercurius/pkg/client"
// 	"go.uber.org/zap"
// )

// const ADDR = "0.0.0.0:9000"
// const TopicName = "one-to-one"
// const CLIENT_NAME = "Sample Client"

// var logger = logger2.NewLogger()
// var messageCount = atomic.Uint64{}
// var N = 1

// func main() {
// 	c, err := client.NewClient(CLIENT_NAME, ADDR)
// 	if err != nil {
// 		logger.Error("Err", zap.Error(err))
// 	}

// 	logger.Info("Published Event")

// 	wg := sync.WaitGroup{}
// 	wg.Add(N)

// 	for i := 0; i < N; i++ {
// 		go func(w *sync.WaitGroup) {
// 			for j := 0; j < 100; j++ {
// 				if err := c.Publish(strconv.Itoa(j), []byte(strconv.FormatUint(messageCount.Load(), 10)), context.Background()); err != nil {
// 					logger.Error("Err", zap.Error(err))
// 				}
// 				fmt.Println(strconv.FormatUint(messageCount.Load(), 10))

// 				messageCount.Add(1)
// 			}
// 			w.Done()
// 		}(&wg)
// 	}

// 	wg.Wait()

// 	time.Sleep(1 * time.Hour)
// }
