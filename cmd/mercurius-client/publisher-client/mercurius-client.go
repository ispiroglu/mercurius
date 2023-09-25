package main

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"

	logger2 "github.com/ispiroglu/mercurius/internal/logger"
	"github.com/ispiroglu/mercurius/pkg/client"
	"go.uber.org/zap"
)

const ADDR = "0.0.0.0:9000"
const TopicName = "one-to-one"
const CLIENT_NAME = "Sample Client"

var logger = logger2.NewLogger()
var messageCount = atomic.Uint64{}
var N = 100

func main() {
	c, err := client.NewClient(CLIENT_NAME, ADDR)
	if err != nil {
		logger.Error("Err", zap.Error(err))
	}

	logger.Info("Published Event")

	wg := sync.WaitGroup{}
	wg.Add(N)
	for i := 0; i < N; i++ {
		go func(w *sync.WaitGroup) {
			for j := 0; j < 100; j++ {
				x := messageCount.Add(1)
				if err := c.Publish(TopicName, []byte(strconv.FormatUint(x, 10)), context.Background()); err != nil {
					logger.Error("Err", zap.Error(err))
				}
				fmt.Println(strconv.FormatUint(x, 10))
				//time.Sleep(time.Millisecond)
			}
			w.Done()
		}(&wg)
		//time.Sleep(200 * time.Second)
	}

	wg.Wait()
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
