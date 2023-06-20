package main

import (
	"context"
	"fmt"
	"sync"
	"time"

	logger2 "github.com/ispiroglu/mercurius/internal/logger"
	"github.com/ispiroglu/mercurius/pkg/client"
	"go.uber.org/zap"
)

const ADDR = "0.0.0.0:9000"
const TopicName = "one-to-one"
const CLIENT_NAME = "Sample Client"

var logger = logger2.NewLogger()

func main() {
	c, err := client.NewClient(CLIENT_NAME, ADDR)
	if err != nil {
		logger.Error("Err", zap.Error(err))
	}

	cc := 50

	wg := sync.WaitGroup{}
	wg.Add(cc)
	for j := 0; j < cc; j++ {
		go func(w *sync.WaitGroup, j int) {
			for i := 0; i < 100; i++ {
				body := []byte(fmt.Sprintf("This is the sample message: %d", j*100+i))
				fmt.Println(string(body))
				if err := c.Publish(TopicName, body, context.Background()); err != nil {
					logger.Error("Err", zap.Error(err))
				}
				time.Sleep(500 * time.Microsecond)
			}
			w.Done()
		}(&wg, j)
	}

	wg.Wait()
}
