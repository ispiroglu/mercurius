package main

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/segmentio/kafka-go"
)

const (
	brokerAddress     = "localhost:9094"
	topicName         = "my-topic"
	publisherCount    = 1000
	publishCount      = 100
	subscriberCount   = 1
	totalPublishCount = publisherCount * publishCount
	totalReceiveCount = subscriberCount * totalPublishCount
	partitionCount    = 1
)

func main() {

	go subClient()
	time.Sleep(3 * time.Second)
	go pubClient()

	time.Sleep(10 * time.Hour)
}

func pubClient() {
	wg := &sync.WaitGroup{}
	wg.Add(totalPublishCount)

	pCount := atomic.Uint64{}
	var start time.Time
	for i := 0; i < publisherCount; i++ {
		writer := kafka.NewWriter(kafka.WriterConfig{
			Brokers:      []string{brokerAddress},
			Topic:        topicName,
			Balancer:     &kafka.RoundRobin{},
			WriteTimeout: 10 * time.Hour,
			ReadTimeout:  10 * time.Hour,
		})

		for j := 0; j < publishCount; j++ {
			go func(wg *sync.WaitGroup, publisherID int, pCount *atomic.Uint64, writer *kafka.Writer) {
				message := kafka.Message{
					Key:   nil,
					Value: []byte(fmt.Sprintf("Message from publisher %d, event %d", publisherID, j)),
				}

				err := writer.WriteMessages(context.Background(), message)
				if err != nil {
					for err != nil {
						fmt.Printf("Failed to write message: %v\n", err)
						writer.WriteMessages(context.Background(), message)
					}
				}

				x := pCount.Add(1)
				fmt.Printf("Publish x: %v\n", x)
				if x == 1 {
					start = time.Now()
				} else if x == totalPublishCount {
					fmt.Println("execution time: ", time.Since(start))
				}

				wg.Done()
				if err := writer.Close(); err != nil {
					fmt.Printf("Failed to close writer: %v\n", err)
				}

			}(wg, i, &pCount, writer)

		}
	}

	wg.Wait()
	fmt.Println("All messages published successfully")
}

func subClient() {
	wg := &sync.WaitGroup{}
	wg.Add(subscriberCount * partitionCount)
	sCount := atomic.Uint64{}
	var start time.Time

	for i := 0; i < subscriberCount; i++ {

		for j := 0; j < partitionCount; j++ {
			reader := kafka.NewReader(kafka.ReaderConfig{
				Brokers:   []string{brokerAddress},
				Topic:     topicName,
				Partition: i % partitionCount,
				MinBytes:  1,
				MaxBytes:  10e6,
			})

			go func(r *kafka.Reader, w *sync.WaitGroup, subscriberID int, sCount *atomic.Uint64) {
				for {
					_, err := r.ReadMessage(context.Background())
					if err != nil {
						fmt.Printf("Failed to read message: %v\n", err)
						break
					}
					x := sCount.Add(1)
					fmt.Printf("x: %v\n", x)
					if x == 1 {
						start = time.Now()
					} else if x == totalReceiveCount {
						t := time.Since(start)

						panic("execution time: " + t.String())
					}

				}

				if err := r.Close(); err != nil {
					fmt.Printf("Failed to close reader: %v\n", err)
				}
				wg.Done()
			}(reader, wg, i, &sCount)
		}
	}

	wg.Wait()
	fmt.Println("All subscribers finished reading")
}
