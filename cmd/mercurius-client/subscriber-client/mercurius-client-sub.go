package main

import (
	"context"
	"fmt"
	logger2 "github.com/ispiroglu/mercurius/internal/logger"
	"github.com/ispiroglu/mercurius/pkg/serialize"
	"github.com/ispiroglu/mercurius/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/timestamppb"
	"log"
	"os"
	"time"
)

const ADDR = "0.0.0.0:9000"

var logger = logger2.NewLogger()

func main() {
	conn, err := grpc.Dial(ADDR, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("Failed to dial to %s, %v", ADDR, err)
	}
	defer conn.Close()

	c := proto.NewMercuriusClient(conn)
	s := serialize.NewSerializer()

	logger.Info("Dialled to: " + ADDR)

	clientName := os.Args[1]
	topicName := os.Args[2]

	logger.Info("Client Name: " + clientName)
	logger.Info("Topic Name: " + topicName)

	x := 250000
	for x > 0 {
		//time.Sleep(1 * time.Second)
		go func(y int) {
			//println(y)
			sReq := &proto.SubscribeRequest{
				SubscriberID:   fmt.Sprintf("%d", y),
				SubscriberName: fmt.Sprintf("%d", y),
				Topic:          topicName,
				CreatedAt:      timestamppb.Now(),
			}

			ctx, fn := context.WithCancel(context.Background())
			client, err := c.Subscribe(ctx, sReq)
			if err != nil {
				log.Println(err)
			}
			_ = client
			//count := 0
			_ = s
			for {
				//go func() {
				//	time.Sleep(5 * time.Second)
				//	fn()
				//}()
				event, err := client.Recv()
				if err != nil {
					log.Println(err)
				}
				//
				println(event)
				_ = fn
			}
		}(x)

		x--
	}

	x = 2500000
	for x > 0 {

		time.Sleep(1 * time.Second)
		//time.Sleep(1 * time.Second)
		go func(y int) {
			println(y)
			sReq := &proto.SubscribeRequest{
				SubscriberID:   fmt.Sprintf("%d1", y),
				SubscriberName: fmt.Sprintf("%d1", y),
				Topic:          topicName,
				CreatedAt:      timestamppb.Now(),
			}

			ctx, fn := context.WithCancel(context.Background())
			client, err := c.Subscribe(ctx, sReq)
			if err != nil {
				log.Println(err)
			}
			_ = client
			//count := 0
			_ = s
			for {
				_ = fn

				//go func() {
				//	time.Sleep(5 * time.Second)
				//	fn()
				//}()
				event, err := client.Recv()
				if err != nil {
					log.Println(err)
				}
				//
				println(event)

			}
		}(x)

		x--
	}

	time.Sleep(99999 * time.Second)
	//go func(c *int) {
	//	time.Sleep(180 * time.Second)
	//
	//	logger.Info("", zap.Int("Received event count", *c))
	//}(&count)
	//for {
	//	event, err := client.Recv()
	//	if err != nil {
	//		log.Println(err)
	//		return
	//	}
	//
	//	//logger.Info("Got Event")
	//	body := ""
	//	err = s.Decode(event.Body, &body)
	//	if err != nil {
	//		log.Println(err)
	//		return
	//	}
	//	logger.Info("Event body -> " + body)
	//
	//	count++
	//
	//}
}
