package main

import (
	"context"
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

	sReq := &proto.SubscribeRequest{
		SubscriberID:   clientName,
		SubscriberName: clientName,
		Topic:          topicName,
		CreatedAt:      timestamppb.Now(),
	}

	client, err := c.Subscribe(context.Background(), sReq)
	if err != nil {
		log.Println(err)
	}
	_ = client
	//count := 0
	_ = s
	for {
		//event, err := client.Recv()
		//if err != nil {
		//	log.Println(err)
		//}
		//
		//println(event)
		time.Sleep(500 * time.Second)

		break
	}

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
