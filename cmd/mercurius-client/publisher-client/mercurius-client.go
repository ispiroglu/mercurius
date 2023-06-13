package main

import (
	"context"
	"log"
	"os"
	"strconv"
	"time"

	logger2 "github.com/ispiroglu/mercurius/internal/logger"
	"github.com/ispiroglu/mercurius/pkg/serialize"
	"github.com/ispiroglu/mercurius/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const ADDR = "0.0.0.0:9000"
const TopicName = "SampleTopicName"

var logger = logger2.NewLogger()

//type counter struct {
//	a atomic.Int32
//}

// TODO: Create Client Struct!
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
	countArg := os.Args[3]

	logger.Info("Client Name: " + clientName)
	logger.Info("Topic Name: " + topicName)

	eventBody, err := s.Encode("This event sent by " + clientName + " to topic " + topicName)
	if err != nil {
		log.Println("Cannot encode event body")
	}

	//time.Sleep(1 * time.Second)
	event := &proto.Event{
		Id:        "ID",
		Topic:     topicName,
		Body:      eventBody,
		CreatedAt: timestamppb.Now(),
		ExpiresAt: uint32(250),
	}

	//logger.Info("Publishing event with body")
	//ack, err := c.Publish(context.Background(), event)
	//if err != nil {
	//	log.Println(err)
	//	return
	//}
	//logger.Info("Got ACK: " + ack.String())

	count := 1
	upperBound, _ := strconv.Atoi(countArg)
	go func() {
		for ; count <= upperBound; count++ {
			go func(count int) {
				logger.Info("Publishing event")
				_, err := c.Publish(context.Background(), event)
				if err != nil {
					log.Println("Cannot publish event")
					log.Println(err)

				}
			}(count)
		}
	}()

	time.Sleep(5 * time.Hour)
}
