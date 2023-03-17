package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/ispiroglu/mercurius/pkg/serialize"
	"github.com/ispiroglu/mercurius/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const ADDR = "0.0.0.0:9000"

// TODO: Create Client Struct!
func main() {
	conn, err := grpc.Dial(ADDR, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("Failed to dial to %s, %v", ADDR, err)
	}
	defer conn.Close()

	c := proto.NewMercuriusClient(conn)
	s := serialize.NewSerializer()

	fmt.Println("Dialled to", ADDR)

	eventBody, err := s.Encode("Sample event body")
	if err != nil {
		log.Println("Cannout encode event body")
	}

	event := &proto.Event{
		Id:        "ID",
		Topic:     "Sample Topic Name",
		Body:      eventBody,
		CreatedAt: timestamppb.Now(),
		ExpiresAt: 1232,
	}

	count := 0
	for count < 10 {
		time.Sleep(250 * time.Microsecond)

		ack, err := c.Publish(context.Background(), event)
		if err != nil {
			log.Println("Cannot publish event")
		}

		if ack != nil {
			fmt.Println("Ack Aldim")
		}
		count++
	}
}
