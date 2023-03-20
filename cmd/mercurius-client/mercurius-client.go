package main

import (
	"context"
	"fmt"
	"github.com/google/uuid"
	"github.com/ispiroglu/mercurius/pkg/serialize"
	"github.com/ispiroglu/mercurius/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/timestamppb"
	"log"
	"time"
)

const ADDR = "0.0.0.0:9000"
const TopicName = "|USER_CREATED|"

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
		log.Println("Cannot encode event body")
	}

	event := &proto.Event{
		Id:        "ID",
		Topic:     TopicName,
		Body:      eventBody,
		CreatedAt: timestamppb.Now(),
		ExpiresAt: 1232,
	}

	go func() {
		count := 0
		for ; count < 250; count++ {
			time.Sleep(2 * time.Second)
			_, err := c.Publish(context.Background(), event)
			if err != nil {
				log.Println("Cannot publish event")
				log.Println(err)
			}
		}
	}()

	// SubA Scope
	{
		sReqA := &proto.SubscribeRequest{
			SubscriberID:   uuid.New().String(),
			SubscriberName: "Sub-A",
			Topic:          TopicName,
			CreatedAt:      timestamppb.Now(),
		}

		clientA, err := c.Subscribe(context.Background(), sReqA)
		if err != nil {
			log.Println(err)
		}

		go func() {
			for {
				event, err := clientA.Recv()
				if err != nil {
					log.Println(err)
					// TODO: Handle mechanism
				}

				// Handle Event!
				log.Println("Received event on ClientA", event)
			}
		}()
	}

	// SubB Scope
	{
		sReqB := &proto.SubscribeRequest{
			SubscriberID:   uuid.New().String(),
			SubscriberName: "Sub-B",
			Topic:          TopicName,
			CreatedAt:      timestamppb.Now(),
		}

		clientB, err := c.Subscribe(context.Background(), sReqB)
		if err != nil {
			log.Println(err)
		}

		go func() {
			for {
				event, err := clientB.Recv()
				if err != nil {
					log.Println(err)
					// TODO: Handle mechanism
				}

				// Handle Event!
				log.Println("Received event on ClientB", event)
			}
		}()
	}

	time.Sleep(5 * time.Hour)
}
