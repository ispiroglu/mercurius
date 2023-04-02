package main

import (
	"context"
	"fmt"
	"github.com/ispiroglu/mercurius/pkg/serialize"
	"github.com/ispiroglu/mercurius/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/timestamppb"
	"log"
	"sync/atomic"
	"time"
)

const ADDR = "0.0.0.0:9000"
const TopicName = "SampleTopicName"

type counter struct {
	a atomic.Int32
}

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

	_ = eventBody
	count := 1

	go func() {
		//time.Sleep(3 * time.Second)
		for ; count <= 1000; count++ {
			go func(count int) {
				event := &proto.Event{
					Id:        "ID",
					Topic:     TopicName,
					Body:      eventBody,
					CreatedAt: timestamppb.Now(),
					ExpiresAt: uint32(count),
				}
				//time.Sleep(2 * time.Second)
				//fmt.Println(count)
				_, err := c.Publish(context.Background(), event)
				if err != nil {
					log.Println("Cannot publish event")
					log.Println(err)
				}
			}(count)
		}
	}()
	a := counter{a: atomic.Int32{}}
	// SubA Scope
	{
		x := 1
		for ; x <= 1000; x++ {
			go func(x int) {
				n := fmt.Sprintf("Sub%d", x)
				sReqA := &proto.SubscribeRequest{
					SubscriberID:   n,
					SubscriberName: n,
					Topic:          TopicName,
					CreatedAt:      timestamppb.Now(),
				}

				clientA, err := c.Subscribe(context.Background(), sReqA)
				if err != nil {
					log.Println(err)
				}

				for {
					event, err := clientA.Recv()
					if err != nil {
						log.Println(err)
						// TODO: Handle mechanism
					}

					// Handle Event!
					log.Println("Received event on", sReqA.SubscriberID, event)
					a.a.Add(1)
				}
			}(x)
		}
	}

	// SubB Scope
	//{
	//	sReqB := &proto.SubscribeRequest{
	//		SubscriberID:   uuid.New().String(),
	//		SubscriberName: "Sub-B",
	//		Topic:          TopicName,
	//		CreatedAt:      timestamppb.Now(),
	//	}
	//
	//	clientB, err := c.Subscribe(context.Background(), sReqB)
	//	if err != nil {
	//		log.Println(err)
	//	}
	//
	//	go func() {
	//		for {
	//			event, err := clientB.Recv()
	//			if err != nil {
	//				log.Println(err)
	//				// TODO: Handle mechanism
	//			}
	//
	//			// Handle Event!
	//			log.Println("Received event on ClientB", event)
	//		}
	//	}()
	//}

	time.Sleep(60 * time.Second)
	fmt.Println()
	//fmt.Println(a.a)
	time.Sleep(5 * time.Hour)
}
