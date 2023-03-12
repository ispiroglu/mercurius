package main

import (
	"bytes"
	"context"
	"encoding/gob"
	"fmt"
	"github.com/ispiroglu/mercurius/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"log"
	"time"
)

type sampleJson struct {
	Id int
}

var sampleJsonPayload = sampleJson{
	Id: 1,
}

func main() {
	fmt.Println("gRPC Client")

	var buf bytes.Buffer
	encoder := gob.NewEncoder(&buf)

	conn, err := grpc.Dial("0.0.0.0:9000", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("Failed to connect: %v", err)
	}

	defer conn.Close()

	c := proto.NewEventServiceClient(conn)
	err = encoder.Encode(&sampleJsonPayload)
	if err != nil {
		log.Println(err)
	}

	if err != nil {
		log.Fatalln(err)
	}

	streamEvent := &proto.Event{
		UUID:    "Stream id",
		Payload: buf.Bytes(),
	}

	buf2 := bytes.NewBuffer(streamEvent.Payload)
	dec := gob.NewDecoder(buf2)

	var sampleDecoded sampleJson

	err = dec.Decode(&sampleDecoded)
	if err != nil {
		log.Println("Error on decode lol")
	}

	fmt.Println("Sample : ", sampleDecoded)
	fmt.Println("Sample Decoded: ", sampleDecoded)

	fmt.Println("!")
	count := 0
	for count < 240 {
		time.Sleep(250 * time.Millisecond)

		//post, err2 := c.Post(context.Background(), streamEvent)
		//if err2 != nil {
		//	log.Fatalln(err2)
		//}

		stream, err := c.PostStream(context.Background())
		if err != nil {
			log.Fatalln(err)
		}

		fmt.Println(streamEvent)

		err3 := stream.Send(streamEvent)
		if err3 != nil {
			log.Fatalf("%v", err)
		}

		//if err2 != nil {
		//	log.Fatalf("error on response: %v", err2)
		//}
		count++

		//fmt.Println("Response ", post)
	}
	fmt.Println("2")
}
