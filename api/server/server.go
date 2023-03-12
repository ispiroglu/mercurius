package main

import (
	"bytes"
	"context"
	"encoding/gob"
	"fmt"
	pb "github.com/ispiroglu/mercurius/proto"
	"google.golang.org/grpc"
	"log"
	"net"
)

type sampleJson struct {
	Id int
}

type Server struct {
	pb.UnimplementedEventServiceServer
}

func (s *Server) Post(ctx context.Context, req *pb.Event) (*pb.EventResponse, error) {
	fmt.Println("Has Request: ", req.UUID, "234")
	return &pb.EventResponse{Ack: true}, nil
}

func (s *Server) PostStream(stream pb.EventService_PostStreamServer) error {
	req, err := stream.Recv()
	if err != nil {
		log.Fatalf("stream Recv! %v", err)

	}
	var buf = bytes.NewBuffer(req.GetPayload())
	decoder := gob.NewDecoder(buf)

	var incomingRequest sampleJson

	err = decoder.Decode(&incomingRequest)
	if err != nil {
		fmt.Println("cannot decode")
	}

	fmt.Printf("Recieved Message: %s\n", incomingRequest)
	err = stream.SendMsg(&pb.EventResponse{Ack: true})
	if err != nil {
		log.Fatalln("Error on sern", err)
	}
	defer func() {}()

	return nil
}

func main() {
	fmt.Println("gRPC")

	list, err := net.Listen("tcp", "0.0.0.0:9000")
	if err != nil {
		panic(err)
	}

	protoServer := Server{}
	grpcServer := grpc.NewServer()
	pb.RegisterEventServiceServer(grpcServer, &protoServer)

	if err := grpcServer.Serve(list); err != nil {
		log.Fatalf("Failed to serve: %s", err)
	}
}
