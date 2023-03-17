package main

import (
	"log"
	"net"

	sv "github.com/ispiroglu/mercurius/internal/server"
	"github.com/ispiroglu/mercurius/proto"
	"google.golang.org/grpc"
)

const ADDR = "0.0.0.0:9000"
const TCP = "tcp"

func main() {
	list, err := net.Listen(TCP, ADDR)
	if err != nil {
		log.Fatalf("Cannout listen %s on %s: %v", TCP, ADDR, err)
	}
	log.Printf("Listening %s on %s", TCP, ADDR)

	grpcServer := grpc.NewServer()
	server := sv.NewMercuriusServer()

	proto.RegisterMercuriusServer(grpcServer, server)

	if err := grpcServer.Serve(list); err != nil {
		log.Fatalf("Failed to serve %v", err)
	}
}
