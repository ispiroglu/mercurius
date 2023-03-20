package main

import (
	"github.com/ispiroglu/mercurius/internal/logger"
	"go.uber.org/zap"
	"net"

	sv "github.com/ispiroglu/mercurius/internal/server"
	"github.com/ispiroglu/mercurius/proto"
	"google.golang.org/grpc"
)

const ADDR = "0.0.0.0:9000"
const TCP = "tcp"

var log = logger.NewLogger()

func main() {
	list, err := net.Listen(TCP, ADDR)
	if err != nil {
		log.Fatal("Cannot listen", zap.String("TCP", TCP), zap.String("ADDR", ADDR), zap.Error(err))
	}
	log.Info("Listening")

	grpcServer := grpc.NewServer()
	server := sv.NewMercuriusServer()

	proto.RegisterMercuriusServer(grpcServer, server)

	if err := grpcServer.Serve(list); err != nil {
		log.Fatal("Failed to serve", zap.Error(err))
	}
}
