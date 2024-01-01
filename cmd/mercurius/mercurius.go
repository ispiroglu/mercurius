package main

import (
	"fmt"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"net"
	"net/http"
	_ "net/http/pprof"

	"github.com/ispiroglu/mercurius/internal/logger"
	sv "github.com/ispiroglu/mercurius/internal/server"
	"github.com/ispiroglu/mercurius/proto"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

const ADDR = "0.0.0.0:9000"
const TCP = "tcp"

var log = logger.NewLogger()

func main() {
	go func() {
		fmt.Println(http.ListenAndServe("localhost:6060", nil))
	}()
	list, err := net.Listen(TCP, ADDR)
	if err != nil {
		log.Fatal("Cannot listen", zap.String("TCP", TCP), zap.String("ADDR", ADDR), zap.Error(err))
	}
	log.Info("Listening on: " + ADDR)

	grpcServer := grpc.NewServer()
	server := sv.NewMercuriusServer()

	proto.RegisterMercuriusServer(grpcServer, server)

	go func() {
		http.Handle("/metrics", promhttp.Handler())
		_ = http.ListenAndServe(":8081", nil)
	}()
	if err := grpcServer.Serve(list); err != nil {
		log.Fatal("Failed to serve", zap.Error(err))
	}
}
