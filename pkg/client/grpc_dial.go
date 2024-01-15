package client

import (
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func getConnection(addr string) *grpc.ClientConn {
	c, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock())
	if err != nil {
		panic(err)
	}
	return c
}
