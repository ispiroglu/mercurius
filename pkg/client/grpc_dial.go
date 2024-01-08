package client

import (
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var (
	conn *grpc.ClientConn
	o    sync.Once
)

func getConnection(addr string) *grpc.ClientConn {
	/*.Do(func() {
		c, err := grpc.Dial(addr,
			grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			conn = nil
		} else {
			conn = c
		}
	})*/

	c, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock())
	if err != nil {
		panic(err)
	}
	// c, _ := grpc.Dial(addr,
	// 	grpc.WithTransportCredentials(insecure.NewCredentials()),
	// 	grpc.WithInitialWindowSize(1024*1024),
	// 	grpc.WithInitialConnWindowSize(1024*1024))
	return c
}
