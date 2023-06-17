package client

import (
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"sync"
)

var (
	conn *grpc.ClientConn
	o    sync.Once
)

func getConnection(addr string) *grpc.ClientConn {
	o.Do(func() {
		c, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			conn = nil
		} else {
			conn = c
		}
	})

	return conn
}
