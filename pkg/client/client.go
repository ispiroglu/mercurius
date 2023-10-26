package client

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"

	"github.com/google/uuid"
	"github.com/ispiroglu/mercurius/internal/logger"
	"github.com/ispiroglu/mercurius/pkg/serialize"
	"github.com/ispiroglu/mercurius/proto"
	"go.uber.org/zap"

	"google.golang.org/protobuf/types/known/timestamppb"
)

type Client struct {
	Name string
	c    proto.MercuriusClient
	s    *serialize.Serializer
}

var l = logger.NewLogger()

// Where to locate defer conn.Close()
func NewClient(name string, addr string) (*Client, error) {
	conn := getConnection(addr)
	if conn == nil {
		l.Error("could not Create a connection")
		return nil, errors.New("could not Create a connection")
	}

	c := proto.NewMercuriusClient(conn)
	l.Info("Created the client")

	return &Client{
		c:    c,
		Name: name,
		s:    serialize.NewSerializer(),
	}, nil
}

func (client *Client) Subscribe(topicName string, ctx context.Context, fn func(event *proto.Event) error) error {
	r := client.createSubRequest(topicName)
	reqCount := atomic.Uint32{}
	subStream, err := client.c.Subscribe(ctx, r)
	if err != nil {
		return err
	}

	go func() {
		for {
			bulkEvent, err := subStream.Recv()
			fmt.Println("--------------", reqCount.Add(1))
			if err != nil {
				// TODO: What if cannot receive?
				l.Error("", zap.Error(err))
				panic(err)
			}

			go func() {
				for _, v := range bulkEvent.EventList {
					go fn(v)
				}
			}()
		}
	}()

	return nil
}

// Publish This function needs to be sync in order to be able to handle error on publish.
func (client *Client) Publish(topicName string, body []byte, ctx context.Context) error {
	e, err := client.createEvent(topicName, body)
	if err != nil {
		return err
	}

	_, err = client.c.Publish(ctx, e)
	return err
}

func (client *Client) retry(ctx context.Context, e *proto.Event, subId string) error {
	r := &proto.RetryRequest{
		SubscriberID: subId,
		Event:        e,
		CreatedAt:    timestamppb.Now(),
	}
	ack, err := client.c.Retry(ctx, r)
	if err != nil || ack != nil {
		return err
	}

	return nil
}

func (client *Client) createSubRequest(topicName string) *proto.SubscribeRequest {
	return &proto.SubscribeRequest{
		SubscriberID:   uuid.NewString(),
		SubscriberName: fmt.Sprintf("%s:%s", client.Name, topicName),
		Topic:          topicName,
		CreatedAt:      timestamppb.Now(),
	}
}

func (client *Client) createEvent(topicName string, body []byte) (*proto.Event, error) {
	//b, err := client.s.Encode(body)
	//if err != nil {
	//	l.Error("error while encoding the event", zap.Any("Event", body), zap.Error(err))
	//	return nil, errors.New("error while encoding the event")
	//}
	e := proto.Event{
		Id:        uuid.NewString(),
		Topic:     topicName,
		Body:      body,
		CreatedAt: timestamppb.Now(),
		ExpiresAt: 0, // TODO:
	}

	return &e, nil
}
