package client

import (
	"context"
	"errors"
	"fmt"
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
	subStream, err := client.c.Subscribe(ctx, r)
	if err != nil {
		return err
	}

	go func() {
		for {
			e, err := subStream.Recv()
			if err != nil {
				// TODO: What if cannot receive?
				continue
			}

			l.Info("Received Event", zap.String("Client", client.Name), zap.String("Topic", e.Topic))
			err = fn(e)
			if err != nil {
				_ = client.retry(ctx, e, r.SubscriberID)
			}
		}
	}()

	return nil
}

func (client *Client) Publish(topicName string, body any, ctx context.Context) error {
	e, err := client.createEvent(topicName, body)
	if err != nil {
		return err
	}

	ack, err := client.c.Publish(ctx, e)
	if err != nil || ack != nil {
		return err
	}

	return nil
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

func (client *Client) createEvent(topicName string, body any) (*proto.Event, error) {
	b, err := client.s.Encode(body)
	if err != nil {
		l.Error("error while encoding the event", zap.Any("Event", body), zap.Error(err))
		return nil, errors.New("error while encoding the event")
	}
	e := proto.Event{
		Id:        uuid.NewString(),
		Topic:     topicName,
		Body:      b,
		CreatedAt: timestamppb.Now(),
		ExpiresAt: 0, // TODO:
	}

	return &e, nil
}
