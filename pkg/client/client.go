package client

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	client_example "github.com/ispiroglu/mercurius/cmd/mercurius-client"
	"github.com/ispiroglu/mercurius/internal/logger"
	"github.com/ispiroglu/mercurius/pkg/serialize"
	"github.com/ispiroglu/mercurius/proto"
	"go.uber.org/zap"

	"google.golang.org/protobuf/types/known/timestamppb"
)

type Client struct {
	id uuid.UUID
	c  proto.MercuriusClient
	s  *serialize.Serializer
}

type EventHandler func(event *proto.Event) error

var l = logger.NewLogger()

// Where to locate defer conn.Close()
func NewClient(id uuid.UUID, addr string) (*Client, error) {
	conn := getConnection(addr)
	if conn == nil {
		l.Error("could not Create a connection")
		return nil, errors.New("could not Create a connection")
	}

	c := proto.NewMercuriusClient(conn)
	l.Info("Created the client")

	return &Client{
		id: id,
		c:  c,
		s:  serialize.NewSerializer(),
	}, nil
}

var messageCount = atomic.Uint64{}
var start time.Time

func (client *Client) Subscribe(topicName string, ctx context.Context, fn func(event *proto.Event) error) error {

	for i := 0; i < client_example.StreamPerSubscriber; i++ {
		go func(x int) {
			r := client.createSubRequest(topicName, x)
			subStream, err := client.c.Subscribe(ctx, r)
			if err != nil {
				panic(err)
			}

			go func(stream *proto.Mercurius_SubscribeClient) {
				for {

					bulkEvent, err := (*stream).Recv()
					// time.Sleep(1 * time.Microsecond)
					y := messageCount.Add(1)
					if y == 1 {
						start = time.Now()
					}
					// fmt.Println(string(e.Body))
					if y == client_example.TotalReceiveCount {

						z := time.Since(start)
						fmt.Println("Execution time: ", z)
					}

					if err != nil {
						// TODO: What if cannot receive?
						l.Error("", zap.Error(err))
						panic(err)

					}
					fmt.Println("recevied from: ", x)
					go func() {
						for _, v := range bulkEvent.EventList {
							go func(e *proto.Event) {
								err := fn(e)
								if err != nil {
									client.retry(ctx, e, r.SubscriberName) //nolint:errcheck
								}
							}(v)
						}
					}()
				}
			}(&subStream)

		}(i)
	}

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

func (client *Client) retry(ctx context.Context, e *proto.Event, subName string) error {
	r := &proto.RetryRequest{
		SubscriberID: subName,
		Event:        e,
		CreatedAt:    timestamppb.Now(),
	}
	ack, err := client.c.Retry(ctx, r)
	if err != nil || ack != nil {
		return err
	}

	return nil
}

func (client *Client) createSubRequest(topicName string, x int) *proto.SubscribeRequest {
	subName := fmt.Sprintf("%s", client.id)
	fmt.Println(subName)
	return &proto.SubscribeRequest{
		SubscriberID:   fmt.Sprintf("%d", x),
		SubscriberName: subName,
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
