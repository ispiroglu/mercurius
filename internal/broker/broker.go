package broker

import (
	"context"
	"github.com/ispiroglu/mercurius/internal/logger"
	pb "github.com/ispiroglu/mercurius/proto"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Broker struct {
	logger *zap.Logger
	*TopicRepository
}

type IBroker interface {
	Publish(event *pb.Event) (*pb.ACK, error)
	Subscribe(ctx context.Context, topic string, sId string, sName string) (<-chan *pb.Event, error)
}

func NewBroker() *Broker {
	return &Broker{
		logger:          logger.NewLogger(),
		TopicRepository: NewTopicRepository(),
	}
}

func (b *Broker) Publish(event *pb.Event) (*pb.ACK, error) {
	b.logger.Info("Broker received event for publishing", zap.String("Topic", event.Topic))

	topic, err := b.GetTopic(event.Topic)
	if err != nil {
		b.logger.Warn("Broker cannot found the topic to publish", zap.String("Topic", event.Topic), zap.Error(err))
		topic, err = b.CreateTopic(event.Topic)

		st, ok := status.FromError(err)
		if !ok && st.Code() != codes.AlreadyExists {
			b.logger.Error("Broker cannot create topic", zap.String("Topic", event.Topic), zap.Error(err))
			return nil, err
		}

		topic, _ = b.GetTopic(event.Topic)
	}

	topic.PublishEvent(event)
	return &pb.ACK{}, nil
}

func (b *Broker) Subscribe(ctx context.Context, topicName string, sId string, sName string) (<-chan *pb.Event, error) {
	b.logger.Info("Broker received subscription request", zap.String("Topic", topicName), zap.String("SubscriberID", sId))

	t, err := b.GetTopic(topicName)
	if err != nil {
		b.logger.Warn("Broker cannot found the topic to subscribe", zap.String("Topic", topicName), zap.Error(err))
		t, err = b.CreateTopic(topicName)

		st, ok := status.FromError(err)
		b.logger.Info("", zap.Int("Code", int(st.Code())), zap.Bool("OK", ok))
		if !ok && st.Code() != codes.AlreadyExists {
			b.logger.Error("Broker cannot create topic", zap.String("Topic", topicName), zap.Error(err), zap.Bool("A", st.Code() != codes.AlreadyExists), zap.String("Code", st.String()))
			return nil, err
		}

		t, err = b.GetTopic(topicName)
	}

	ch, err := t.AddSubscriber(ctx, sId, sName)
	if err != nil {
		b.logger.Error("Broker could not add subscriber to topic", zap.String("Topic", topicName), zap.String("SubscriberID", sId), zap.Error(err))
		return nil, err
	}
	return ch, nil
}
