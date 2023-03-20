package broker

import (
	"context"
	"github.com/ispiroglu/mercurius/internal/logger"
	pb "github.com/ispiroglu/mercurius/proto"
	"go.uber.org/zap"
)

// Broker TODO: Should we have broker interface instead of struct?
type Broker struct {
	logger *zap.Logger
	*TopicRepository
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
		if err != nil {
			b.logger.Error("Broker cannot create topic", zap.String("Topic", event.Topic), zap.Error(err))
			return nil, err
		}
	}

	topic.PublishEvent(event)
	return &pb.ACK{}, nil
}

// Subscribe Who is the subscriber? How to handle fanouts??
func (b *Broker) Subscribe(ctx context.Context, topic string, sId string, sName string) (<-chan *pb.Event, error) {
	b.logger.Info("Broker received subscription request", zap.String("Topic", topic), zap.String("SubscriberID", sId))
	t, err := b.GetTopic(topic)
	if err != nil {
		b.logger.Warn("Broker cannot found the topic to subscribe", zap.String("Topic", topic), zap.Error(err))
		t, err = b.CreateTopic(topic)
		if err != nil {
			b.logger.Error("Broker cannot create topic", zap.String("Topic", topic), zap.Error(err))
			return nil, err
		}
	}

	if err := t.AddSubscriber(ctx, sId, sName); err != nil {
		b.logger.Error("Broker could not add subscriber to topic", zap.String("Topic", topic), zap.String("SubscriberID", sId), zap.Error(err))
		return nil, err
	}

	return t.Subscribers[sId].EventChannel, nil
}
