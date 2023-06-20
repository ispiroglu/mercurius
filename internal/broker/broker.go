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
	SubscriberRepository *SubscriberRepository
}

type IBroker interface {
	Publish(event *pb.Event) (*pb.ACK, error)
	Subscribe(ctx context.Context, topic string, sId string, sName string) (<-chan *pb.Event, error)
}

func NewBroker() *Broker {
	return &Broker{
		logger:               logger.NewLogger(),
		TopicRepository:      NewTopicRepository(),
		SubscriberRepository: NewSubscriberRepository(),
	}
}

func (b *Broker) Publish(event *pb.Event) (*pb.ACK, error) {
	b.logger.Info("Broker received event for publishing", zap.String("Topic", event.Topic))

	t, err := b.findOrInsertTopic(event.Topic)
	if err != nil {
		return nil, err
	}

	t.PublishEvent(event)
	return &pb.ACK{}, nil
}

func (b *Broker) Subscribe(ctx context.Context, topicName string, sId string, sName string) (*Subscriber, error) {
	b.logger.Info("Broker received subscription request", zap.String("Topic", topicName), zap.String("SubscriberID", sId))

	t, err := b.findOrInsertTopic(topicName)
	if err != nil {
		return nil, err
	}

	s, err := t.AddSubscriber(ctx, sId, sName)
	if err != nil {
		b.logger.Error("Broker could not add subscriber to topic", zap.String("Topic", topicName), zap.String("SubscriberID", sId)) //, zap.Error(err))
		return nil, err
	}
	go b.SubscriberRepository.addSub(s)
	return s, nil
}

func (b *Broker) Unsubscribe(sub *Subscriber) {
	b.logger.Info("Unsubscribing", zap.String("ID", sub.Id), zap.String("Subscriber Name", sub.Name))
	b.TopicRepository.Unsubscribe(sub)
	b.SubscriberRepository.Lock()
	delete(b.SubscriberRepository.Subscribers, sub.Id)
	b.SubscriberRepository.Unlock()
}

func (b *Broker) findOrInsertTopic(topicName string) (*Topic, error) {
	t, err := b.GetTopic(topicName)
	if err != nil {
		b.logger.Info("Broker cannot found the topic to subscribe", zap.String("Topic", topicName)) //, zap.Error(err))
		t, err = b.CreateTopic(topicName)

		st, ok := status.FromError(err)
		if !ok && st.Code() != codes.AlreadyExists {
			b.logger.Error("Broker cannot create topic", zap.String("Topic", topicName)) //, zap.Error(err), zap.Bool("A", st.Code() != codes.AlreadyExists), zap.String("Code", st.String()))
			return nil, err
		} else if st.Code() == codes.AlreadyExists {
			t, _ = b.GetTopic(topicName)
		}
	}

	return t, nil
}
