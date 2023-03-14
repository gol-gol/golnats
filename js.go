package golnats

import (
	"context"
	"fmt"
	"log"
	"time"

	nats "github.com/nats-io/nats.go"
)

/*
go-nats facade for pattern-based JetStream usage.
*/

var (
	PubAsyncMaxPending = 256
)

func ConnectNatsJS(natsURLs, stream string, subjects []string, authToken string) (GolNats, error) {
	nc := GolNats{
		URL:         natsURLs,
		ConnName:    fmt.Sprintf("js::%s", stream),
		IsJetStream: true,
		ConnToken:   authToken,
	}
	nc.Connect()

	var errJS error
	nc.JS, errJS = nc.Connection.JetStream(nats.PublishAsyncMaxPending(PubAsyncMaxPending))
	if errJS != nil {
		return nc, errJS
	}

	if len(subjects) == 0 {
		subjects = []string{fmt.Sprintf("%s.>", stream)} // to inherit namespace
	}
	log.Printf("Creating/Adding\n\tstream: %s\n\tsubjects: %v", stream, subjects)
	_, errStream := nc.JS.AddStream(&nats.StreamConfig{
		Name:     stream,
		Subjects: subjects,
	})
	return nc, errStream
}

func (nc *GolNats) AddDurableConsumerJS(stream, consumerID string) error {
	_, err := nc.JS.AddConsumer(stream, &nats.ConsumerConfig{
		Durable: consumerID,

		AckPolicy:     nats.AckExplicitPolicy,
		DeliverPolicy: nats.DeliverAllPolicy,
	})
	return err
}

func (nc *GolNats) PublishJS(subject string, msg []byte) error {
	_, errPub := nc.JS.Publish(subject, msg)
	return errPub
}

func (nc *GolNats) SubscriberJS(subject string, evalMsg func(*nats.Msg)) error {
	_, err := nc.JS.Subscribe(subject, func(m *nats.Msg) {
		m.InProgress()
		evalMsg(m)
	}, nats.OrderedConsumer())

	return err
}

func (nc *GolNats) LogJS() {
	fmt.Println("Streams:")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for info := range nc.JS.StreamsInfo(nats.Context(ctx)) {
		fmt.Println("\t*", info.Config.Name)
	}

	fmt.Println("Consumers:")
	for info := range nc.JS.ConsumersInfo("FOO", nats.MaxWait(9*time.Second)) {
		fmt.Println("\t*", info.Name)
	}
}
