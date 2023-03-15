package golnats

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"time"

	nats "github.com/nats-io/nats.go"
)

/*
go-nats facade for pattern-based JetStream usage.

TODO:
https://gist.github.com/wallyqs/b01ba613341170b4442acbffcaea0a81
https://github.com/nats-io/nats.go/issues/712#issuecomment-835431721
*/

var (
	PubAsyncMaxPending = 256
	//https://docs.nats.io/nats-concepts/core-nats/queue
	DefaultRetentionPolicy = nats.LimitsPolicy // nats.WorkQueuePolicy
	//https://docs.nats.io/nats-concepts/jetstream/consumers
	DefaultAckPolicy       = nats.AckExplicitPolicy
	DefaultAckWaitMilliSec = 60 * time.Second
	DefaultDeliverPolicy   = nats.DeliverAllPolicy
	DefaultMaxDeliver      = 5
)

type JSGolNats struct {
	GolNats        GolNats
	JS             nats.JetStreamContext
	Stream         string
	Subjects       []string
	Retention      nats.RetentionPolicy
	AckPolicy      nats.AckPolicy
	AckWait        time.Duration
	DeliveryPolicy nats.DeliverPolicy
	MaxDeliver     int
}

func (jnc *JSGolNats) Connect(ctx context.Context, natsURLs, authToken string) error {
	jnc.GolNats = GolNats{
		URL:       natsURLs,
		ConnName:  fmt.Sprintf("js::%s", jnc.Stream),
		ConnToken: authToken,
	}
	if errNats := jnc.GolNats.Connect(); errNats != nil {
		return errNats
	}

	var errJS error
	jnc.JS, errJS = jnc.GolNats.Connection.JetStream(nats.PublishAsyncMaxPending(PubAsyncMaxPending))
	if errJS != nil {
		return errJS
	}

	if len(jnc.Subjects) == 0 {
		jnc.Subjects = []string{fmt.Sprintf("%s.>", jnc.Stream)} // to inherit namespace
		log.Println("Setting Subjects to default:", jnc.Subjects)
	}
	log.Printf("Creating/Adding\n\tstream: %s\n\tsubjects: %v", jnc.Stream, jnc.Subjects)
	_, errStream := jnc.JS.AddStream(&nats.StreamConfig{
		Name:      jnc.Stream,
		Subjects:  jnc.Subjects,
		Retention: jnc.Retention,
	}, nats.Context(ctx))
	return errStream
}

func (jnc *JSGolNats) AddDurableConsumerJS(ctx context.Context, consumerID string) error {
	_, err := jnc.JS.AddConsumer(jnc.Stream, &nats.ConsumerConfig{
		Durable:       consumerID,
		AckPolicy:     jnc.AckPolicy,
		AckWait:       jnc.AckWait,
		DeliverPolicy: jnc.DeliveryPolicy,
		MaxDeliver:    jnc.MaxDeliver,
	})
	return err
}

func (jnc *JSGolNats) PublishJS(ctx context.Context, subject string, msg []byte) error {
	_, errPub := jnc.JS.Publish(subject, msg, nats.Context(ctx))
	return errPub
}

func (jnc *JSGolNats) SubscriberJS(ctx context.Context, subject string, evalMsg func(*nats.Msg)) error {
	_, err := jnc.JS.Subscribe(subject, func(m *nats.Msg) {
		m.InProgress()
		evalMsg(m)
	}, nats.OrderedConsumer(), nats.Context(ctx))

	return err
}

func (jnc *JSGolNats) PullSubscriberJS(ctx context.Context, subject, qName string, evalMsg func(*nats.Msg), breakMsg func(*nats.Msg) bool) error {
	subscription, errSub := jnc.JS.PullSubscribe(subject, qName)
	if errSub != nil {
		return errSub
	}
	for {
		msg, err := subscription.Fetch(1, nats.Context(ctx))
		if err != nil {
			log.Printf("[ERROR] Could not fetch msg: %v\n%v\n", msg, err)
			if errors.Is(err, context.DeadlineExceeded) {
				break
			}
			continue
		}
		if breakMsg(msg[0]) {
			msg[0].Ack()
			break
		}
		msg[0].InProgress()
		evalMsg(msg[0])
		err = msg[0].Ack()
		if err != nil {
			log.Printf("[WARN] Could not ack msg: %s\n%v\n", msg[0].Data, err)
		}
	}
	return nil
}

func (jnc *JSGolNats) Close() {
	jnc.GolNats.Close()
}

func (jnc *JSGolNats) LogJS() {
	fmt.Println("Streams:")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for info := range jnc.JS.StreamsInfo(nats.Context(ctx)) {
		b, _ := json.MarshalIndent(info.State, "", " ")
		fmt.Println("\t*", info.Config.Name, ":", string(b))
	}

	fmt.Println("Consumers:")
	for info := range jnc.JS.ConsumersInfo("FOO", nats.MaxWait(9*time.Second)) {
		fmt.Println("\t*", info.Name)
	}
}
