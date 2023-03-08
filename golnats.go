package golnats

import (
	"fmt"
	"log"
	"time"

	nats "github.com/nats-io/nats.go"
)

/*
go-nats wrapper for quick pub-sub usage
*/

var (
	DefaultTimeoutMillisec = 100 * time.Millisecond
	DrainTimeoutMillisec   = 1000 * time.Millisecond
	FlushTimeoutMillisec   = 1000 * time.Millisecond
)

type GolNats struct {
	URL       string // this would support CSV for cluster as well
	ConnName  string // for monitoring stats
	ConnToken string // for using user:password, make them part of URLs as "myname:password@127.0.0.1;4222"

	Connection  *nats.Conn
	IsJetStream bool
	JS          nats.JetStreamContext
	stream      *nats.StreamInfo

	Subscription *nats.Subscription
	Channel      chan *nats.Msg
	Subject      string
	LastMessage  []byte
	Timeout      time.Duration
}

func ConnectNats(natsURLs string, subject string) GolNats {
	nc := GolNats{
		URL:      natsURLs,
		ConnName: fmt.Sprintf("connect::%s", subject),
		Subject:  subject,
	}
	nc.Connect()
	return nc
}

func (nc *GolNats) Connect() (err error) {
	if nc.Timeout == (0 * time.Millisecond) {
		nc.Timeout = DefaultTimeoutMillisec
	}

	nc.Connection, err = nats.Connect(
		nc.URL,
		nats.Name(nc.ConnName),
		nats.Timeout(nc.Timeout),
		nats.Token(nc.ConnToken),
		nats.DrainTimeout(DrainTimeoutMillisec),
	)
	if err != nil {
		log.Println("Connection to ", nc.URL, " failed. ", err.Error())
		return
	}
	log.Println("Connected to " + nc.URL)
	return
}

func (nc *GolNats) Close() {
	if err := nc.Connection.Drain(); err != nil {
		log.Fatalln(err)
	}
	if err := nc.Connection.FlushTimeout(FlushTimeoutMillisec); err != nil {
		log.Println("ERROR: during flush for unsubscribe: ", err.Error())
	}
	nc.Connection.Close()
}

func (nc *GolNats) Unsubscribe() {
	if err := nc.Connection.FlushTimeout(FlushTimeoutMillisec); err != nil {
		log.Println("ERROR: during flush for unsubscribe: ", err.Error())
	}
	nc.Subscription.Unsubscribe()
	nc.Subscription = nil
}

func (nc *GolNats) Request(reqData []byte) (result []byte, err error) {
	var msg *nats.Msg

	msg, err = nc.Connection.Request(nc.Subject, reqData, nc.Timeout)
	if err != nil || msg == nil {
		log.Println(err)
		return
	}

	result = msg.Data
	nc.LastMessage = msg.Data
	return
}

func (nc *GolNats) Reply(evalMsg func(*nats.Msg)) (err error) {
	nc.Subscription, err = nc.Connection.Subscribe(nc.Subject, evalMsg)
	return
}

func (nc *GolNats) Publish(msg []byte) (err error) {
	return nc.Connection.Publish(nc.Subject, msg)
}

func (nc *GolNats) PublishMessage(reply string, data []byte) (err error) {
	natsMsg := &nats.Msg{Subject: nc.Subject}
	if reply != "" {
		natsMsg.Reply = reply
	}
	if len(data) > 0 {
		natsMsg.Data = data
	}
	nc.Connection.PublishMsg(natsMsg)
	return
}

func (nc *GolNats) QueueSubscriber(qName string, evalMsg func(*nats.Msg)) (err error) {
	nc.Subscription, err = nc.Connection.QueueSubscribe(nc.Subject, qName, evalMsg)
	return
}

func (nc *GolNats) SubscriberAsync(evalMsg func(*nats.Msg)) (err error) {
	nc.Subscription, err = nc.Connection.Subscribe(nc.Subject, evalMsg)
	return
}

func (nc *GolNats) SubscriberSync(evalMsg func(*nats.Msg), timeout time.Duration) (err error) {
	var msg *nats.Msg

	if nc.Subscription == nil {
		nc.Subscription, err = nc.Connection.SubscribeSync(nc.Subject)
		if err != nil {
			log.Println(err)
			return
		}
	}

	msg, err = nc.Subscription.NextMsg(timeout)
	if err != nil || msg == nil {
		log.Println(err)
		return
	}

	evalMsg(msg)
	nc.LastMessage = msg.Data
	return
}

func (nc *GolNats) SubscriberChan(evalMsg func(*nats.Msg)) (err error) {
	if nc.Channel == nil {
		nc.Channel = make(chan *nats.Msg, 64)
	}
	if nc.Subscription == nil {
		nc.Subscription, err = nc.Connection.ChanSubscribe(nc.Subject, nc.Channel)
		if err != nil {
			log.Println(err)
			return
		}
	}

	for msg := range nc.Channel {
		if msg == nil {
			log.Println(err)
			continue
		}

		evalMsg(msg)
	}
	return
}

func (nc *GolNats) Log() {
	log.Println(nc.ConnName, "|", string(nc.LastMessage))
}

func Log(m *nats.Msg) {
	log.Printf("%s: %s", m.Subject, m.Data)
}
