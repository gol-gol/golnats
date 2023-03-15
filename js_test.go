package golnats

import (
	"context"
	"log"
	"testing"
	"time"

	nats "github.com/nats-io/nats.go"
)

var (
	TestNatsJetstreamUrl = "127.0.0.1:4231"
	TestJSStream         = "TESTxSTREAM"
	TestJSSubjects       = []string{"test.sub.x", "test.sub.y"}
	TestJSGolNats        = JSGolNats{
		Stream:         TestJSStream,
		Subjects:       TestJSSubjects,
		AckPolicy:      DefaultAckPolicy, //WorkQueue Stream requires ExplicitAck
		AckWait:        DefaultAckWaitMilliSec,
		DeliveryPolicy: nats.DeliverAllPolicy,
		MaxDeliver:     DefaultMaxDeliver,
		Retention:      nats.InterestPolicy,
	}
)

func TestJetstreamConnectAndClose(t *testing.T) {
	ctx := context.Background()
	jnc := TestJSGolNats
	err := jnc.Connect(ctx, TestNatsJetstreamUrl, "")
	if err != nil {
		t.Errorf("FAILED for ConnectNats\n%v", err)
	}
	if jnc.GolNats.Connection == nil {
		t.Fatalf("FAILED to connect NATS server at: %s", TestNatsJetstreamUrl)
	}
	jnc.JS.DeleteStream(TestJSStream)
	jnc.Close()
}

func TestJetstreamDurableConsumer(t *testing.T) {
	ctx := context.Background()
	jnc := TestJSGolNats
	err := jnc.Connect(ctx, TestNatsJetstreamUrl, "")
	if err != nil {
		t.Errorf("FAILED for ConnectNats\n%v", err)
	}

	if errCon := jnc.AddDurableConsumerJS(ctx, "DuraConX"); errCon != nil {
		t.Errorf("FAILED to add Durable Consumer.\n%s", errCon.Error())
	}

	var result string
	assignToResult := func(m *nats.Msg) {
		result = string(m.Data)
		log.Printf("[%s] Request: %s\n", m.Subject, result)
		m.AckSync()
	}
	if errSub := jnc.SubscriberJS(ctx, TestJSSubjects[0], assignToResult); errSub != nil {
		t.Errorf("FAILED to subsrcibe: %v\n%v", TestJSSubjects[0], errSub)
	}

	assignWithPrefixToResult := func(m *nats.Msg) {
		result = "result: " + string(m.Data)
		log.Printf("[%s] Request: %s\n", m.Subject, result)
		m.AckSync()
	}
	if errSub := jnc.SubscriberJS(ctx, TestJSSubjects[1], assignWithPrefixToResult); errSub != nil {
		t.Errorf("FAILED to subsrcibe: %v\n%v", TestJSSubjects[1], errSub)
	}

	if errPub := jnc.PublishJS(ctx, TestJSSubjects[0], []byte("101")); errPub != nil {
		t.Errorf("FAILED to publish to: %v", TestJSSubjects[1])
	}
	time.Sleep(5 * time.Millisecond)
	if result != "101" {
		t.Errorf("FAILED for subcribe/publish at Jetstream[0] with %s", result)
	}

	if errPub := jnc.PublishJS(ctx, TestJSSubjects[1], []byte("202")); errPub != nil {
		t.Errorf("FAILED to publish to: %v", TestJSSubjects[1])
	}
	time.Sleep(5 * time.Millisecond)
	if result != "result: 202" {
		t.Errorf("FAILED for subcribe/publish at Jetstream[1] with %s", result)
	}

	jnc.JS.DeleteConsumer(TestJSStream, "DuraConX")
	jnc.JS.DeleteStream(TestJSStream)
	jnc.Close()
}

// Tests for One Message Processed Once With Multiple Subscribers
func TestJetstreamPullSubscriber(t *testing.T) {
	// for PullSubscribe, nats: context requires a deadline
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	jncX := TestJSGolNats
	jncY := TestJSGolNats
	jncX.Retention = nats.WorkQueuePolicy
	jncY.Retention = nats.WorkQueuePolicy
	errX := jncX.Connect(ctx, TestNatsJetstreamUrl, "")
	errY := jncY.Connect(ctx, TestNatsJetstreamUrl, "")
	if errX != nil || errY != nil {
		t.Errorf("FAILED for ConnectNats\n%v\n%v", errX, errY)
	}

	jncX.LogJS()
	jncY.LogJS()

	subject := TestJSSubjects[0]
	callCount := 0
	breakMsg := func(m *nats.Msg) bool {
		return len(m.Data) == 0
	}

	go func() {
		countMeX := func(m *nats.Msg) {
			callCount += 1
			log.Println("Called countMeX")
		}
		if errSub := jncX.PullSubscriberJS(ctx, subject, "qXY", countMeX, breakMsg); errSub != nil {
			t.Errorf("FAILED to subscribe: %v\n%v", subject, errSub)
		}
	}()
	go func() {
		countMeY := func(m *nats.Msg) {
			callCount += 1
			log.Println("Called countMeY")
		}
		if errSub := jncY.PullSubscriberJS(ctx, subject, "qXY", countMeY, breakMsg); errSub != nil {
			t.Errorf("FAILED to subscribe: %v\n%v", subject, errSub)
		}
	}()

	jncX.PublishJS(ctx, subject, []byte("1"))
	jncX.PublishJS(ctx, subject, []byte("2"))
	jncX.PublishJS(ctx, subject, []byte("3"))
	jncY.PublishJS(ctx, subject, []byte("4"))
	jncY.PublishJS(ctx, subject, []byte("5"))
	jncY.PublishJS(ctx, subject, []byte{})
	jncY.PublishJS(ctx, subject, []byte{})
	time.Sleep(5 * time.Millisecond)
	if callCount != 5 {
		t.Errorf("FAILED for subcribe/publish at Jetstream for count: %d", callCount)
	}

	jncX.JS.DeleteStream(TestJSStream)
	jncX.Close()

	jncY.JS.DeleteStream(TestJSStream)
	jncY.Close()
}
