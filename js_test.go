package golnats

import (
	"log"
	"testing"
	"time"

	nats "github.com/nats-io/nats.go"
)

var (
	TestNatsJetstreamUrl = "127.0.0.1:4231"
	TestJSStream         = "TESTxSTREAM"
	TestJSSubjects       = []string{"test.sub.x", "test.sub.y"}
)

func TestJetstreamConnectAndClose(t *testing.T) {
	nc, err := ConnectNatsJS(TestNatsJetstreamUrl, TestJSStream, TestJSSubjects, "")
	if err != nil {
		t.Errorf("FAILED for ConnectNats\n%v", err)
	}
	if nc.Connection == nil {
		t.Fatalf("FAILED to connect NATS server at: %s", TestNatsJetstreamUrl)
	}
	nc.JS.DeleteStream(TestJSStream)
	nc.Close()
}

func TestJetstreamDurableConsumer(t *testing.T) {
	nc, err := ConnectNatsJS(TestNatsJetstreamUrl, TestJSStream, TestJSSubjects, "")
	if err != nil {
		t.Errorf("FAILED for ConnectNats\n%v", err)
	}

	if errCon := nc.AddDurableConsumerJS(TestJSStream, "DuraConX"); errCon != nil {
		t.Errorf("FAILED to add Durable Consumer.\n%s", errCon.Error())
	}

	var result string
	assignToResult := func(m *nats.Msg) {
		result = string(m.Data)
		log.Printf("[%s] Request: %s\n", m.Subject, result)
		m.AckSync()
	}
	if errSub := nc.SubscriberJS(TestJSSubjects[0], assignToResult); errSub != nil {
		t.Errorf("FAILED to subsrcibe: %v", TestJSSubjects[0])
	}

	assignWithPrefixToResult := func(m *nats.Msg) {
		result = "result: " + string(m.Data)
		log.Printf("[%s] Request: %s\n", m.Subject, result)
		m.AckSync()
	}
	if errSub := nc.SubscriberJS(TestJSSubjects[1], assignWithPrefixToResult); errSub != nil {
		t.Errorf("FAILED to subsrcibe: %v", TestJSSubjects[1])
	}

	if errPub := nc.PublishJS(TestJSSubjects[0], []byte("101")); errPub != nil {
		t.Errorf("FAILED to publish to: %v", TestJSSubjects[1])
	}
	time.Sleep(5 * time.Millisecond)
	if result != "101" {
		t.Errorf("FAILED for subcribe/publish at Jetstream[0] with %s", result)
	}

	if errPub := nc.PublishJS(TestJSSubjects[1], []byte("202")); errPub != nil {
		t.Errorf("FAILED to publish to: %v", TestJSSubjects[1])
	}
	time.Sleep(5 * time.Millisecond)
	if result != "result: 202" {
		t.Errorf("FAILED for subcribe/publish at Jetstream[1] with %s", result)
	}

	nc.JS.DeleteConsumer(TestJSStream, "DuraConX")
	nc.JS.DeleteStream(TestJSStream)
	nc.Close()
}
