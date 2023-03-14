package golnats

import (
	"fmt"
	"log"
	"testing"
	"time"

	nats "github.com/nats-io/nats.go"
)

var (
	TestNatsNonJetstreamUrl = "127.0.0.1:4230"
	TestNatsSubject         = "test-stream"
	TestNatsToken           = "some-very-long-charset-and-loaded-from-config-to-be-safe"
)

func init() {
	fmt.Printf(`expects NATS server to be running
* in non-jetstream mode at 127.0.0.1:4230 | cmd: "nats-server -p 4230 -auth %s"
* in jetstream mode at 127.0.0.1:4231     | cmd: "nats-server -js -p 4231"
	`, TestNatsToken)
}

func TestGolNatsConnectAndClose(t *testing.T) {
	nc := GolNats{
		URL:       TestNatsNonJetstreamUrl,
		ConnName:  "test-connection",
		ConnToken: TestNatsToken,
		Subject:   TestNatsSubject,
	}
	nc.Connect()
	if nc.Connection == nil {
		t.Fatalf("FAILED to connect NATS server at: %s", TestNatsNonJetstreamUrl)
	}

	nc.Close()
}

func TestGolNatsRequestAndReply(t *testing.T) {
	nc := ConnectNats(TestNatsNonJetstreamUrl, TestNatsSubject, TestNatsToken)

	var result string
	assignToResult := func(m *nats.Msg) {
		result = string(m.Data)
		log.Println("[+] Request:", result)
		reply := fmt.Sprintf("Length: %d", len(result))
		m.Respond([]byte(reply))
	}

	if nc.Reply(assignToResult) != nil {
		t.Fatal("FAILED to subscribe for Reply process.")
	}

	response, errReq := nc.Request([]byte("FOUR"))
	if errReq != nil {
		t.Fatal("FAILED to Request.")
	} else if string(response) != fmt.Sprintf("Length: %d", len(result)) {
		t.Fatalf("FAILED for receiving wrong response: %s | request was %s", string(response), result)
	}
	nc.Log()

	nc.Close()
}

func TestGolNatsReqReplyAfterUnsubscribe(t *testing.T) {
	nc := ConnectNats(TestNatsNonJetstreamUrl, TestNatsSubject, TestNatsToken)

	var result = "SHOULDN'T MATTER"
	assignToResult := func(m *nats.Msg) {
		m.Respond([]byte(result))
	}

	nc.Reply(assignToResult)
	nc.Unsubscribe()

	response, errReq := nc.Request([]byte("FOUR"))
	if errReq == nil {
		t.Fatal("FAILED; Request should have failed as responder should have been unsubscribed.")
	} else if len(response) != 0 {
		t.Fatalf("FAILED for receiving wrong response: %s | request was %s", string(response), result)
	}
	nc.Log()

	nc.Close()
}

func TestPublishAndSubscriber(t *testing.T) {
	ncP := ConnectNats(TestNatsNonJetstreamUrl, TestNatsSubject, TestNatsToken)
	ncS := ConnectNats(TestNatsNonJetstreamUrl, TestNatsSubject, TestNatsToken)
	subscriberCounter := 0

	var msgBytes = []byte("FOUR")
	checkMsg := func(m *nats.Msg) {
		subscriberCounter += 1
		Log(m)
		if string(m.Data) != string(msgBytes) {
			t.Errorf("FAILED to receive published msg: %s | got: %s", string(msgBytes), string(m.Data))
		}
	}

	if errS := ncS.QueueSubscriber("testQ", checkMsg); errS != nil {
		t.Errorf("FAILED to call QueueSubscriber at %s | %s", TestNatsSubject, errS.Error())
	}
	time.Sleep(10 * time.Millisecond) // to give conn time for subscriber
	if errP := ncP.Publish(msgBytes); errP != nil {
		t.Errorf("FAILED to call Publish at %s | %s", TestNatsSubject, errP.Error())
	}

	time.Sleep(10 * time.Millisecond) // to give conn time for subscriber
	ncS.Unsubscribe()
	time.Sleep(10 * time.Millisecond) // to give conn time for subscriber

	if errS := ncS.SubscriberAsync(checkMsg); errS != nil {
		t.Errorf("FAILED to call QueueSubscriber at %s | %s", TestNatsSubject, errS.Error())
	}
	time.Sleep(10 * time.Millisecond) // to give conn time for subscriber
	if errP := ncP.Publish(msgBytes); errP != nil {
		t.Errorf("FAILED to call Publish at %s | %s", TestNatsSubject, errP.Error())
	}

	time.Sleep(10 * time.Millisecond) // to give conn time for subscriber
	ncS.Unsubscribe()
	time.Sleep(10 * time.Millisecond) // to give conn time for subscriber

	go ncS.SubscriberSync(checkMsg, 100*time.Millisecond)
	time.Sleep(10 * time.Millisecond) // to give conn time for subscriber
	ncP.Publish(msgBytes)

	time.Sleep(10 * time.Millisecond) // to give conn time for subscriber
	ncS.Unsubscribe()
	time.Sleep(10 * time.Millisecond) // to give conn time for subscriber

	go ncS.SubscriberChan(checkMsg)
	time.Sleep(10 * time.Millisecond) // to give conn time for subscriber
	ncP.Publish(msgBytes)
	ncP.Publish(msgBytes)

	ncP.Close()
	ncS.Close()
	if subscriberCounter != 5 {
		t.Errorf("FAILED for subscriberCounter, it is %d", subscriberCounter)
	}
}
