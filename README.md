
## golnats

> facade for regular used NATS use-cases
>
> extracted from an older pandora box of such packages at [abhishekkr/gol](https://github.com/abhishekkr/gol)

### Public Functions

#### Common

* `(*GolNats) Connect() error`
* `(*GolNats) Close()`
* `(*GolNats) Unsubscribe()`
* `Log(*nats.Msg)`

#### Simple Flow, not Jetstream

* `ConnectNats(natsURLs string, subject string) GolNats`
* `(*GolNats) Request(reqData []byte) (result []byte, err error)`
* `(*GolNats) Reply(evalMsg func(*nats.Msg)) error`
* `(*GolNats) Publish([]byte) error`
* `(*GolNats) PublishMessage(reply string, data []byte) error`
* `(*GolNats) QueueSubscriber(qName string, evalMsg func(*nats.Msg)) error`
* `(*GolNats) SubscriberAsync(func(*nats.Msg)) error`
* `(*GolNats) SubscriberSync(func(*nats.Msg), time.Duration) error`
* `(*GolNats) SubscriberChan(func(*nats.Msg)) error`
* `(*GolNats) Log()`

#### Jetstream Flow

* `ConnectNatsJS(natsURLs string, stream string, subjects []string) GolNats`
*  `(*GolNats) AddDurableConsumerJS(stream, consumerID string) error`
*  `(*GolNats) PublishJS(subject string, msg []byte) error`
*  `(*GolNats) SubscriberJS(subject string, funv(*nats.Msg)) error`
*  `(*GolNats) LogJS()`

---
