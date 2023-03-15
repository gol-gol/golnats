
## golnats

> facade for regular used NATS use-cases
>
> extracted from an older pandora box of such packages at [abhishekkr/gol](https://github.com/abhishekkr/gol)

### Public Functions

#### Simple Flow, not Jetstream

* `(*GolNats) Connect() (err error)`
* `(*GolNats) Close()`
* `(*GolNats) Unsubscribe()`
* `(*GolNats) Request(reqData []byte) (result []byte, err error)`
* `(*GolNats) Reply(evalMsg func(*nats.Msg)) (err error)`
* `(*GolNats) Publish(msg []byte) (err error)`
* `(*GolNats) PublishMessage(reply string, data []byte) (err error)`
* `(*GolNats) QueueSubscriber(qName string, evalMsg func(*nats.Msg)) (err error)`
* `(*GolNats) SubscriberAsync(evalMsg func(*nats.Msg)) (err error)`
* `(*GolNats) SubscriberSync(evalMsg func(*nats.Msg), timeout time.Duration) (err error)`
* `(*GolNats) SubscriberChan(evalMsg func(*nats.Msg)) (err error)`
* `(*GolNats) Log()`
* `ConnectNats(natsURLs, subject, authToken string) GolNats`
* `Log(m *nats.Msg)`

#### Jetstream Flow


* `(*JSGolNats) Connect(ctx context.Context, natsURLs, authToken string) error`
* `(*JSGolNats) Close()`
* `(*JSGolNats) AddDurableConsumerJS(ctx context.Context, consumerID string) error`
* `(*JSGolNats) PublishJS(ctx context.Context, subject string, msg []byte) error`
* `(*JSGolNats) SubscriberJS(ctx context.Context, subject string, evalMsg func(*nats.Msg)) error`
* `(*JSGolNats) PullSubscriberJS(ctx context.Context, subject, qName string, evalMsg func(*nats.Msg), breakMsg func(*nats.Msg) bool) error`
* `(*JSGolNats) LogJS()`


---
