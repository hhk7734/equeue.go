package nats

import (
	"context"
	"encoding/json"
	"errors"
	"sync"

	"github.com/cloudevents/sdk-go/v2/event"
	"github.com/hhk7734/equeue.go"
	"github.com/nats-io/nats.go"
)

func Open(url string, opts ...nats.Option) *natsDriver {
	n, err := nats.Connect(url, opts...)
	if err != nil {
		panic(err)
	}

	js, err := n.JetStream()
	if err != nil {
		panic(err)
	}

	return &natsDriver{js: js}
}

var _ equeue.Driver = new(natsDriver)

type natsDriver struct {
	js nats.JetStreamContext
}

func (n *natsDriver) Client() nats.JetStreamContext {
	return n.js
}

func (n *natsDriver) Publish(ctx context.Context, topic string, event *event.Event) error {
	data, err := json.Marshal(event)
	if err != nil {
		return err
	}
	// TODO: custom subject from topic and Event
	_, err = n.js.Publish(topic, data, nats.ExpectStream(topic))
	return err
}

func (n *natsDriver) Consumer(topic string, subscriptionName string, maxAckPending int) (equeue.Consumer, error) {
	if _, err := n.js.ConsumerInfo(topic, subscriptionName); err != nil {
		return nil, err
	}

	sub, err := n.js.PullSubscribe("", subscriptionName, nats.BindStream(topic))
	if err != nil {
		return nil, err
	}

	ctx, cancelCtx := context.WithCancel(context.Background())

	return &natsConsumer{sub: sub, maxAckPending: maxAckPending, ctx: ctx, cancelCtx: cancelCtx}, nil
}

func (n *natsDriver) Close() error {
	return nil
}

var _ equeue.Consumer = new(natsConsumer)

type natsConsumer struct {
	sub           *nats.Subscription
	maxAckPending int
	ctx           context.Context
	cancelCtx     context.CancelFunc

	mu   sync.Mutex
	msgs map[*natsMessage]struct{}
}

func (n *natsConsumer) Receive() (equeue.Message, error) {
	for {
		select {
		case <-n.ctx.Done():
			return nil, equeue.ErrConsumerStopped
		default:
		}

		nMsgs, err := n.sub.Fetch(1, nats.Context(n.ctx))
		switch {
		case errors.Is(err, nats.ErrTimeout):
			continue
		case errors.Is(err, context.Canceled):
			return nil, equeue.ErrConsumerStopped
		case err != nil:
			return nil, err
		}
		nMsg := nMsgs[0]

		event := event.New()
		if err := json.Unmarshal(nMsg.Data, &event); err != nil {
			return nil, err
		}

		msg := &natsMessage{msg: nMsg, event: &event}

		n.mu.Lock()
		if n.msgs == nil {
			n.msgs = make(map[*natsMessage]struct{})
		}
		n.msgs[msg] = struct{}{}
		n.mu.Unlock()

		return msg, nil
	}
}

func (n *natsConsumer) Stop() error {
	n.cancelCtx()
	return nil
}

var _ equeue.Message = new(natsMessage)

type natsMessage struct {
	msg   *nats.Msg
	event *event.Event
}

func (n *natsMessage) Event() *event.Event {
	return n.event
}

func (n *natsMessage) Ack() {
	n.msg.Ack()
}

func (n *natsMessage) Nack() {
	n.msg.Nak()
}
