package nats

import (
	"context"
	"encoding/json"
	"errors"
	"math/rand"
	"sync"
	"time"

	cloudevents "github.com/cloudevents/sdk-go/v2"
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

func (n *natsDriver) Publish(ctx context.Context, topic string, event cloudevents.Event) error {
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

	mu             sync.Mutex
	activeMessages map[*natsMessage]struct{}
}

func (n *natsConsumer) Receive() (equeue.Message, error) {
	for {
		select {
		case <-n.ctx.Done():
			return nil, equeue.ErrConsumerStopped
		default:
		}

		n.mu.Lock()
		if n.maxAckPending > 0 && len(n.activeMessages) >= n.maxAckPending {
			n.mu.Unlock()
			time.Sleep(2*time.Second + time.Duration(rand.Intn(int(time.Second))))
			continue
		}
		n.mu.Unlock()

		nMsgs, err := n.sub.Fetch(1, nats.Context(n.ctx))
		switch {
		case errors.Is(err, nats.ErrTimeout):
			continue
		case errors.Is(err, context.DeadlineExceeded):
			continue
		case errors.Is(err, context.Canceled):
			return nil, equeue.ErrConsumerStopped
		case err != nil:
			return nil, err
		}
		nMsg := nMsgs[0]

		event := cloudevents.NewEvent()
		if err := json.Unmarshal(nMsg.Data, &event); err != nil {
			return nil, err
		}

		msg := &natsMessage{msg: nMsg, event: &event, consumer: n}

		if !n.trackMessages(msg, true) {
			return nil, equeue.ErrConsumerStopped
		}

		return msg, nil
	}
}

func (n *natsConsumer) Stop() error {
	n.cancelCtx()
	return nil
}

func (n *natsConsumer) trackMessages(msg *natsMessage, add bool) bool {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.activeMessages == nil {
		n.activeMessages = make(map[*natsMessage]struct{})
	}

	if add {
		select {
		case <-n.ctx.Done():
			return false
		default:
		}
		n.activeMessages[msg] = struct{}{}
	} else {
		delete(n.activeMessages, msg)
	}

	return true
}

var _ equeue.Message = new(natsMessage)

type natsMessage struct {
	msg      *nats.Msg
	event    *cloudevents.Event
	consumer *natsConsumer
}

func (n *natsMessage) Event() *cloudevents.Event {
	return n.event
}

func (n *natsMessage) Ack() {
	n.msg.Ack()
	n.consumer.trackMessages(n, false)
}

func (n *natsMessage) Nack() {
	n.msg.Nak()
	n.consumer.trackMessages(n, false)
}
