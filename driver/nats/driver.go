package nats

import (
	"bytes"
	"context"
	"io"

	"github.com/cloudevents/sdk-go/v2/binding"
	"github.com/cloudevents/sdk-go/v2/binding/format"
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

func (n *natsDriver) Send(ctx context.Context, topic string, msg binding.Message) error {
	var err error
	defer msg.Finish(err)

	writer := new(bytes.Buffer)
	if err = WriteMsg(ctx, msg, writer); err != nil {
		return err
	}

	// TODO: custom subject from topic and Event
	_, err = n.js.Publish(topic, writer.Bytes(), nats.ExpectStream(topic))
	return err
}

func (n *natsDriver) Consumer(topic string, subscriptionName string) (equeue.Consumer, error) {
	if _, err := n.js.ConsumerInfo(topic, subscriptionName); err != nil {
		return nil, err
	}

	sub, err := n.js.PullSubscribe("", subscriptionName, nats.BindStream(topic))
	if err != nil {
		return nil, err
	}

	ctx, cancelCtx := context.WithCancel(context.Background())

	return &natsConsumer{sub: sub, ctx: ctx, cancelCtx: cancelCtx}, nil
}

func (n *natsDriver) Close() error {
	return nil
}

func WriteMsg(ctx context.Context, m binding.Message, writer io.ReaderFrom, transformers ...binding.Transformer) error {
	structuredWriter := &natsMessageWriter{writer}

	_, err := binding.Write(
		ctx,
		m,
		structuredWriter,
		nil,
		transformers...,
	)
	return err
}

var _ binding.StructuredWriter = new(natsMessageWriter)

type natsMessageWriter struct {
	io.ReaderFrom
}

func (w *natsMessageWriter) SetStructuredEvent(_ context.Context, _ format.Format, event io.Reader) error {
	if _, err := w.ReadFrom(event); err != nil {
		return err
	}

	return nil
}
